/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.append;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FieldStats;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.stats.FieldStatsArraySerializer;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.ExecutorThreadFactory;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.RecordWriter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;

import static org.apache.paimon.io.DataFileMeta.getMaxSequenceNumber;
import static org.assertj.core.api.Assertions.assertThat;

/** Test the correctness for {@link AppendOnlyWriter}. */
public class AppendOnlyWriterTest {

    private static final InternalRow EMPTY_ROW = BinaryRow.EMPTY_ROW;
    private static final RowType SCHEMA =
            RowType.builder()
                    .fields(
                            new DataType[] {new IntType(), new VarCharType(), new VarCharType()},
                            new String[] {"id", "name", "dt"})
                    .build();
    private static final FieldStatsArraySerializer STATS_SERIALIZER =
            new FieldStatsArraySerializer(SCHEMA);

    @TempDir public java.nio.file.Path tempDir;
    public DataFilePathFactory pathFactory;

    private static final String AVRO = "avro";
    private static final String PART = "2022-05-01";
    private static final long SCHEMA_ID = 0L;
    private static final int MIN_FILE_NUM = 3;
    private static final int MAX_FILE_NUM = 4;

    @BeforeEach
    public void before() {
        pathFactory = createPathFactory();
    }

    @Test
    public void testEmptyCommits() throws Exception {
        RecordWriter<InternalRow> writer = createEmptyWriter(1024 * 1024L);

        for (int i = 0; i < 3; i++) {
            writer.sync();
            CommitIncrement inc = writer.prepareCommit(true);

            assertThat(inc.newFilesIncrement().isEmpty()).isTrue();
            assertThat(inc.compactIncrement().isEmpty()).isTrue();
        }
    }

    @Test
    public void testSingleWrite() throws Exception {
        RecordWriter<InternalRow> writer = createEmptyWriter(1024 * 1024L);
        writer.write(row(1, "AAA", PART));
        CommitIncrement increment = writer.prepareCommit(true);
        writer.close();

        assertThat(increment.newFilesIncrement().newFiles().size()).isEqualTo(1);
        DataFileMeta meta = increment.newFilesIncrement().newFiles().get(0);
        assertThat(meta).isNotNull();

        Path path = pathFactory.toPath(meta.fileName());
        assertThat(LocalFileIO.create().exists(path)).isTrue();

        assertThat(meta.rowCount()).isEqualTo(1L);
        assertThat(meta.minKey()).isEqualTo(EMPTY_ROW);
        assertThat(meta.maxKey()).isEqualTo(EMPTY_ROW);
        assertThat(meta.keyStats()).isEqualTo(DataFileMeta.EMPTY_KEY_STATS);

        FieldStats[] expected =
                new FieldStats[] {
                    initStats(1, 1, 0), initStats("AAA", "AAA", 0), initStats(PART, PART, 0)
                };
        assertThat(meta.valueStats()).isEqualTo(STATS_SERIALIZER.toBinary(expected));

        assertThat(meta.minSequenceNumber()).isEqualTo(0);
        assertThat(meta.maxSequenceNumber()).isEqualTo(0);
        assertThat(meta.level()).isEqualTo(DataFileMeta.DUMMY_LEVEL);
    }

    @Test
    public void testMultipleCommits() throws Exception {
        RecordWriter<InternalRow> writer =
                createWriter(1024 * 1024L, true, Collections.emptyList()).getLeft();

        // Commit 5 continues txn.
        for (int txn = 0; txn < 5; txn += 1) {

            // Write the records with range [ txn*100, (txn+1)*100 ).
            int start = txn * 100;
            int end = txn * 100 + 100;
            for (int i = start; i < end; i++) {
                writer.write(row(i, String.format("%03d", i), PART));
            }

            writer.sync();
            CommitIncrement inc = writer.prepareCommit(true);
            if (txn > 0 && txn % 3 == 0) {
                assertThat(inc.compactIncrement().compactBefore()).hasSize(4);
                assertThat(inc.compactIncrement().compactAfter()).hasSize(1);
                DataFileMeta compactAfter = inc.compactIncrement().compactAfter().get(0);
                assertThat(compactAfter.fileName()).startsWith("compact-");
                assertThat(compactAfter.fileSize())
                        .isEqualTo(
                                inc.compactIncrement().compactBefore().stream()
                                        .mapToLong(DataFileMeta::fileSize)
                                        .sum());
                assertThat(compactAfter.rowCount())
                        .isEqualTo(
                                inc.compactIncrement().compactBefore().stream()
                                        .mapToLong(DataFileMeta::rowCount)
                                        .sum());
            } else {
                assertThat(inc.compactIncrement().compactBefore())
                        .isEqualTo(Collections.emptyList());
                assertThat(inc.compactIncrement().compactAfter())
                        .isEqualTo(Collections.emptyList());
            }

            assertThat(inc.newFilesIncrement().newFiles().size()).isEqualTo(1);
            DataFileMeta meta = inc.newFilesIncrement().newFiles().get(0);

            Path path = pathFactory.toPath(meta.fileName());
            assertThat(LocalFileIO.create().exists(path)).isTrue();

            assertThat(meta.rowCount()).isEqualTo(100L);
            assertThat(meta.minKey()).isEqualTo(EMPTY_ROW);
            assertThat(meta.maxKey()).isEqualTo(EMPTY_ROW);
            assertThat(meta.keyStats()).isEqualTo(DataFileMeta.EMPTY_KEY_STATS);

            FieldStats[] expected =
                    new FieldStats[] {
                        initStats(start, end - 1, 0),
                        initStats(String.format("%03d", start), String.format("%03d", end - 1), 0),
                        initStats(PART, PART, 0)
                    };
            assertThat(meta.valueStats()).isEqualTo(STATS_SERIALIZER.toBinary(expected));

            assertThat(meta.minSequenceNumber()).isEqualTo(start);
            assertThat(meta.maxSequenceNumber()).isEqualTo(end - 1);
            assertThat(meta.level()).isEqualTo(DataFileMeta.DUMMY_LEVEL);
        }
    }

    @Test
    public void testRollingWrite() throws Exception {
        // Set a very small target file size, so the threshold to trigger rolling becomes record
        // count instead of file size, because we check rolling per 1000 records.
        AppendOnlyWriter writer = createEmptyWriter(10L);

        for (int i = 0; i < 10 * 1000; i++) {
            writer.write(row(i, String.format("%03d", i), PART));
        }

        writer.sync();
        CommitIncrement firstInc = writer.prepareCommit(true);
        assertThat(firstInc.compactIncrement().compactBefore()).isEqualTo(Collections.emptyList());
        assertThat(firstInc.compactIncrement().compactAfter()).isEqualTo(Collections.emptyList());

        assertThat(firstInc.newFilesIncrement().newFiles().size()).isEqualTo(10);

        int id = 0;
        for (DataFileMeta meta : firstInc.newFilesIncrement().newFiles()) {
            Path path = pathFactory.toPath(meta.fileName());
            assertThat(LocalFileIO.create().exists(path)).isTrue();

            assertThat(meta.rowCount()).isEqualTo(1000L);
            assertThat(meta.minKey()).isEqualTo(EMPTY_ROW);
            assertThat(meta.maxKey()).isEqualTo(EMPTY_ROW);
            assertThat(meta.keyStats()).isEqualTo(DataFileMeta.EMPTY_KEY_STATS);

            int min = id * 1000;
            int max = id * 1000 + 999;
            FieldStats[] expected =
                    new FieldStats[] {
                        initStats(min, max, 0),
                        initStats(String.format("%03d", min), String.format("%03d", max), 0),
                        initStats(PART, PART, 0)
                    };
            assertThat(meta.valueStats()).isEqualTo(STATS_SERIALIZER.toBinary(expected));

            assertThat(meta.minSequenceNumber()).isEqualTo(min);
            assertThat(meta.maxSequenceNumber()).isEqualTo(max);
            assertThat(meta.level()).isEqualTo(DataFileMeta.DUMMY_LEVEL);

            id += 1;
        }

        // increase target file size to test compaction
        long targetFileSize = 1024 * 1024L;
        Pair<AppendOnlyWriter, List<DataFileMeta>> writerAndToCompact =
                createWriter(targetFileSize, true, firstInc.newFilesIncrement().newFiles());
        writer = writerAndToCompact.getLeft();
        List<DataFileMeta> toCompact = writerAndToCompact.getRight();
        assertThat(toCompact).containsExactlyElementsOf(firstInc.newFilesIncrement().newFiles());
        writer.write(row(id, String.format("%03d", id), PART));
        writer.sync();
        CommitIncrement secInc = writer.prepareCommit(true);

        // check compact before and after
        List<DataFileMeta> compactBefore = secInc.compactIncrement().compactBefore();
        List<DataFileMeta> compactAfter = secInc.compactIncrement().compactAfter();
        assertThat(compactBefore)
                .containsExactlyInAnyOrderElementsOf(
                        firstInc.newFilesIncrement().newFiles().subList(0, 4));
        assertThat(compactAfter).hasSize(1);
        assertThat(compactBefore.stream().mapToLong(DataFileMeta::fileSize).sum())
                .isEqualTo(compactAfter.stream().mapToLong(DataFileMeta::fileSize).sum());
        assertThat(compactBefore.stream().mapToLong(DataFileMeta::rowCount).sum())
                .isEqualTo(compactAfter.stream().mapToLong(DataFileMeta::rowCount).sum());
        // check seq number
        assertThat(compactBefore.get(0).minSequenceNumber())
                .isEqualTo(compactAfter.get(0).minSequenceNumber());
        assertThat(compactBefore.get(compactBefore.size() - 1).maxSequenceNumber())
                .isEqualTo(compactAfter.get(compactAfter.size() - 1).maxSequenceNumber());
        assertThat(secInc.newFilesIncrement().newFiles()).hasSize(1);
    }

    private FieldStats initStats(Integer min, Integer max, long nullCount) {
        return new FieldStats(min, max, nullCount);
    }

    private FieldStats initStats(String min, String max, long nullCount) {
        return new FieldStats(
                BinaryString.fromString(min), BinaryString.fromString(max), nullCount);
    }

    private InternalRow row(int id, String name, String dt) {
        return GenericRow.of(id, BinaryString.fromString(name), BinaryString.fromString(dt));
    }

    private DataFilePathFactory createPathFactory() {
        return new DataFilePathFactory(
                new Path(tempDir.toString()),
                "dt=" + PART,
                0,
                CoreOptions.FILE_FORMAT.defaultValue().toString());
    }

    private AppendOnlyWriter createEmptyWriter(long targetFileSize) {
        return createWriter(targetFileSize, false, Collections.emptyList()).getLeft();
    }

    private Pair<AppendOnlyWriter, List<DataFileMeta>> createWriter(
            long targetFileSize, boolean forceCompact, List<DataFileMeta> scannedFiles) {
        FileFormat fileFormat = FileFormat.fromIdentifier(AVRO, new Options());
        LinkedList<DataFileMeta> toCompact = new LinkedList<>(scannedFiles);
        AppendOnlyCompactManager compactManager =
                new AppendOnlyCompactManager(
                        Executors.newSingleThreadScheduledExecutor(
                                new ExecutorThreadFactory("compaction-thread")),
                        toCompact,
                        MIN_FILE_NUM,
                        MAX_FILE_NUM,
                        targetFileSize,
                        compactBefore ->
                                compactBefore.isEmpty()
                                        ? Collections.emptyList()
                                        : Collections.singletonList(
                                                generateCompactAfter(compactBefore)),
                        false);
        AppendOnlyWriter writer =
                new AppendOnlyWriter(
                        LocalFileIO.create(),
                        SCHEMA_ID,
                        fileFormat,
                        targetFileSize,
                        AppendOnlyWriterTest.SCHEMA,
                        getMaxSequenceNumber(toCompact),
                        compactManager,
                        forceCompact,
                        pathFactory,
                        null,
                        CoreOptions.FILE_COMPRESSION.defaultValue());
        return Pair.of(writer, compactManager.allFiles());
    }

    private DataFileMeta generateCompactAfter(List<DataFileMeta> toCompact) {
        int size = toCompact.size();
        long minSeq = toCompact.get(0).minSequenceNumber();
        long maxSeq = toCompact.get(size - 1).maxSequenceNumber();
        String fileName = "compact-" + UUID.randomUUID();
        return DataFileMeta.forAppend(
                fileName,
                toCompact.stream().mapToLong(DataFileMeta::fileSize).sum(),
                toCompact.stream().mapToLong(DataFileMeta::rowCount).sum(),
                STATS_SERIALIZER.toBinary(
                        new FieldStats[] {
                            initStats(
                                    toCompact.get(0).valueStats().min().getInt(0),
                                    toCompact.get(size - 1).valueStats().max().getInt(0),
                                    0),
                            initStats(
                                    toCompact.get(0).valueStats().min().getString(1).toString(),
                                    toCompact
                                            .get(size - 1)
                                            .valueStats()
                                            .max()
                                            .getString(1)
                                            .toString(),
                                    0),
                            initStats(PART, PART, 0)
                        }),
                minSeq,
                maxSeq,
                toCompact.get(0).schemaId());
    }
}
