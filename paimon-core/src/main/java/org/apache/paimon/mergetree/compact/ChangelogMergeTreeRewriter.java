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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.CoreOptions.MergeEngine;
import org.apache.paimon.KeyValue;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.encryption.EncryptionManager;
import org.apache.paimon.encryption.KmsClient;
import org.apache.paimon.encryption.PlaintextEncryptionManager;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.mergetree.MergeSorter;
import org.apache.paimon.mergetree.MergeTreeReaders;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.reader.RecordReaderIterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A {@link MergeTreeCompactRewriter} which produces changelog files while performing compaction.
 */
public abstract class ChangelogMergeTreeRewriter extends MergeTreeCompactRewriter {

    protected final int maxLevel;
    protected final MergeEngine mergeEngine;

    public ChangelogMergeTreeRewriter(
            int maxLevel,
            MergeEngine mergeEngine,
            KeyValueFileReaderFactory readerFactory,
            KeyValueFileWriterFactory writerFactory,
            Comparator<InternalRow> keyComparator,
            MergeFunctionFactory<KeyValue> mfFactory,
            MergeSorter mergeSorter,
            EncryptionManager encryptionManager,
            KmsClient.CreateKeyResult createKeyResult,
            String encryptionColumns) {
        super(
                readerFactory,
                writerFactory,
                keyComparator,
                mfFactory,
                mergeSorter,
                encryptionManager,
                createKeyResult,
                encryptionColumns);
        this.maxLevel = maxLevel;
        this.mergeEngine = mergeEngine;
    }

    protected abstract boolean rewriteChangelog(
            int outputLevel, boolean dropDelete, List<List<SortedRun>> sections);

    protected abstract UpgradeStrategy upgradeChangelog(int outputLevel, DataFileMeta file);

    protected abstract MergeFunctionWrapper<ChangelogResult> createMergeWrapper(int outputLevel);

    protected boolean rewriteLookupChangelog(int outputLevel, List<List<SortedRun>> sections) {
        if (outputLevel == 0) {
            return false;
        }

        for (List<SortedRun> runs : sections) {
            for (SortedRun run : runs) {
                for (DataFileMeta file : run.files()) {
                    if (file.level() == 0) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    @Override
    public CompactResult rewrite(
            int outputLevel, boolean dropDelete, List<List<SortedRun>> sections) throws Exception {
        if (rewriteChangelog(outputLevel, dropDelete, sections)) {
            return rewriteChangelogCompaction(
                    outputLevel,
                    sections,
                    dropDelete,
                    true,
                    new PlaintextEncryptionManager(),
                    null);
        } else {
            return rewriteCompaction(
                    outputLevel,
                    dropDelete,
                    sections,
                    new PlaintextEncryptionManager(),
                    null,
                    null);
        }
    }

    /**
     * Rewrite and produce changelog at the same time.
     *
     * @param dropDelete whether to drop delete when rewrite compact file
     * @param rewriteCompactFile whether to rewrite compact file
     */
    private CompactResult rewriteChangelogCompaction(
            int outputLevel,
            List<List<SortedRun>> sections,
            boolean dropDelete,
            boolean rewriteCompactFile,
            EncryptionManager encryptionManager,
            KmsClient.CreateKeyResult createKeyResult)
            throws Exception {
        List<ConcatRecordReader.ReaderSupplier<ChangelogResult>> sectionReaders = new ArrayList<>();
        for (List<SortedRun> section : sections) {
            sectionReaders.add(
                    () ->
                            MergeTreeReaders.readerForSection(
                                    section,
                                    readerFactory,
                                    keyComparator,
                                    createMergeWrapper(outputLevel),
                                    mergeSorter,
                                    encryptionManager,
                                    encryptionColumns));
        }

        RecordReaderIterator<ChangelogResult> iterator = null;
        RollingFileWriter<KeyValue, DataFileMeta> compactFileWriter = null;
        RollingFileWriter<KeyValue, DataFileMeta> changelogFileWriter = null;

        try {
            iterator = new RecordReaderIterator<>(ConcatRecordReader.create(sectionReaders));
            if (rewriteCompactFile) {
                compactFileWriter =
                        writerFactory.createRollingMergeTreeFileWriter(
                                outputLevel, encryptionManager, createKeyResult);
            }
            changelogFileWriter =
                    writerFactory.createRollingChangelogFileWriter(
                            outputLevel, encryptionManager, createKeyResult);

            while (iterator.hasNext()) {
                ChangelogResult result = iterator.next();
                KeyValue keyValue = result.result();
                if (rewriteCompactFile && keyValue != null && (!dropDelete || keyValue.isAdd())) {
                    compactFileWriter.write(keyValue);
                }
                for (KeyValue kv : result.changelogs()) {
                    changelogFileWriter.write(kv);
                }
            }
        } finally {
            if (iterator != null) {
                iterator.close();
            }
            if (compactFileWriter != null) {
                compactFileWriter.close();
            }
            if (changelogFileWriter != null) {
                changelogFileWriter.close();
            }
        }

        List<DataFileMeta> before = extractFilesFromSections(sections);
        List<DataFileMeta> after =
                rewriteCompactFile
                        ? compactFileWriter.result()
                        : before.stream()
                                .map(x -> x.upgrade(outputLevel))
                                .collect(Collectors.toList());

        return new CompactResult(before, after, changelogFileWriter.result());
    }

    @Override
    public CompactResult upgrade(int outputLevel, DataFileMeta file) throws Exception {
        UpgradeStrategy strategy = upgradeChangelog(outputLevel, file);
        if (strategy.changelog) {
            return rewriteChangelogCompaction(
                    outputLevel,
                    Collections.singletonList(
                            Collections.singletonList(SortedRun.fromSingle(file))),
                    false,
                    strategy.rewrite,
                    new PlaintextEncryptionManager(),
                    null);
        } else {
            return super.upgrade(outputLevel, file);
        }
    }

    /** Strategy for upgrade. */
    protected enum UpgradeStrategy {
        NO_CHANGELOG(false, false),
        CHANGELOG_NO_REWRITE(true, false),
        CHANGELOG_WITH_REWRITE(true, true);

        private final boolean changelog;
        private final boolean rewrite;

        UpgradeStrategy(boolean changelog, boolean rewrite) {
            this.changelog = changelog;
            this.rewrite = rewrite;
        }
    }
}
