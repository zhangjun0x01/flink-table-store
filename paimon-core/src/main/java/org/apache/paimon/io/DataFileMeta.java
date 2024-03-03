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

package org.apache.paimon.io;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryArray;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.encryption.KeyMetadata;
import org.apache.paimon.fs.Path;
import org.apache.paimon.stats.BinaryTableStats;
import org.apache.paimon.stats.FieldStatsArraySerializer;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.apache.paimon.data.BinaryRow.EMPTY_ROW;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.SerializationUtils.newBytesType;
import static org.apache.paimon.utils.SerializationUtils.newStringType;

/** Metadata of a data file. */
public class DataFileMeta {

    // Append only data files don't have any key columns and meaningful level value. it will use
    // the following dummy values.
    public static final BinaryTableStats EMPTY_KEY_STATS =
            new BinaryTableStats(EMPTY_ROW, EMPTY_ROW, BinaryArray.fromLongArray(new Long[0]));
    public static final BinaryRow EMPTY_MIN_KEY = EMPTY_ROW;
    public static final BinaryRow EMPTY_MAX_KEY = EMPTY_ROW;
    public static final int DUMMY_LEVEL = 0;

    private final String fileName;
    private final long fileSize;
    private final long rowCount;

    private final BinaryRow minKey;
    private final BinaryRow maxKey;
    private final BinaryTableStats keyStats;
    private final BinaryTableStats valueStats;

    private final long minSequenceNumber;
    private final long maxSequenceNumber;
    private final long schemaId;
    private final int level;

    private final List<String> extraFiles;
    private final Timestamp creationTime;
    private final KeyMetadata keyMetadata;

    public static DataFileMeta forAppend(
            String fileName,
            long fileSize,
            long rowCount,
            BinaryTableStats rowStats,
            long minSequenceNumber,
            long maxSequenceNumber,
            long schemaId,
            KeyMetadata keyMetadata) {
        return new DataFileMeta(
                fileName,
                fileSize,
                rowCount,
                EMPTY_MIN_KEY,
                EMPTY_MAX_KEY,
                EMPTY_KEY_STATS,
                rowStats,
                minSequenceNumber,
                maxSequenceNumber,
                schemaId,
                DUMMY_LEVEL,
                keyMetadata);
    }

    public DataFileMeta(
            String fileName,
            long fileSize,
            long rowCount,
            BinaryRow minKey,
            BinaryRow maxKey,
            BinaryTableStats keyStats,
            BinaryTableStats valueStats,
            long minSequenceNumber,
            long maxSequenceNumber,
            long schemaId,
            int level,
            KeyMetadata keyMetadata) {
        this(
                fileName,
                fileSize,
                rowCount,
                minKey,
                maxKey,
                keyStats,
                valueStats,
                minSequenceNumber,
                maxSequenceNumber,
                schemaId,
                level,
                Collections.emptyList(),
                Timestamp.fromLocalDateTime(LocalDateTime.now()).toMillisTimestamp(),
                keyMetadata);
    }

    public DataFileMeta(
            String fileName,
            long fileSize,
            long rowCount,
            BinaryRow minKey,
            BinaryRow maxKey,
            BinaryTableStats keyStats,
            BinaryTableStats valueStats,
            long minSequenceNumber,
            long maxSequenceNumber,
            long schemaId,
            int level,
            List<String> extraFiles,
            Timestamp creationTime,
            KeyMetadata keyMetadata) {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.rowCount = rowCount;

        this.minKey = minKey;
        this.maxKey = maxKey;
        this.keyStats = keyStats;
        this.valueStats = valueStats;

        this.minSequenceNumber = minSequenceNumber;
        this.maxSequenceNumber = maxSequenceNumber;
        this.level = level;
        this.schemaId = schemaId;
        this.extraFiles = Collections.unmodifiableList(extraFiles);
        this.creationTime = creationTime;
        this.keyMetadata = keyMetadata;
    }

    public String fileName() {
        return fileName;
    }

    public long fileSize() {
        return fileSize;
    }

    public long rowCount() {
        return rowCount;
    }

    public BinaryRow minKey() {
        return minKey;
    }

    public BinaryRow maxKey() {
        return maxKey;
    }

    public BinaryTableStats keyStats() {
        return keyStats;
    }

    public BinaryTableStats valueStats() {
        return valueStats;
    }

    public long minSequenceNumber() {
        return minSequenceNumber;
    }

    public long maxSequenceNumber() {
        return maxSequenceNumber;
    }

    public long schemaId() {
        return schemaId;
    }

    public int level() {
        return level;
    }

    public KeyMetadata keyMetadata() {
        return keyMetadata;
    }

    /**
     * Usage:
     *
     * <ul>
     *   <li>Paimon 0.2
     *       <ul>
     *         <li>Stores changelog files for {@link CoreOptions.ChangelogProducer#INPUT}. Changelog
     *             files are moved to {@link NewFilesIncrement} since Paimon 0.3.
     *       </ul>
     * </ul>
     */
    public List<String> extraFiles() {
        return extraFiles;
    }

    public Timestamp creationTime() {
        return creationTime;
    }

    public long creationTimeEpochMillis() {
        return creationTime
                .toLocalDateTime()
                .atZone(ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli();
    }

    public Optional<CoreOptions.FileFormatType> fileFormat() {
        String[] split = fileName.split("\\.");
        try {
            return Optional.of(
                    CoreOptions.FileFormatType.valueOf(split[split.length - 1].toUpperCase()));
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }

    public DataFileMeta upgrade(int newLevel) {
        checkArgument(newLevel > this.level);
        return new DataFileMeta(
                fileName,
                fileSize,
                rowCount,
                minKey,
                maxKey,
                keyStats,
                valueStats,
                minSequenceNumber,
                maxSequenceNumber,
                schemaId,
                newLevel,
                extraFiles,
                creationTime,
                keyMetadata);
    }

    public List<Path> collectFiles(DataFilePathFactory pathFactory) {
        List<Path> paths = new ArrayList<>();
        paths.add(pathFactory.toPath(fileName));
        extraFiles.forEach(f -> paths.add(pathFactory.toPath(f)));
        return paths;
    }

    public DataFileMeta copy(List<String> newExtraFiles) {
        return new DataFileMeta(
                fileName,
                fileSize,
                rowCount,
                minKey,
                maxKey,
                keyStats,
                valueStats,
                minSequenceNumber,
                maxSequenceNumber,
                schemaId,
                level,
                newExtraFiles,
                creationTime,
                keyMetadata);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof DataFileMeta)) {
            return false;
        }
        DataFileMeta that = (DataFileMeta) o;
        return Objects.equals(fileName, that.fileName)
                && fileSize == that.fileSize
                && rowCount == that.rowCount
                && Objects.equals(minKey, that.minKey)
                && Objects.equals(maxKey, that.maxKey)
                && Objects.equals(keyStats, that.keyStats)
                && Objects.equals(valueStats, that.valueStats)
                && minSequenceNumber == that.minSequenceNumber
                && maxSequenceNumber == that.maxSequenceNumber
                && schemaId == that.schemaId
                && level == that.level
                && Objects.equals(extraFiles, that.extraFiles)
                && Objects.equals(creationTime, that.creationTime)
                && Objects.equals(keyMetadata, that.keyMetadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                fileName,
                fileSize,
                rowCount,
                minKey,
                maxKey,
                keyStats,
                valueStats,
                minSequenceNumber,
                maxSequenceNumber,
                schemaId,
                level,
                extraFiles,
                creationTime,
                keyMetadata);
    }

    @Override
    public String toString() {
        return String.format(
                "{%s, %d, %d, %s, %s, %s, %s, %d, %d, %d, %d, %s, %s,%s}",
                fileName,
                fileSize,
                rowCount,
                minKey,
                maxKey,
                keyStats,
                valueStats,
                minSequenceNumber,
                maxSequenceNumber,
                schemaId,
                level,
                extraFiles,
                creationTime,
                keyMetadata);
    }

    public static RowType schema() {
        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(0, "_FILE_NAME", newStringType(false)));
        fields.add(new DataField(1, "_FILE_SIZE", new BigIntType(false)));
        fields.add(new DataField(2, "_ROW_COUNT", new BigIntType(false)));
        fields.add(new DataField(3, "_MIN_KEY", newBytesType(false)));
        fields.add(new DataField(4, "_MAX_KEY", newBytesType(false)));
        fields.add(new DataField(5, "_KEY_STATS", FieldStatsArraySerializer.schema()));
        fields.add(new DataField(6, "_VALUE_STATS", FieldStatsArraySerializer.schema()));
        fields.add(new DataField(7, "_MIN_SEQUENCE_NUMBER", new BigIntType(false)));
        fields.add(new DataField(8, "_MAX_SEQUENCE_NUMBER", new BigIntType(false)));
        fields.add(new DataField(9, "_SCHEMA_ID", new BigIntType(false)));
        fields.add(new DataField(10, "_LEVEL", new IntType(false)));
        fields.add(new DataField(11, "_EXTRA_FILES", new ArrayType(false, newStringType(false))));
        fields.add(new DataField(12, "_CREATION_TIME", DataTypes.TIMESTAMP_MILLIS()));
        fields.add(new DataField(13, "_KEY_METADATA", KeyMetadata.schema()));
        return new RowType(fields);
    }

    public static long getMaxSequenceNumber(List<DataFileMeta> fileMetas) {
        return fileMetas.stream()
                .map(DataFileMeta::maxSequenceNumber)
                .max(Long::compare)
                .orElse(-1L);
    }
}
