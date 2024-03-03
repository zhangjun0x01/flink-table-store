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

import org.apache.paimon.KeyValue;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.encryption.EncryptionManager;
import org.apache.paimon.encryption.KmsClient;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.mergetree.MergeSorter;
import org.apache.paimon.mergetree.MergeTreeReaders;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;

import java.util.Comparator;
import java.util.List;

/** Default {@link CompactRewriter} for merge trees. */
public class MergeTreeCompactRewriter extends AbstractCompactRewriter {

    protected final KeyValueFileReaderFactory readerFactory;
    protected final KeyValueFileWriterFactory writerFactory;
    protected final Comparator<InternalRow> keyComparator;
    protected final MergeFunctionFactory<KeyValue> mfFactory;
    protected final MergeSorter mergeSorter;
    protected final EncryptionManager encryptionManager;
    protected final KmsClient.CreateKeyResult createKeyResult;
    protected final String encryptionColumns;

    public MergeTreeCompactRewriter(
            KeyValueFileReaderFactory readerFactory,
            KeyValueFileWriterFactory writerFactory,
            Comparator<InternalRow> keyComparator,
            MergeFunctionFactory<KeyValue> mfFactory,
            MergeSorter mergeSorter,
            EncryptionManager encryptionManager,
            KmsClient.CreateKeyResult createKeyResult,
            String encryptionColumns) {
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;
        this.keyComparator = keyComparator;
        this.mfFactory = mfFactory;
        this.mergeSorter = mergeSorter;
        this.encryptionManager = encryptionManager;
        this.createKeyResult = createKeyResult;
        this.encryptionColumns = encryptionColumns;
    }

    @Override
    public CompactResult rewrite(
            int outputLevel, boolean dropDelete, List<List<SortedRun>> sections) throws Exception {
        return rewriteCompaction(
                outputLevel,
                dropDelete,
                sections,
                encryptionManager,
                createKeyResult,
                encryptionColumns);
    }

    protected CompactResult rewriteCompaction(
            int outputLevel,
            boolean dropDelete,
            List<List<SortedRun>> sections,
            EncryptionManager encryptionManager,
            KmsClient.CreateKeyResult createKeyResult,
            String encryptionColumns)
            throws Exception {
        RollingFileWriter<KeyValue, DataFileMeta> writer =
                writerFactory.createRollingMergeTreeFileWriter(
                        outputLevel, encryptionManager, createKeyResult);
        RecordReader<KeyValue> sectionsReader =
                MergeTreeReaders.readerForMergeTree(
                        sections,
                        dropDelete,
                        readerFactory,
                        keyComparator,
                        mfFactory.create(),
                        mergeSorter,
                        encryptionManager,
                        encryptionColumns);
        writer.write(new RecordReaderIterator<>(sectionsReader));
        writer.close();
        return new CompactResult(extractFilesFromSections(sections), writer.result());
    }
}
