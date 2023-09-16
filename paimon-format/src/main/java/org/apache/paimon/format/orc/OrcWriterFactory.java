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

package org.apache.paimon.format.orc;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.orc.writer.OrcBulkWriter;
import org.apache.paimon.format.orc.writer.Vectorizer;
import org.apache.paimon.fs.PositionOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.WriterImpl;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * A factory that creates an ORC {@link FormatWriter}. The factory takes a user supplied {@link
 * Vectorizer} implementation to convert the element into an {@link
 * org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch}.
 */
public class OrcWriterFactory implements FormatWriterFactory {

    private final Vectorizer<InternalRow> vectorizer;
    private final Properties writerProperties;
    private final Map<String, String> confMap;

    private OrcFile.WriterOptions writerOptions;
    private final CoreOptions coreOptions;

    /**
     * Creates a new OrcBulkWriterFactory using the provided Vectorizer implementation.
     *
     * @param vectorizer The vectorizer implementation to convert input record to a
     *     VectorizerRowBatch.
     */
    public OrcWriterFactory(Vectorizer<InternalRow> vectorizer) {
        this(vectorizer, new Configuration());
    }

    /**
     * Creates a new OrcBulkWriterFactory using the provided Vectorizer, Hadoop Configuration.
     *
     * @param vectorizer The vectorizer implementation to convert input record to a
     *     VectorizerRowBatch.
     */
    public OrcWriterFactory(Vectorizer<InternalRow> vectorizer, Configuration configuration) {
        this(vectorizer, new Properties(), configuration);
    }

    /**
     * Creates a new OrcBulkWriterFactory using the provided Vectorizer, Hadoop Configuration, ORC
     * writer properties.
     *
     * @param vectorizer The vectorizer implementation to convert input record to a
     *     VectorizerRowBatch.
     * @param writerProperties Properties that can be used in ORC WriterOptions.
     */
    public OrcWriterFactory(
            Vectorizer<InternalRow> vectorizer,
            Properties writerProperties,
            Configuration configuration) {
        this.vectorizer = checkNotNull(vectorizer);
        this.writerProperties = checkNotNull(writerProperties);
        this.confMap = new HashMap<>();

        // Todo: Replace the Map based approach with a better approach
        for (Map.Entry<String, String> entry : configuration) {
            confMap.put(entry.getKey(), entry.getValue());
        }
        coreOptions = new CoreOptions(this.confMap);
    }

    @Override
    public FormatWriter create(
            PositionOutputStream out, FileFormatFactory.FormatContext formatContext)
            throws IOException {
        if (formatContext != null && formatContext.compression() != null) {
            writerProperties.setProperty(
                    OrcConf.COMPRESS.getAttribute(), formatContext.compression());
        }

        OrcFile.WriterOptions opts = getWriterOptions(formatContext);

        // use fsDataOutputStream to build PhysicalFsWriter.
        FSDataOutputStream fsDataOutputStream =
                new FSDataOutputStream(out, null) {
                    @Override
                    public void close() throws IOException {
                        // do nothing
                    }
                };

        // The path of the Writer is not used to indicate the destination file
        // in this case since we have used a dedicated physical writer to write
        // to the give output stream directly. However, the path would be used as
        // the key of writer in the ORC memory manager, thus we need to make it unique.
        Path unusedPath = new Path(UUID.randomUUID().toString());
        return new OrcBulkWriter(
                vectorizer,
                new WriterImpl(null, unusedPath, opts, formatContext, fsDataOutputStream),
                out,
                coreOptions.orcWriteBatch());
    }

    @VisibleForTesting
    protected OrcFile.WriterOptions getWriterOptions(
            FileFormatFactory.FormatContext formatContext) {
        Configuration conf = new ThreadLocalClassLoaderConfiguration();
        if (null == writerOptions) {
            for (Map.Entry<String, String> entry : confMap.entrySet()) {
                conf.set(entry.getKey(), entry.getValue());
            }

            writerOptions = OrcFile.writerOptions(writerProperties, conf);
            writerOptions.setSchema(this.vectorizer.getSchema());
        }

        //  enable the encryption.
        if (formatContext != null && formatContext.keyId() != null) {
            String encryptionColumns = formatContext.encryptionColumns();
            String encryption;
            if (encryptionColumns == null) {
                List<TypeDescription> typeDescriptions = vectorizer.getSchema().getChildren();
                String allColumns =
                        typeDescriptions.stream()
                                .map(TypeDescription::getFullFieldName)
                                .collect(Collectors.joining(","));
                encryption = formatContext.keyId() + ":" + allColumns;
            } else {
                encryption = formatContext.keyId() + ":" + encryptionColumns;
            }

            writerOptions.encrypt(encryption);
        }

        return writerOptions;
    }
}
