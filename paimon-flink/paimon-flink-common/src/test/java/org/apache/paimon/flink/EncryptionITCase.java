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

package org.apache.paimon.flink;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.orc.OrcReaderFactory;
import org.apache.paimon.format.parquet.ParquetUtil;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for encrypting data file. */
public class EncryptionITCase extends CatalogITCaseBase {

    private static final String TABLE_NAME = "test_encryption";

    @ParameterizedTest
    @ValueSource(strings = {"orc", "parquet"})
    public void testEncryptionTableWithPK(String format) throws IOException {
        sql(
                String.format(
                        "CREATE TABLE %s "
                                + " (\n"
                                + "  `id` INT PRIMARY KEY NOT ENFORCED,"
                                + "  `name` STRING"
                                + ") WITH (\n"
                                + "  'encryption.mechanism' = 'envelope',\n"
                                + "  'encryption.kms-client' = 'memory',\n"
                                + "  'file.format' = '%s'\n"
                                + ")",
                        TABLE_NAME, format));

        // generate 5 snapshots, trigger the compact.
        sql("INSERT INTO " + TABLE_NAME + " SELECT 1,'aaa'");
        sql("INSERT INTO " + TABLE_NAME + " SELECT 2,'bbb'");
        sql("INSERT INTO " + TABLE_NAME + " SELECT 3,'ccc'");
        sql("INSERT INTO " + TABLE_NAME + " SELECT 4,'ddd'");
        sql("INSERT INTO " + TABLE_NAME + " SELECT 5,'eee'");

        // read the encrypted data.
        List<Row> result = sql("SELECT * FROM " + TABLE_NAME);
        Assertions.assertEquals(
                "[+I[1, aaa], +I[2, bbb], +I[3, ccc], +I[4, ddd], +I[5, eee]]", result.toString());

        Consumer<InternalRow> action =
                row -> {
                    assertThat(row.isNullAt(0)).isTrue();
                    assertThat(row.isNullAt(1)).isTrue();
                };

        assertDataFileEncrypted(format, action);
    }

    @ParameterizedTest
    @ValueSource(strings = {"orc", "parquet"})
    public void testEncryptionAppendOnlyTable(String format) throws IOException {
        sql(
                String.format(
                        "CREATE TABLE %s "
                                + " (\n"
                                + "  `id` INT,"
                                + "  `name` STRING"
                                + ") WITH (\n"
                                + "  'encryption.mechanism' = 'envelope',\n"
                                + "  'encryption.kms-client' = 'memory',\n"
                                + "  'file.format' = '%s'\n"
                                + ")",
                        TABLE_NAME, format));

        sql("INSERT INTO " + TABLE_NAME + " SELECT 1,'aaa'");

        // read the encrypted data.
        List<Row> result = sql("SELECT * FROM " + TABLE_NAME);
        Assertions.assertEquals("[+I[1, aaa]]", result.toString());

        // assert the data file is encrypted.
        Consumer<InternalRow> action =
                row -> {
                    assertThat(row.isNullAt(0)).isTrue();
                    assertThat(row.isNullAt(1)).isTrue();
                };
        assertDataFileEncrypted(format, action);
    }

    @Test
    public void testPartialColumnsEncryption() throws IOException {
        sql(
                String.format(
                        "CREATE TABLE %s "
                                + " (\n"
                                + "  `id` INT,"
                                + "  `name` STRING"
                                + ") WITH (\n"
                                + "  'encryption.mechanism' = 'envelope',\n"
                                + "  'encryption.kms-client' = 'memory',\n"
                                + "  'encryption.columns' = 'id'\n"
                                + ")",
                        TABLE_NAME));

        sql("INSERT INTO " + TABLE_NAME + " SELECT 1,'aaa'");

        // read the encrypted data.
        List<Row> result = sql("SELECT * FROM " + TABLE_NAME);
        Assertions.assertEquals("[+I[1, aaa]]", result.toString());

        // assert the data file is encrypted.

        Consumer<InternalRow> action =
                row -> {
                    assertThat(row.isNullAt(0)).isTrue();
                    assertThat(row.isNullAt(1)).isFalse();
                };

        assertDataFileEncrypted("orc", action);
    }

    private void assertDataFileEncrypted(String format, Consumer<InternalRow> action)
            throws IOException {
        // assert the data file is encrypted.
        DataTable table = (DataTable) getPaimonTable(TABLE_NAME);
        FileIO fileIO = table.fileIO();
        Path tablePath = table.location();
        FileStatus[] fileStatuses = fileIO.listStatus(new Path(tablePath.toString() + "/bucket-0"));
        Path dataFilePath = fileStatuses[0].getPath();
        if (format.equals("parquet")) {
            assertThatThrownBy(() -> ParquetUtil.getParquetReader(fileIO, dataFilePath, null))
                    .hasMessage("Trying to read file with encrypted footer. No keys available");
        } else if (format.equals("orc")) {
            RowType rowType =
                    RowType.builder()
                            .fields(
                                    new DataType[] {DataTypes.INT(), DataTypes.STRING()},
                                    new String[] {"id", "name"})
                            .build();

            forEach(rowType, dataFilePath, action);
        }
    }

    private void forEach(RowType readType, Path path, Consumer<InternalRow> action)
            throws IOException {
        OrcReaderFactory orcReaderFactory =
                new OrcReaderFactory(new Configuration(), readType, new ArrayList<>(), 10);
        FileFormatFactory.FormatContext formatContext =
                FileFormatFactory.formatContextBuilder().build();
        RecordReader<InternalRow> reader =
                orcReaderFactory.createReader(new LocalFileIO(), path, formatContext);
        reader.forEachRemaining(action);
    }
}
