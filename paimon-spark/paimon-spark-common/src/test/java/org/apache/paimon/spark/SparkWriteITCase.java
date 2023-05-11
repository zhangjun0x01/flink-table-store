/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.spark;

import org.apache.paimon.fs.Path;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Comparator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for spark reader. */
public class SparkWriteITCase {

    private static SparkSession spark = null;

    @BeforeAll
    public static void startMetastoreAndSpark(@TempDir java.nio.file.Path tempDir) {
        Path warehousePath = new Path("file:" + tempDir.toString());
        spark = SparkSession.builder().master("local[2]").getOrCreate();
        spark.conf().set(SQLConf.PARTITION_OVERWRITE_MODE().key(), "dynamic");
        spark.conf().set("spark.sql.catalog.paimon", SparkCatalog.class.getName());
        spark.conf().set("spark.sql.catalog.paimon.warehouse", warehousePath.toString());
        spark.sql("CREATE DATABASE paimon.db");
        spark.sql("USE paimon.db");
    }

    @AfterAll
    public static void stopMetastoreAndSpark() {
        if (spark != null) {
            spark.stop();
            spark = null;
        }
    }

    @AfterEach
    public void afterEach() {
        spark.sql("DROP TABLE T");
    }

    @Test
    public void testWrite() {
        spark.sql(
                "CREATE TABLE T (a INT, b INT, c STRING) TBLPROPERTIES"
                        + " ('primary-key'='a', 'bucket'='4', 'file.format'='avro')");
        innerSimpleWrite();
    }

    @Test
    public void testInsertOverwrite() {
//        spark.sql(
//                "CREATE TABLE T (a INT, b INT, c STRING) TBLPROPERTIES"
//                        + " ('primary-key'='a')");
//        spark.sql("INSERT INTO T VALUES(1,1,'aaa'),(2,2,'bbb')");
//        List<Row> rows = spark.sql("SELECT * FROM T").collectAsList();
//        assertThat(rows.toString()).isEqualTo("[[1,1,aaa], [2,2,bbb]]");
//        spark.sql("INSERT OVERWRITE T VALUES(3,3,'ccc')");


        spark.sql("CREATE TABLE T (\n" +
                "    user_id BIGINT,\n" +
                "    dt STRING\n" +
                ") PARTITIONED BY (dt)");
        spark.sql("insert overwrite T PARTITION(dt= '1') select 2 ");
    }

    @Test
    public void testWritePartitionTable() {
        spark.sql(
                "CREATE TABLE T (a INT, b INT, c STRING) PARTITIONED BY (a) TBLPROPERTIES"
                        + " ('primary-key'='a,b', 'bucket'='4', 'file.format'='avro')");
        innerSimpleWrite();
    }

    private void innerSimpleWrite() {
        spark.sql("INSERT INTO T VALUES (1, 2, '3')").collectAsList();
        List<Row> rows = spark.sql("SELECT * FROM T").collectAsList();
        assertThat(rows.toString()).isEqualTo("[[1,2,3]]");

        spark.sql("INSERT INTO T VALUES (4, 5, '6')").collectAsList();
        spark.sql("INSERT INTO T VALUES (1, 2, '7')").collectAsList();
        spark.sql("INSERT INTO T VALUES (4, 5, '8')").collectAsList();
        rows = spark.sql("SELECT * FROM T").collectAsList();
        rows.sort(Comparator.comparingInt(o -> o.getInt(0)));
        assertThat(rows.toString()).isEqualTo("[[1,2,7], [4,5,8]]");

        spark.sql("DELETE FROM T WHERE a=1").collectAsList();
        rows = spark.sql("SELECT * FROM T").collectAsList();
        assertThat(rows.toString()).isEqualTo("[[4,5,8]]");

        spark.sql("DELETE FROM T").collectAsList();
        rows = spark.sql("SELECT * FROM T").collectAsList();
        assertThat(rows.toString()).isEqualTo("[]");
    }

    @Test
    public void testDeleteWhereNonePk() {
        spark.sql(
                "CREATE TABLE T (a INT, b INT, c STRING) TBLPROPERTIES"
                        + " ('primary-key'='a', 'file.format'='avro')");
        spark.sql("INSERT INTO T VALUES (1, 11, '111'), (2, 22, '222')").collectAsList();
        spark.sql("DELETE FROM T WHERE b=11").collectAsList();
        List<Row> rows = spark.sql("SELECT * FROM T").collectAsList();
        assertThat(rows.toString()).isEqualTo("[[2,22,222]]");
    }
}
