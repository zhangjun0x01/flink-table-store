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

package org.apache.paimon.hive;

import org.apache.paimon.catalog.AbstractCatalog;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogLock;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalog;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.hive.runner.PaimonEmbeddedHiveRunner;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.ExceptionUtils;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/** IT cases for using Paimon {@link HiveCatalog} together with Paimon Hive connector. */
@RunWith(PaimonEmbeddedHiveRunner.class)
public abstract class HiveCatalogITCaseBase {

    @Rule public TemporaryFolder folder = new TemporaryFolder();

    protected String path;
    protected TableEnvironment tEnv;

    @HiveSQL(files = {})
    protected static HiveShell hiveShell;

    @Before
    public void before() throws Exception {
        hiveShell.execute("CREATE DATABASE IF NOT EXISTS test_db");
        hiveShell.execute("USE test_db");
        hiveShell.execute("CREATE TABLE hive_table ( a INT, b STRING )");
        hiveShell.execute("INSERT INTO hive_table VALUES (100, 'Hive'), (200, 'Table')");
        hiveShell.executeQuery("SHOW TABLES");

        path = folder.newFolder().toURI().toString();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        tEnv = TableEnvironmentImpl.create(settings);
        tEnv.executeSql(
                        String.join(
                                "\n",
                                "CREATE CATALOG my_hive WITH (",
                                "  'type' = 'paimon',",
                                "  'metastore' = 'hive',",
                                "  'uri' = '',",
                                "  'warehouse' = '" + path + "',",
                                "  'lock.enabled' = 'true'",
                                ")"))
                .await();
        tEnv.executeSql("USE CATALOG my_hive").await();
        tEnv.executeSql("USE test_db").await();
    }

    @After
    public void after() {
        hiveShell.execute("DROP DATABASE IF EXISTS test_db CASCADE");
        hiveShell.execute("DROP DATABASE IF EXISTS test_db2 CASCADE");
    }

    @Test
    public void testDatabaseOperations() throws Exception {
        // create database
        tEnv.executeSql("CREATE DATABASE test_db2").await();
        Assert.assertEquals(
                Arrays.asList(Row.of("default"), Row.of("test_db"), Row.of("test_db2")),
                collect("SHOW DATABASES"));
        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS test_db2").await();
        try {
            tEnv.executeSql("CREATE DATABASE test_db2").await();
            Assert.fail("No exception is thrown");
        } catch (Throwable t) {
            ExceptionUtils.assertThrowableWithMessage(
                    t, "Database test_db2 already exists in Catalog my_hive");
        }

        // drop database
        tEnv.executeSql("DROP DATABASE test_db2").await();
        Assert.assertEquals(
                Arrays.asList(Row.of("default"), Row.of("test_db")), collect("SHOW DATABASES"));
        tEnv.executeSql("DROP DATABASE IF EXISTS test_db2").await();
        try {
            tEnv.executeSql("DROP DATABASE test_db2").await();
            Assert.fail("No exception is thrown");
        } catch (Throwable t) {
            ExceptionUtils.assertThrowableWithMessage(
                    t, "Database test_db2 does not exist in Catalog my_hive");
        }

        // drop non-empty database
        tEnv.executeSql("CREATE DATABASE test_db2").await();
        tEnv.executeSql("USE test_db2").await();
        tEnv.executeSql("CREATE TABLE t ( a INT, b STRING ) WITH ( 'file.format' = 'avro' )")
                .await();
        tEnv.executeSql("INSERT INTO t VALUES (1, 'Hi'), (2, 'Hello')").await();
        Path tablePath = new Path(path, "test_db2.db/t");
        Assert.assertTrue(tablePath.getFileSystem().exists(tablePath));
        try {
            tEnv.executeSql("DROP DATABASE test_db2").await();
            Assert.fail("No exception is thrown");
        } catch (Throwable t) {
            ExceptionUtils.assertThrowableWithMessage(
                    t, "Database test_db2 in catalog my_hive is not empty");
        }
        tEnv.executeSql("DROP DATABASE test_db2 CASCADE").await();
        Assert.assertEquals(
                Arrays.asList(Row.of("default"), Row.of("test_db")), collect("SHOW DATABASES"));
        Assert.assertFalse(tablePath.getFileSystem().exists(tablePath));
    }

    @Test
    public void testTableOperations() throws Exception {
        // create table
        tEnv.executeSql("CREATE TABLE t ( a INT, b STRING ) WITH ( 'file.format' = 'avro' )")
                .await();
        tEnv.executeSql("CREATE TABLE s ( a INT, b STRING ) WITH ( 'file.format' = 'avro' )")
                .await();
        Assert.assertEquals(Arrays.asList(Row.of("s"), Row.of("t")), collect("SHOW TABLES"));
        tEnv.executeSql(
                        "CREATE TABLE IF NOT EXISTS s ( a INT, b STRING ) WITH ( 'file.format' = 'avro' )")
                .await();
        try {
            tEnv.executeSql("CREATE TABLE s ( a INT, b STRING ) WITH ( 'file.format' = 'avro' )")
                    .await();
            Assert.fail("No exception is thrown");
        } catch (Throwable t) {
            ExceptionUtils.assertThrowableWithMessage(
                    t, "Table (or view) test_db.s already exists in Catalog my_hive");
        }

        // drop table
        tEnv.executeSql("INSERT INTO s VALUES (1, 'Hi'), (2, 'Hello')").await();
        Path tablePath = new Path(path, "test_db.db/s");
        Assert.assertTrue(tablePath.getFileSystem().exists(tablePath));
        tEnv.executeSql("DROP TABLE s").await();
        Assert.assertEquals(Collections.singletonList(Row.of("t")), collect("SHOW TABLES"));
        Assert.assertFalse(tablePath.getFileSystem().exists(tablePath));
        tEnv.executeSql("DROP TABLE IF EXISTS s").await();
        try {
            tEnv.executeSql("DROP TABLE s").await();
            Assert.fail("No exception is thrown");
        } catch (Throwable t) {
            ExceptionUtils.assertThrowableWithMessage(
                    t, "Table with identifier 'my_hive.test_db.s' does not exist");
        }
        try {
            tEnv.executeSql("DROP TABLE hive_table").await();
            Assert.fail("No exception is thrown");
        } catch (Throwable t) {
            ExceptionUtils.assertThrowableWithMessage(
                    t, "Table test_db.hive_table is not a paimon table");
        }

        // alter table
        tEnv.executeSql("ALTER TABLE t SET ( 'manifest.target-file-size' = '16MB' )").await();
        List<Row> actual = collect("SHOW CREATE TABLE t");
        Assert.assertEquals(1, actual.size());
        Assert.assertTrue(
                actual.get(0)
                        .getField(0)
                        .toString()
                        .contains("'manifest.target-file-size' = '16MB'"));
        try {
            tEnv.executeSql("ALTER TABLE s SET ( 'manifest.target-file-size' = '16MB' )").await();
            Assert.fail("No exception is thrown");
        } catch (Throwable t) {
            ExceptionUtils.assertThrowableWithMessage(
                    t, "Table `my_hive`.`test_db`.`s` doesn't exist or is a temporary table");
        }
        try {
            tEnv.executeSql("ALTER TABLE hive_table SET ( 'manifest.target-file-size' = '16MB' )")
                    .await();
            Assert.fail("No exception is thrown");
        } catch (Throwable t) {
            ExceptionUtils.assertThrowableWithMessage(
                    t, "Table test_db.hive_table is not a paimon table");
        }
    }

    @Test
    public void testCreateExternalTable() throws Exception {
        tEnv.executeSql(
                        String.join(
                                "\n",
                                "CREATE CATALOG my_hive_external WITH (",
                                "  'type' = 'paimon',",
                                "  'metastore' = 'hive',",
                                "  'uri' = '',",
                                "  'warehouse' = '" + path + "',",
                                "  'lock.enabled' = 'true',",
                                "  'table.type' = 'EXTERNAL'",
                                ")"))
                .await();
        tEnv.executeSql("USE CATALOG my_hive_external").await();
        tEnv.executeSql("USE test_db").await();
        tEnv.executeSql("CREATE TABLE t ( a INT, b STRING ) WITH ( 'file.format' = 'avro' )")
                .await();
        Assert.assertTrue(
                hiveShell
                        .executeQuery("DESC FORMATTED t")
                        .contains("Table Type:         \tEXTERNAL_TABLE      \tNULL"));
        tEnv.executeSql("DROP TABLE t").await();
        Path tablePath = new Path(path, "test_db.db/t");
        Assert.assertFalse(tablePath.getFileSystem().exists(tablePath));
    }

    @Test
    public void testFlinkWriteAndHiveRead() throws Exception {
        tEnv.executeSql("CREATE TABLE t ( a INT, b STRING ) WITH ( 'file.format' = 'avro' )")
                .await();
        tEnv.executeSql("INSERT INTO t VALUES (1, 'Hi'), (2, 'Hello')").await();
        Assert.assertEquals(
                Arrays.asList("1\tHi", "2\tHello"),
                hiveShell.executeQuery("SELECT * FROM t ORDER BY a"));

        try {
            tEnv.executeSql("INSERT INTO hive_table VALUES (1, 'Hi'), (2, 'Hello')").await();
            Assert.fail("No exception is thrown");
        } catch (Throwable t) {
            ExceptionUtils.assertThrowableWithMessage(
                    t, "Table test_db.hive_table is not a paimon table");
        }
    }

    @Test
    public void testHiveCreateAndFlinkRead() throws Exception {
        hiveShell.execute("SET hive.metastore.warehouse.dir=" + path);
        hiveShell.execute(
                "CREATE TABLE hive_test_table ( a INT, b STRING ) "
                        + "STORED BY '"
                        + PaimonStorageHandler.class.getName()
                        + "'");
        hiveShell.execute("INSERT INTO hive_test_table VALUES (1, 'Apache'), (2, 'Paimon')");
        List<Row> actual = collect("SELECT * FROM hive_test_table");
        Assertions.assertThat(actual).contains(Row.of(1, "Apache"), Row.of(2, "Paimon"));
    }

    @Test
    public void testCreateTableAs() throws Exception {
        tEnv.executeSql("CREATE TABLE t (a INT)").await();
        tEnv.executeSql("INSERT INTO t VALUES(1)").await();
        tEnv.executeSql("CREATE TABLE t1 AS SELECT * FROM t").await();
        List<Row> result = collect("SELECT * FROM t1$schemas s");
        Assertions.assertThat(result.toString())
                .isEqualTo("[+I[0, [{\"id\":0,\"name\":\"a\",\"type\":\"INT\"}], [], [], {}, ]]");
        List<Row> data = collect("SELECT * FROM t1");
        Assertions.assertThat(data).contains(Row.of(1));

        // change option
        tEnv.executeSql("CREATE TABLE t_option (a INT)").await();
        tEnv.executeSql("INSERT INTO t_option VALUES(1)").await();
        tEnv.executeSql(
                        "CREATE TABLE t1_option WITH ('file.format' = 'parquet') AS SELECT * FROM t_option")
                .await();
        List<Row> resultOption = collect("SELECT * FROM t1_option$options");
        Assertions.assertThat(resultOption).containsExactly(Row.of("file.format", "parquet"));
        List<Row> dataOption = collect("SELECT * FROM t1_option");
        Assertions.assertThat(dataOption).contains(Row.of(1));

        // partition table
        tEnv.executeSql(
                "CREATE TABLE t_p (\n"
                        + "    user_id BIGINT,\n"
                        + "    item_id BIGINT,\n"
                        + "    behavior STRING,\n"
                        + "    dt STRING,\n"
                        + "    hh STRING\n"
                        + ") PARTITIONED BY (dt, hh)");
        tEnv.executeSql("INSERT INTO t_p  SELECT 1,2,'a','2023-02-19','12'").await();
        tEnv.executeSql("CREATE TABLE t1_p WITH ('partition' = 'dt') AS SELECT * FROM t_p").await();
        List<Row> resultPartition = collect("SELECT * FROM t1_p$schemas s");
        Assertions.assertThat(resultPartition.toString())
                .isEqualTo(
                        "[+I[0, [{\"id\":0,\"name\":\"user_id\",\"type\":\"BIGINT\"},{\"id\":1,\"name\":\"item_id\",\"type\":\"BIGINT\"},{\"id\":2,\"name\":\"behavior\",\"type\":\"STRING\"}"
                                + ",{\"id\":3,\"name\":\"dt\",\"type\":\"STRING\"},{\"id\":4,\"name\":\"hh\",\"type\":\"STRING\"}], [\"dt\"], [], {}, ]]");
        List<Row> dataPartition = collect("SELECT * FROM t1_p");
        Assertions.assertThat(dataPartition.toString()).isEqualTo("[+I[1, 2, a, 2023-02-19, 12]]");

        // primary key
        tEnv.executeSql(
                        "CREATE TABLE t_pk (\n"
                                + "    user_id BIGINT,\n"
                                + "    item_id BIGINT,\n"
                                + "    behavior STRING,\n"
                                + "    dt STRING,\n"
                                + "    hh STRING,\n"
                                + "    PRIMARY KEY (dt, hh, user_id) NOT ENFORCED\n"
                                + ")")
                .await();
        tEnv.executeSql("INSERT INTO t_pk VALUES(1,2,'aaa','2020-01-02','09')").await();
        tEnv.executeSql("CREATE TABLE t_pk_as WITH ('primary-key' = 'dt') AS SELECT * FROM t_pk")
                .await();
        List<Row> resultPk = collect("SHOW CREATE TABLE t_pk_as");
        Assertions.assertThat(resultPk.toString()).contains("PRIMARY KEY (`dt`)");
        List<Row> dataPk = collect("SELECT * FROM t_pk_as");
        Assertions.assertThat(dataPk.toString()).isEqualTo("[+I[1, 2, aaa, 2020-01-02, 09]]");

        // primary key + partition
        tEnv.executeSql(
                        "CREATE TABLE t_all (\n"
                                + "    user_id BIGINT,\n"
                                + "    item_id BIGINT,\n"
                                + "    behavior STRING,\n"
                                + "    dt STRING,\n"
                                + "    hh STRING,\n"
                                + "    PRIMARY KEY (dt, hh, user_id) NOT ENFORCED\n"
                                + ") PARTITIONED BY (dt, hh)")
                .await();
        tEnv.executeSql("INSERT INTO t_all VALUES(1,2,'login','2020-01-02','09')").await();
        tEnv.executeSql(
                        "CREATE TABLE t_all_as WITH ('primary-key' = 'dt,hh' , 'partition' = 'dt' ) AS SELECT * FROM t_all")
                .await();
        List<Row> resultAll = collect("SHOW CREATE TABLE t_all_as");
        Assertions.assertThat(resultAll.toString()).contains("PRIMARY KEY (`dt`, `hh`)");
        Assertions.assertThat(resultAll.toString()).contains("PARTITIONED BY (`dt`)");
        List<Row> dataAll = collect("SELECT * FROM t_all_as");
        Assertions.assertThat(dataAll.toString()).isEqualTo("[+I[1, 2, login, 2020-01-02, 09]]");

        // primary key do not exist.
        tEnv.executeSql(
                        "CREATE TABLE t_pk_not_exist (\n"
                                + "    user_id BIGINT,\n"
                                + "    item_id BIGINT,\n"
                                + "    behavior STRING,\n"
                                + "    dt STRING,\n"
                                + "    hh STRING,\n"
                                + "    PRIMARY KEY (dt, hh, user_id) NOT ENFORCED\n"
                                + ")")
                .await();

        Assertions.assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                                "CREATE TABLE t_pk_not_exist_as WITH ('primary-key' = 'aaa') AS SELECT * FROM t_pk_not_exist")
                                        .await())
                .hasRootCauseMessage("Primary key column '[aaa]' is not defined in the schema.");

        // primary key in option and DDL.
        Assertions.assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                                "CREATE TABLE t_pk_ddl_option ("
                                                        + "                            user_id BIGINT,"
                                                        + "                            item_id BIGINT,"
                                                        + "                            behavior STRING,"
                                                        + "                            dt STRING,"
                                                        + "                            hh STRING,"
                                                        + "                            PRIMARY KEY (dt, hh, user_id) NOT ENFORCED"
                                                        + "                        ) WITH ('primary-key' = 'dt')")
                                        .await())
                .hasRootCauseMessage(
                        "Cannot define primary key on DDL and table options at the same time.");

        // partition do not exist.
        tEnv.executeSql(
                        "CREATE TABLE t_partition_not_exist (\n"
                                + "    user_id BIGINT,\n"
                                + "    item_id BIGINT,\n"
                                + "    behavior STRING,\n"
                                + "    dt STRING,\n"
                                + "    hh STRING\n"
                                + ") PARTITIONED BY (dt, hh) ")
                .await();

        Assertions.assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                                "CREATE TABLE t_partition_not_exist_as WITH ('partition' = 'aaa') AS SELECT * FROM t_partition_not_exist")
                                        .await())
                .hasRootCauseMessage("Partition column '[aaa]' is not defined in the schema.");

        // partition in option and DDL.
        Assertions.assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                                "CREATE TABLE t_partition_ddl_option ("
                                                        + "                            user_id BIGINT,"
                                                        + "                            item_id BIGINT,"
                                                        + "                            behavior STRING,"
                                                        + "                            dt STRING,"
                                                        + "                            hh STRING"
                                                        + "                        ) PARTITIONED BY (dt, hh)  WITH ('partition' = 'dt')")
                                        .await())
                .hasRootCauseMessage(
                        "Cannot define partition on DDL and table options at the same time.");
    }

    @Test
    public void testRenameTable() throws Exception {
        tEnv.executeSql("CREATE TABLE t1 (a INT)").await();
        tEnv.executeSql("CREATE TABLE t2 (a INT)").await();
        tEnv.executeSql("INSERT INTO t1 SELECT 1");
        // the source table do not exist.
        assertThatThrownBy(() -> tEnv.executeSql("ALTER TABLE t3 RENAME TO t4"))
                .hasMessage(
                        "Table `my_hive`.`test_db`.`t3` doesn't exist or is a temporary table.");

        // the target table has existed.
        assertThatThrownBy(() -> tEnv.executeSql("ALTER TABLE t1 RENAME TO t2"))
                .hasMessage(
                        "Could not execute ALTER TABLE my_hive.test_db.t1 RENAME TO my_hive.test_db.t2");

        // the target table name has upper case.
        assertThatThrownBy(() -> tEnv.executeSql("ALTER TABLE t1 RENAME TO T1"))
                .hasMessage(
                        "Could not execute ALTER TABLE my_hive.test_db.t1 RENAME TO my_hive.test_db.T1");

        tEnv.executeSql("ALTER TABLE t1 RENAME TO t3").await();
        List<String> tables = hiveShell.executeQuery("SHOW TABLES");
        Assert.assertTrue(tables.contains("t3"));
        Assert.assertFalse(tables.contains("t1"));

        Identifier identifier = new Identifier("test_db", "t3");
        Catalog catalog =
                ((FlinkCatalog) tEnv.getCatalog(tEnv.getCurrentCatalog()).get()).catalog();
        org.apache.paimon.fs.Path tablePath =
                ((AbstractCatalog) catalog).getDataTableLocation(identifier);
        Assert.assertEquals(tablePath.toString(), path + "test_db.db" + File.separator + "t3");

        // TODO: the hiverunner (4.0) has a bug ,it can not rename the table path correctly ,
        // we should upgrade it to the 6.0 later ,and  update the test case for query.
        assertThatThrownBy(() -> tEnv.executeSql("SELECT * FROM t3"))
                .hasMessageContaining("SQL validation failed. There is no paimond in");
    }

    @Test
    public void testHiveLock() throws InterruptedException {
        tEnv.executeSql("CREATE TABLE t (a INT)");
        CatalogLock.Factory lockFactory =
                ((FlinkCatalog) tEnv.getCatalog(tEnv.getCurrentCatalog()).get())
                        .catalog()
                        .lockFactory()
                        .get();

        AtomicInteger count = new AtomicInteger(0);
        List<Thread> threads = new ArrayList<>();
        Callable<Void> unsafeIncrement =
                () -> {
                    int nextCount = count.get() + 1;
                    Thread.sleep(1);
                    count.set(nextCount);
                    return null;
                };
        for (int i = 0; i < 10; i++) {
            Thread thread =
                    new Thread(
                            () -> {
                                CatalogLock lock = lockFactory.create();
                                for (int j = 0; j < 10; j++) {
                                    try {
                                        lock.runWithLock("test_db", "t", unsafeIncrement);
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                }
                            });
            thread.start();
            threads.add(thread);
        }

        for (Thread thread : threads) {
            thread.join();
        }

        assertThat(count.get()).isEqualTo(100);
    }

    @Test
    public void testUpperCase() {
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                                "CREATE TABLE T ( a INT, b STRING ) WITH ( 'file.format' = 'avro' )")
                                        .await())
                .hasRootCauseMessage(
                        String.format(
                                "Table name[%s] cannot contain upper case in hive catalog", "T"));

        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                                "CREATE TABLE t (A INT, b STRING, C STRING) WITH ( 'file.format' = 'avro')")
                                        .await())
                .hasRootCauseMessage(
                        String.format(
                                "Field names %s cannot contain upper case in hive catalog",
                                "[A, C]"));
    }

    @Test
    public void testQuickPathInShowTables() throws Exception {
        collect("CREATE TABLE t ( a INT, b STRING )");
        List<Row> tables = collect("SHOW TABLES");
        Assert.assertEquals("[+I[t]]", tables.toString());

        new LocalFileIO().delete(new org.apache.paimon.fs.Path(path, "test_db.db/t"), true);
        tables = collect("SHOW TABLES");
        Assert.assertEquals("[]", tables.toString());
    }

    @Test
    public void testCatalogOptionsInheritAndOverride() throws Exception {
        tEnv.executeSql(
                        String.join(
                                "\n",
                                "CREATE CATALOG my_hive_options WITH (",
                                "  'type' = 'paimon',",
                                "  'metastore' = 'hive',",
                                "  'uri' = '',",
                                "  'warehouse' = '" + path + "',",
                                "  'lock.enabled' = 'true',",
                                "  'table-default.opt1' = 'value1',",
                                "  'table-default.opt2' = 'value2',",
                                "  'table-default.opt3' = 'value3'",
                                ")"))
                .await();
        tEnv.executeSql("USE CATALOG my_hive_options").await();

        // check inherit
        tEnv.executeSql("CREATE TABLE table_without_options (a INT, b STRING)").await();

        Identifier identifier = new Identifier("default", "table_without_options");
        Catalog catalog =
                ((FlinkCatalog) tEnv.getCatalog(tEnv.getCurrentCatalog()).get()).catalog();
        Map<String, String> tableOptions = catalog.getTable(identifier).options();

        Assertions.assertThat(tableOptions).containsEntry("opt1", "value1");
        Assertions.assertThat(tableOptions).containsEntry("opt2", "value2");
        Assertions.assertThat(tableOptions).containsEntry("opt3", "value3");
        Assertions.assertThat(tableOptions).doesNotContainKey("lock.enabled");

        // check override
        tEnv.executeSql(
                        "CREATE TABLE table_with_options (a INT, b STRING) WITH ('opt1' = 'new_value')")
                .await();
        identifier = new Identifier("default", "table_with_options");
        tableOptions = catalog.getTable(identifier).options();

        Assertions.assertThat(tableOptions).containsEntry("opt1", "new_value");
        Assertions.assertThat(tableOptions).containsEntry("opt2", "value2");
        Assertions.assertThat(tableOptions).containsEntry("opt3", "value3");
        Assertions.assertThat(tableOptions).doesNotContainKey("lock.enabled");
    }

    protected List<Row> collect(String sql) throws Exception {
        List<Row> result = new ArrayList<>();
        try (CloseableIterator<Row> it = tEnv.executeSql(sql).collect()) {
            while (it.hasNext()) {
                result.add(it.next());
            }
        }
        return result;
    }
}
