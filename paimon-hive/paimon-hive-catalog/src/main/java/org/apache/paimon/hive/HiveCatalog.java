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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.AbstractCatalog;
import org.apache.paimon.catalog.CatalogLock;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.options.OptionsUtils;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.TableType;
import org.apache.paimon.types.DataField;

import org.apache.flink.table.hive.LegacyHiveClasses;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.paimon.hive.HiveCatalogLock.acquireTimeout;
import static org.apache.paimon.hive.HiveCatalogLock.checkMaxSleep;
import static org.apache.paimon.hive.HiveCatalogOptions.LOCATION_IN_PROPERTIES;
import static org.apache.paimon.options.CatalogOptions.LOCK_ENABLED;
import static org.apache.paimon.options.CatalogOptions.TABLE_TYPE;
import static org.apache.paimon.utils.Preconditions.checkState;
import static org.apache.paimon.utils.StringUtils.isNullOrWhitespaceOnly;

/** A catalog implementation for Hive. */
public class HiveCatalog extends AbstractCatalog {
    private static final Logger LOG = LoggerFactory.getLogger(HiveCatalog.class);

    // we don't include paimon-hive-connector as dependencies because it depends on
    // hive-exec
    private static final String INPUT_FORMAT_CLASS_NAME =
            "org.apache.paimon.hive.mapred.PaimonInputFormat";
    private static final String OUTPUT_FORMAT_CLASS_NAME =
            "org.apache.paimon.hive.mapred.PaimonOutputFormat";
    private static final String SERDE_CLASS_NAME = "org.apache.paimon.hive.PaimonSerDe";
    private static final String STORAGE_HANDLER_CLASS_NAME =
            "org.apache.paimon.hive.PaimonStorageHandler";

    public static final String HIVE_SITE_FILE = "hive-site.xml";

    private final HiveConf hiveConf;
    private final String clientClassName;
    private final IMetaStoreClient client;
    private final String warehouse;

    private LocationHelper locationHelper;

    public HiveCatalog(FileIO fileIO, HiveConf hiveConf, String clientClassName, String warehouse) {
        this(fileIO, hiveConf, clientClassName, Collections.emptyMap(), warehouse);
    }

    public HiveCatalog(
            FileIO fileIO,
            HiveConf hiveConf,
            String clientClassName,
            Map<String, String> options,
            String warehouse) {
        super(fileIO, options);
        this.hiveConf = hiveConf;
        this.clientClassName = clientClassName;
        this.warehouse = warehouse;

        boolean needLocationInProperties =
                hiveConf.getBoolean(
                        LOCATION_IN_PROPERTIES.key(), LOCATION_IN_PROPERTIES.defaultValue());
        if (needLocationInProperties) {
            locationHelper = new TBPropertiesLocationHelper();
        } else {
            // set the warehouse location to the hiveConf
            hiveConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, warehouse);
            locationHelper = new StorageLocationHelper();
        }

        this.client = createClient(hiveConf, clientClassName);
    }

    @Override
    public Optional<CatalogLock.Factory> lockFactory() {
        return lockEnabled()
                ? Optional.of(HiveCatalogLock.createFactory(hiveConf, clientClassName))
                : Optional.empty();
    }

    private boolean lockEnabled() {
        return Boolean.parseBoolean(
                hiveConf.get(LOCK_ENABLED.key(), LOCK_ENABLED.defaultValue().toString()));
    }

    @Override
    public List<String> listDatabases() {
        try {
            return client.getAllDatabases();
        } catch (TException e) {
            throw new RuntimeException("Failed to list all databases", e);
        }
    }

    @Override
    public boolean databaseExists(String databaseName) {
        try {
            client.getDatabase(databaseName);
            return true;
        } catch (NoSuchObjectException e) {
            return false;
        } catch (TException e) {
            throw new RuntimeException(
                    "Failed to determine if database " + databaseName + " exists", e);
        }
    }

    @Override
    public void createDatabase(String name, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException {
        try {
            client.createDatabase(convertToDatabase(name));

            locationHelper.createPathIfRequired(databasePath(name), fileIO);
        } catch (AlreadyExistsException e) {
            if (!ignoreIfExists) {
                throw new DatabaseAlreadyExistException(name, e);
            }
        } catch (TException | IOException e) {
            throw new RuntimeException("Failed to create database " + name, e);
        }
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException {
        try {
            if (!cascade && client.getAllTables(name).size() > 0) {
                throw new DatabaseNotEmptyException(name);
            }

            locationHelper.dropPathIfRequired(databasePath(name), fileIO);
            client.dropDatabase(name, true, false, true);
        } catch (NoSuchObjectException | UnknownDBException e) {
            if (!ignoreIfNotExists) {
                throw new DatabaseNotExistException(name, e);
            }
        } catch (TException | IOException e) {
            throw new RuntimeException("Failed to drop database " + name, e);
        }
    }

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException {
        try {
            return client.getAllTables(databaseName).stream()
                    .filter(
                            tableName -> {
                                Identifier identifier = new Identifier(databaseName, tableName);
                                // the environment here may not be able to access non-paimon
                                // tables.
                                // so we just check the schema file first
                                return schemaFileExists(identifier)
                                        && paimonTableExists(identifier, false);
                            })
                    .collect(Collectors.toList());
        } catch (UnknownDBException e) {
            throw new DatabaseNotExistException(databaseName, e);
        } catch (TException e) {
            throw new RuntimeException("Failed to list all tables in database " + databaseName, e);
        }
    }

    @Override
    public TableSchema getDataTableSchema(Identifier identifier) throws TableNotExistException {
        if (!paimonTableExists(identifier)) {
            throw new TableNotExistException(identifier);
        }
        Path tableLocation = getDataTableLocation(identifier);
        return new SchemaManager(fileIO, tableLocation)
                .latest()
                .orElseThrow(() -> new RuntimeException("There is no paimond in " + tableLocation));
    }

    @Override
    public void dropTable(Identifier identifier, boolean ignoreIfNotExists)
            throws TableNotExistException {
        checkNotSystemTable(identifier, "dropTable");
        if (!paimonTableExists(identifier)) {
            if (ignoreIfNotExists) {
                return;
            } else {
                throw new TableNotExistException(identifier);
            }
        }

        try {
            client.dropTable(
                    identifier.getDatabaseName(), identifier.getObjectName(), true, false, true);
            // Deletes table directory to avoid schema in filesystem exists after dropping hive
            // table successfully to keep the table consistency between which in filesystem and
            // which in Hive metastore.
            Path path = getDataTableLocation(identifier);
            try {
                if (fileIO.exists(path)) {
                    fileIO.deleteDirectoryQuietly(path);
                }
            } catch (Exception ee) {
                LOG.error("Delete directory[{}] fail for table {}", path, identifier, ee);
            }
        } catch (TException e) {
            throw new RuntimeException("Failed to drop table " + identifier.getFullName(), e);
        }
    }

    @Override
    public void createTable(Identifier identifier, Schema schema, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException {
        checkNotSystemTable(identifier, "createTable");
        String databaseName = identifier.getDatabaseName();
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(databaseName);
        }
        if (paimonTableExists(identifier)) {
            if (ignoreIfExists) {
                return;
            } else {
                throw new TableAlreadyExistException(identifier);
            }
        }

        checkFieldNamesUpperCase(schema.rowType().getFieldNames());
        // first commit changes to underlying files
        // if changes on Hive fails there is no harm to perform the same changes to files again

        copyTableDefaultOptions(schema.options());

        TableSchema tableSchema;
        try {
            tableSchema = schemaManager(identifier).createTable(schema);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to commit changes of table "
                            + identifier.getFullName()
                            + " to underlying files",
                    e);
        }
        Table table = newHmsTable(identifier);
        updateHmsTable(table, identifier, tableSchema);
        try {
            client.createTable(table);
        } catch (TException e) {
            Path path = getDataTableLocation(identifier);
            try {
                fileIO.deleteDirectoryQuietly(path);
            } catch (Exception ee) {
                LOG.error("Delete directory[{}] fail for table {}", path, identifier, ee);
            }
            throw new RuntimeException("Failed to create table " + identifier.getFullName(), e);
        }
    }

    @Override
    public void renameTable(Identifier fromTable, Identifier toTable, boolean ignoreIfNotExists)
            throws TableNotExistException, TableAlreadyExistException {
        checkNotSystemTable(fromTable, "renameTable");
        checkNotSystemTable(toTable, "renameTable");
        if (!paimonTableExists(fromTable)) {
            if (ignoreIfNotExists) {
                return;
            } else {
                throw new TableNotExistException(fromTable);
            }
        }

        if (paimonTableExists(toTable)) {
            throw new TableAlreadyExistException(toTable);
        }

        try {
            checkIdentifierUpperCase(toTable);
            String fromDB = fromTable.getDatabaseName();
            String fromTableName = fromTable.getObjectName();
            Table table = client.getTable(fromDB, fromTableName);
            table.setDbName(toTable.getDatabaseName());
            table.setTableName(toTable.getObjectName());
            client.alter_table(fromDB, fromTableName, table);
        } catch (TException e) {
            throw new RuntimeException("Failed to rename table " + fromTable.getFullName(), e);
        }
    }

    @Override
    public void alterTable(
            Identifier identifier, List<SchemaChange> changes, boolean ignoreIfNotExists)
            throws TableNotExistException {
        checkNotSystemTable(identifier, "alterTable");
        if (!paimonTableExists(identifier)) {
            if (ignoreIfNotExists) {
                return;
            } else {
                throw new TableNotExistException(identifier);
            }
        }

        checkFieldNamesUpperCaseInSchemaChange(changes);
        try {
            final SchemaManager schemaManager = schemaManager(identifier);
            // first commit changes to underlying files
            TableSchema schema = schemaManager.commitChanges(changes);

            try {
                // sync to hive hms
                Table table =
                        client.getTable(identifier.getDatabaseName(), identifier.getObjectName());
                updateHmsTable(table, identifier, schema);
                client.alter_table(identifier.getDatabaseName(), identifier.getObjectName(), table);
            } catch (TException te) {
                schemaManager.deleteSchema(schema.id());
                throw te;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean caseSensitive() {
        return false;
    }

    @Override
    public void close() throws Exception {
        client.close();
    }

    @Override
    protected String warehouse() {
        return warehouse;
    }

    private void checkIdentifierUpperCase(Identifier identifier) {
        checkState(
                identifier.getDatabaseName().equals(identifier.getDatabaseName().toLowerCase()),
                String.format(
                        "Database name[%s] cannot contain upper case in hive catalog",
                        identifier.getDatabaseName()));
        checkState(
                identifier.getObjectName().equals(identifier.getObjectName().toLowerCase()),
                String.format(
                        "Table name[%s] cannot contain upper case in hive catalog",
                        identifier.getObjectName()));
    }

    private void checkFieldNamesUpperCaseInSchemaChange(List<SchemaChange> changes) {
        List<String> fieldNames = new ArrayList<>();
        for (SchemaChange change : changes) {
            if (change instanceof SchemaChange.AddColumn) {
                SchemaChange.AddColumn addColumn = (SchemaChange.AddColumn) change;
                fieldNames.add(addColumn.fieldName());
            } else if (change instanceof SchemaChange.RenameColumn) {
                SchemaChange.RenameColumn rename = (SchemaChange.RenameColumn) change;
                fieldNames.add(rename.newName());
            } else {
                // do nothing
            }
        }
        checkFieldNamesUpperCase(fieldNames);
    }

    private void checkFieldNamesUpperCase(List<String> fieldNames) {
        List<String> illegalFieldNames =
                fieldNames.stream()
                        .filter(f -> !f.equals(f.toLowerCase()))
                        .collect(Collectors.toList());
        checkState(
                illegalFieldNames.isEmpty(),
                String.format(
                        "Field names %s cannot contain upper case in hive catalog",
                        illegalFieldNames));
    }

    private Database convertToDatabase(String name) {
        Database database = new Database();
        database.setName(name);
        locationHelper.specifyDatabaseLocation(databasePath(name), database);
        return database;
    }

    private Table newHmsTable(Identifier identifier) {
        long currentTimeMillis = System.currentTimeMillis();
        TableType tableType =
                OptionsUtils.convertToEnum(
                        hiveConf.get(TABLE_TYPE.key(), TableType.MANAGED.toString()),
                        TableType.class);
        Table table =
                new Table(
                        identifier.getObjectName(),
                        identifier.getDatabaseName(),
                        // current linux user
                        System.getProperty("user.name"),
                        (int) (currentTimeMillis / 1000),
                        (int) (currentTimeMillis / 1000),
                        Integer.MAX_VALUE,
                        null,
                        Collections.emptyList(),
                        new HashMap<>(),
                        null,
                        null,
                        tableType.toString().toUpperCase(Locale.ROOT) + "_TABLE");
        table.getParameters()
                .put(hive_metastoreConstants.META_TABLE_STORAGE, STORAGE_HANDLER_CLASS_NAME);
        if (TableType.EXTERNAL.equals(tableType)) {
            table.getParameters().put("EXTERNAL", "TRUE");
        }
        return table;
    }

    private void updateHmsTable(Table table, Identifier identifier, TableSchema schema) {
        StorageDescriptor sd = convertToStorageDescriptor(schema);
        table.setSd(sd);

        // update location
        locationHelper.specifyTableLocation(table, getDataTableLocation(identifier).toString());
    }

    private StorageDescriptor convertToStorageDescriptor(TableSchema schema) {
        StorageDescriptor sd = new StorageDescriptor();

        sd.setCols(
                schema.fields().stream()
                        .map(this::convertToFieldSchema)
                        .collect(Collectors.toList()));

        sd.setInputFormat(INPUT_FORMAT_CLASS_NAME);
        sd.setOutputFormat(OUTPUT_FORMAT_CLASS_NAME);

        SerDeInfo serDeInfo = new SerDeInfo();
        serDeInfo.setParameters(new HashMap<>());
        serDeInfo.setSerializationLib(SERDE_CLASS_NAME);
        sd.setSerdeInfo(serDeInfo);

        return sd;
    }

    @VisibleForTesting
    IMetaStoreClient getHmsClient() {
        return client;
    }

    private FieldSchema convertToFieldSchema(DataField dataField) {
        return new FieldSchema(
                dataField.name(),
                HiveTypeUtils.logicalTypeToTypeInfo(dataField.type()).getTypeName(),
                dataField.description());
    }

    private boolean paimonTableExists(Identifier identifier) {
        return paimonTableExists(identifier, true);
    }

    private boolean schemaFileExists(Identifier identifier) {
        return new SchemaManager(fileIO, getDataTableLocation(identifier)).latest().isPresent();
    }

    private boolean paimonTableExists(Identifier identifier, boolean throwException) {
        Table table;
        try {
            table = client.getTable(identifier.getDatabaseName(), identifier.getObjectName());
        } catch (NoSuchObjectException e) {
            return false;
        } catch (TException e) {
            throw new RuntimeException(
                    "Cannot determine if table " + identifier.getFullName() + " is a paimon table.",
                    e);
        }

        boolean isPaimonTable = isPaimonTable(table) || LegacyHiveClasses.isPaimonTable(table);
        if (!isPaimonTable && throwException) {
            throw new IllegalArgumentException(
                    "Table "
                            + identifier.getFullName()
                            + " is not a paimon table. It's input format is "
                            + table.getSd().getInputFormat()
                            + " and its output format is "
                            + table.getSd().getOutputFormat());
        }
        return isPaimonTable;
    }

    private static boolean isPaimonTable(Table table) {
        return INPUT_FORMAT_CLASS_NAME.equals(table.getSd().getInputFormat())
                && OUTPUT_FORMAT_CLASS_NAME.equals(table.getSd().getOutputFormat());
    }

    private SchemaManager schemaManager(Identifier identifier) {
        checkIdentifierUpperCase(identifier);
        return new SchemaManager(fileIO, getDataTableLocation(identifier))
                .withLock(lock(identifier));
    }

    private Lock lock(Identifier identifier) {
        if (!lockEnabled()) {
            return new Lock.EmptyLock();
        }

        HiveCatalogLock lock =
                new HiveCatalogLock(client, checkMaxSleep(hiveConf), acquireTimeout(hiveConf));
        return Lock.fromCatalog(lock, identifier);
    }

    private static final List<Class<?>[]> GET_PROXY_PARAMS =
            Arrays.asList(
                    // for hive 2.x
                    new Class<?>[] {
                        HiveConf.class,
                        HiveMetaHookLoader.class,
                        ConcurrentHashMap.class,
                        String.class,
                        Boolean.TYPE
                    },
                    // for hive 3.x
                    new Class<?>[] {
                        Configuration.class,
                        HiveMetaHookLoader.class,
                        ConcurrentHashMap.class,
                        String.class,
                        Boolean.TYPE
                    });

    static IMetaStoreClient createClient(HiveConf hiveConf, String clientClassName) {
        Method getProxy = null;
        RuntimeException methodNotFound =
                new RuntimeException(
                        "Failed to find desired getProxy method from RetryingMetaStoreClient");
        for (Class<?>[] classes : GET_PROXY_PARAMS) {
            try {
                getProxy = RetryingMetaStoreClient.class.getMethod("getProxy", classes);
            } catch (NoSuchMethodException e) {
                methodNotFound.addSuppressed(e);
            }
        }
        if (getProxy == null) {
            throw methodNotFound;
        }

        IMetaStoreClient client;
        try {
            client =
                    (IMetaStoreClient)
                            getProxy.invoke(
                                    null,
                                    hiveConf,
                                    (HiveMetaHookLoader) (tbl -> null),
                                    new ConcurrentHashMap<>(),
                                    clientClassName,
                                    true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return isNullOrWhitespaceOnly(hiveConf.get(HiveConf.ConfVars.METASTOREURIS.varname))
                ? client
                : HiveMetaStoreClient.newSynchronizedClient(client);
    }

    public static HiveConf createHiveConf(
            @Nullable String hiveConfDir, @Nullable String hadoopConfDir) {
        // create HiveConf from hadoop configuration with hadoop conf directory configured.
        Configuration hadoopConf = null;
        if (!isNullOrWhitespaceOnly(hadoopConfDir)) {
            hadoopConf = getHadoopConfiguration(hadoopConfDir);
            if (hadoopConf == null) {
                String possiableUsedConfFiles =
                        "core-site.xml | hdfs-site.xml | yarn-site.xml | mapred-site.xml";
                throw new RuntimeException(
                        "Failed to load the hadoop conf from specified path:" + hadoopConfDir,
                        new FileNotFoundException(
                                "Please check the path none of the conf files ("
                                        + possiableUsedConfFiles
                                        + ") exist in the folder."));
            }
        }
        if (hadoopConf == null) {
            hadoopConf = new Configuration();
        }

        LOG.info("Setting hive conf dir as {}", hiveConfDir);
        if (hiveConfDir != null) {
            // ignore all the static conf file URLs that HiveConf may have set
            HiveConf.setHiveSiteLocation(null);
            HiveConf.setLoadMetastoreConfig(false);
            HiveConf.setLoadHiveServer2Config(false);
            HiveConf hiveConf = new HiveConf(hadoopConf, HiveConf.class);

            org.apache.hadoop.fs.Path hiveSite =
                    new org.apache.hadoop.fs.Path(hiveConfDir, HIVE_SITE_FILE);
            if (!hiveSite.toUri().isAbsolute()) {
                hiveSite = new org.apache.hadoop.fs.Path(new File(hiveSite.toString()).toURI());
            }
            try (InputStream inputStream = hiveSite.getFileSystem(hadoopConf).open(hiveSite)) {
                hiveConf.addResource(inputStream, hiveSite.toString());
                // trigger a read from the conf to avoid input stream is closed
                isEmbeddedMetastore(hiveConf);
            } catch (IOException e) {
                throw new RuntimeException(
                        "Failed to load hive-site.xml from specified path:" + hiveSite, e);
            }
            hiveConf.addResource(hiveSite);

            return hiveConf;
        } else {
            return new HiveConf(hadoopConf, HiveConf.class);
        }
    }

    public static boolean isEmbeddedMetastore(HiveConf hiveConf) {
        return isNullOrWhitespaceOnly(hiveConf.getVar(HiveConf.ConfVars.METASTOREURIS));
    }

    /**
     * Returns a new Hadoop Configuration object using the path to the hadoop conf configured.
     *
     * @param hadoopConfDir Hadoop conf directory path.
     * @return A Hadoop configuration instance.
     */
    public static Configuration getHadoopConfiguration(String hadoopConfDir) {
        if (new File(hadoopConfDir).exists()) {
            List<File> possiableConfFiles = new ArrayList<File>();
            File coreSite = new File(hadoopConfDir, "core-site.xml");
            if (coreSite.exists()) {
                possiableConfFiles.add(coreSite);
            }
            File hdfsSite = new File(hadoopConfDir, "hdfs-site.xml");
            if (hdfsSite.exists()) {
                possiableConfFiles.add(hdfsSite);
            }
            File yarnSite = new File(hadoopConfDir, "yarn-site.xml");
            if (yarnSite.exists()) {
                possiableConfFiles.add(yarnSite);
            }
            // Add mapred-site.xml. We need to read configurations like compression codec.
            File mapredSite = new File(hadoopConfDir, "mapred-site.xml");
            if (mapredSite.exists()) {
                possiableConfFiles.add(mapredSite);
            }
            if (possiableConfFiles.isEmpty()) {
                return null;
            } else {
                Configuration hadoopConfiguration = new Configuration();
                for (File confFile : possiableConfFiles) {
                    hadoopConfiguration.addResource(
                            new org.apache.hadoop.fs.Path(confFile.getAbsolutePath()));
                }
                return hadoopConfiguration;
            }
        }
        return null;
    }
}
