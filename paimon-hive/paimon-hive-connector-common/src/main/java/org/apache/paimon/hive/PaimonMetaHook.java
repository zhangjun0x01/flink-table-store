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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.AbstractCatalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.hive.mapred.PaimonInputFormat;
import org.apache.paimon.hive.mapred.PaimonOutputFormat;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.apache.paimon.hive.HiveTypeUtils.typeInfoToLogicalType;

/**
 * {@link HiveMetaHook} for paimon. Currently this class is only used to set input and output
 * formats.
 */
public class PaimonMetaHook implements HiveMetaHook {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonMetaHook.class);

    private static final String COMMENT = "comment";
    private final Configuration conf;

    public PaimonMetaHook(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public void preCreateTable(Table table) throws MetaException {
        if (table.getPartitionKeysSize() != 0) {
            throw new MetaException(
                    "Paimon currently does not support creating partitioned table "
                            + "with PARTITIONED BY clause. If you want to create a partitioned table, "
                            + "please set partition fields in properties.");
        }

        // hive ql parse cannot recognize input near '$' in table name, no need to add paimon system
        // table verification.

        table.getSd().setInputFormat(PaimonInputFormat.class.getCanonicalName());
        table.getSd().setOutputFormat(PaimonOutputFormat.class.getCanonicalName());

        String location = LocationKeyExtractor.getLocation(table);
        if (location == null) {
            String warehouse = conf.get(HiveConf.ConfVars.METASTOREWAREHOUSE.varname);
            Identifier identifier = Identifier.create(table.getDbName(), table.getTableName());
            location = AbstractCatalog.dataTableLocation(warehouse, identifier).toUri().toString();
            table.getSd().setLocation(location);
        }

        Path path = new Path(location);
        CatalogContext context = catalogContext(table, location);
        FileIO fileIO;
        try {
            fileIO = FileIO.get(path, context);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        SchemaManager schemaManager = new SchemaManager(fileIO, path);
        Optional<TableSchema> tableSchema = schemaManager.latest();
        if (tableSchema.isPresent()) {
            // paimon table already exists
            return;
        }
        // create paimon table
        List<FieldSchema> cols = table.getSd().getCols();
        Schema.Builder schemaBuilder =
                Schema.newBuilder()
                        .options(context.options().toMap())
                        .comment(table.getParameters().get(COMMENT));
        cols.iterator()
                .forEachRemaining(
                        fieldSchema ->
                                schemaBuilder.column(
                                        fieldSchema.getName().toLowerCase(),
                                        typeInfoToLogicalType(fieldSchema.getType()),
                                        fieldSchema.getComment()));
        try {
            schemaManager.createTable(schemaBuilder.build());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void rollbackCreateTable(Table table) throws MetaException {
        if (!MetaStoreUtils.isExternalTable(table)) {
            return;
        }

        // we have created a paimon table, so we delete it to roll back;
        String location = LocationKeyExtractor.getLocation(table);

        Path path = new Path(location);
        CatalogContext context = catalogContext(table, location);
        try {
            FileIO fileIO = FileIO.get(path, context);
            if (fileIO.exists(path)) {
                fileIO.deleteDirectoryQuietly(path);
            }
        } catch (IOException e) {
            LOG.error("Delete directory [{}] fail for the paimon table.", path, e);
        }
    }

    @Override
    public void commitCreateTable(Table table) throws MetaException {}

    @Override
    public void preDropTable(Table table) throws MetaException {}

    @Override
    public void rollbackDropTable(Table table) throws MetaException {}

    @Override
    public void commitDropTable(Table table, boolean b) throws MetaException {}

    private CatalogContext catalogContext(Table table, String location) {
        Options options = PaimonJobConf.extractCatalogConfig(conf);
        options.set(CoreOptions.PATH, location);
        table.getParameters().forEach(options::set);
        return CatalogContext.create(options, conf);
    }
}
