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

package org.apache.paimon.catalog;

import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.TableSchema;

import java.util.List;
import java.util.Map;

/** Class that wraps an paimon catalog to cache tables. */
public class CacheCatalogImpl extends CacheCatalog {
    protected CacheCatalogImpl(Catalog catalog, long expirationIntervalMillis) {
        super(catalog, expirationIntervalMillis);
    }

    @Override
    protected boolean databaseExistsImpl(String databaseName) {
        return catalog.databaseExistsImpl(databaseName);
    }

    @Override
    protected Map<String, String> loadDatabasePropertiesImpl(String name) {
        return catalog.loadDatabasePropertiesImpl(name);
    }

    @Override
    protected void createDatabaseImpl(String name, Map<String, String> properties) {
        catalog.createDatabaseImpl(name, properties);
    }

    @Override
    protected void dropDatabaseImpl(String name) {
        catalog.dropDatabaseImpl(name);
    }

    @Override
    protected List<String> listTablesImpl(String databaseName) {
        return catalog.listTablesImpl(databaseName);
    }

    @Override
    protected void dropTableImpl(Identifier identifier) {
        catalog.dropTableImpl(identifier);
    }

    @Override
    protected void createTableImpl(Identifier identifier, Schema schema) {
        catalog.createTableImpl(identifier, schema);
    }

    @Override
    protected void renameTableImpl(Identifier fromTable, Identifier toTable) {
        catalog.renameTableImpl(fromTable, toTable);
    }

    @Override
    protected void alterTableImpl(Identifier identifier, List<SchemaChange> changes)
            throws TableNotExistException, ColumnAlreadyExistException, ColumnNotExistException {
        catalog.alterTableImpl(identifier, changes);
    }

    @Override
    public String warehouse() {
        return catalog.warehouse();
    }

    @Override
    protected TableSchema getDataTableSchema(Identifier identifier) throws TableNotExistException {
        return catalog.getDataTableSchema(identifier);
    }
}
