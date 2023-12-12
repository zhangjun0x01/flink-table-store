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

package org.apache.paimon.flink.sink.cdc;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.types.DataField;

import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * A {@link ProcessFunction} to parse CDC change event to either a list of {@link DataField}s or
 * {@link CdcRecord} and send them to different downstreams.
 *
 * <p>This {@link ProcessFunction} can only handle records for a single constant table. To handle
 * records for different tables, see {@link CdcMultiTableParsingProcessFunction}.
 *
 * @param <T> CDC change event type
 */
public class CdcParsingProcessFunction<T> extends ProcessFunction<T, CdcRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(CdcParsingProcessFunction.class);

    public static final OutputTag<List<DataField>> NEW_DATA_FIELD_LIST_OUTPUT_TAG =
            new OutputTag<>("new-data-field-list", new ListTypeInfo<>(DataField.class));

    private final EventParser.Factory<T> parserFactory;
    private final Catalog.Loader catalogLoader;
    private final Identifier identifier;

    private transient EventParser<T> parser;

    public CdcParsingProcessFunction(
            EventParser.Factory<T> parserFactory,
            Catalog.Loader catalogLoader,
            Identifier identifier) {
        this.parserFactory = parserFactory;
        this.catalogLoader = catalogLoader;
        this.identifier = identifier;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        parser = parserFactory.create();
    }

    @Override
    public void processElement(T raw, Context context, Collector<CdcRecord> collector)
            throws Exception {
        parser.setRawEvent(raw);
        List<DataField> schemaChange = parser.parseSchemaChange();
        if (!schemaChange.isEmpty()) {
            context.output(NEW_DATA_FIELD_LIST_OUTPUT_TAG, schemaChange);
        }

        parser.parseTableComment()
                .ifPresent(
                        c -> {
                            try {
                                SchemaChange updateTableComment =
                                        SchemaChange.updateTableComment(c);
                                catalogLoader
                                        .load()
                                        .alterTable(identifier, updateTableComment, true);
                            } catch (Exception e) {
                                LOG.error(
                                        "Cannot change table ({}) comment.",
                                        identifier.getFullName(),
                                        e);
                            }
                        });

        parser.parseRecords().forEach(collector::collect);
    }
}
