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

import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A {@link ProcessFunction} to handle schema changes. New schema is represented by a list of {@link
 * DataField}s.
 *
 * <p>NOTE: To avoid concurrent schema changes, the parallelism of this {@link ProcessFunction} must
 * be 1.
 */
public class UpdatedDataFieldsProcessFunction extends ProcessFunction<List<DataField>, Void> {

    private static final Logger LOG =
            LoggerFactory.getLogger(UpdatedDataFieldsProcessFunction.class);

    private final SchemaManager schemaManager;

    public UpdatedDataFieldsProcessFunction(SchemaManager schemaManager) {
        this.schemaManager = schemaManager;
    }

    @Override
    public void processElement(
            List<DataField> updatedDataFields, Context context, Collector<Void> collector)
            throws Exception {
        for (SchemaChange schemaChange : extractSchemaChanges(updatedDataFields)) {
            applySchemaChange(schemaChange);
        }
    }

    private List<SchemaChange> extractSchemaChanges(List<DataField> updatedDataFields) {
        RowType oldRowType = schemaManager.latest().get().logicalRowType();
        Map<String, DataField> oldFields = new HashMap<>();
        Map<String, DataField> newFields = new HashMap<>();
        List<SchemaChange> result = new ArrayList<>();

        for (DataField newField : updatedDataFields) {
            newFields.put(newField.name(), newField);
        }

        for (DataField oldField : oldRowType.getFields()) {
            oldFields.put(oldField.name(), oldField);
            //drop column
            if (!newFields.containsKey(oldField.name())) {
                result.add(SchemaChange.dropColumn(oldField.name()));
            }
        }


        for (DataField newField : updatedDataFields) {
            if (oldFields.containsKey(newField.name())) {
                DataField oldField = oldFields.get(newField.name());
                // we compare by ignoring nullable, because partition keys and primary keys might be
                // nullable in source database, but they can't be null in Paimon
                if (oldField.type().equalsIgnoreNullable(newField.type())) {
                    if (!Objects.equals(oldField.description(), newField.description())) {
                        result.add(
                                SchemaChange.updateColumnComment(
                                        new String[]{newField.name()}, newField.description()));
                    }
                } else {
                    result.add(SchemaChange.updateColumnType(newField.name(), newField.type()));
                    if (newField.description() != null) {
                        result.add(
                                SchemaChange.updateColumnComment(
                                        new String[]{newField.name()}, newField.description()));
                    }
                }
            } else {
                result.add(
                        SchemaChange.addColumn(
                                newField.name(), newField.type(), newField.description(), null));
            }
        }
        return result;
    }

    private void applySchemaChange(SchemaChange schemaChange) throws Exception {
        if (schemaChange instanceof SchemaChange.AddColumn) {
            try {
                schemaManager.commitChanges(schemaChange);
            } catch (IllegalArgumentException e) {
                // This is normal. For example when a table is split into multiple database tables,
                // all these tables will be added the same column. However schemaManager can't
                // handle duplicated column adds, so we just catch the exception and log it.
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "Failed to perform SchemaChange.AddColumn {}, "
                                    + "possibly due to duplicated column name",
                            schemaChange,
                            e);
                }
            }
        } else if (schemaChange instanceof SchemaChange.UpdateColumnType) {
            SchemaChange.UpdateColumnType updateColumnType =
                    (SchemaChange.UpdateColumnType) schemaChange;
            TableSchema schema =
                    schemaManager
                            .latest()
                            .orElseThrow(
                                    () ->
                                            new RuntimeException(
                                                    "Table does not exist. This is unexpected."));
            int idx = schema.fieldNames().indexOf(updateColumnType.fieldName());
            Preconditions.checkState(
                    idx >= 0,
                    "Field name "
                            + updateColumnType.fieldName()
                            + " does not exist in table. This is unexpected.");
            DataType oldType = schema.fields().get(idx).type();
            DataType newType = updateColumnType.newDataType();
            switch (canConvert(oldType, newType)) {
                case CONVERT:
                    schemaManager.commitChanges(schemaChange);
                    break;
                case EXCEPTION:
                    throw new UnsupportedOperationException(
                            String.format(
                                    "Cannot convert field %s from type %s to %s",
                                    updateColumnType.fieldName(), oldType, newType));
            }
        } else if (schemaChange instanceof SchemaChange.UpdateColumnComment || schemaChange instanceof SchemaChange.DropColumn) {
            schemaManager.commitChanges(schemaChange);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported schema change class "
                            + schemaChange.getClass().getName()
                            + ", content "
                            + schemaChange);
        }
    }

    private static final List<DataTypeRoot> STRING_TYPES =
            Arrays.asList(DataTypeRoot.CHAR, DataTypeRoot.VARCHAR);
    private static final List<DataTypeRoot> BINARY_TYPES =
            Arrays.asList(DataTypeRoot.BINARY, DataTypeRoot.VARBINARY);
    private static final List<DataTypeRoot> INTEGER_TYPES =
            Arrays.asList(
                    DataTypeRoot.TINYINT,
                    DataTypeRoot.SMALLINT,
                    DataTypeRoot.INTEGER,
                    DataTypeRoot.BIGINT);
    private static final List<DataTypeRoot> FLOATING_POINT_TYPES =
            Arrays.asList(DataTypeRoot.FLOAT, DataTypeRoot.DOUBLE);

    public static ConvertAction canConvert(DataType oldType, DataType newType) {
        if (oldType.equalsIgnoreNullable(newType)) {
            return ConvertAction.CONVERT;
        }

        int oldIdx = STRING_TYPES.indexOf(oldType.getTypeRoot());
        int newIdx = STRING_TYPES.indexOf(newType.getTypeRoot());
        if (oldIdx >= 0 && newIdx >= 0) {
            return DataTypeChecks.getLength(oldType) <= DataTypeChecks.getLength(newType)
                    ? ConvertAction.CONVERT
                    : ConvertAction.IGNORE;
        }

        oldIdx = BINARY_TYPES.indexOf(oldType.getTypeRoot());
        newIdx = BINARY_TYPES.indexOf(newType.getTypeRoot());
        if (oldIdx >= 0 && newIdx >= 0) {
            return DataTypeChecks.getLength(oldType) <= DataTypeChecks.getLength(newType)
                    ? ConvertAction.CONVERT
                    : ConvertAction.IGNORE;
        }

        oldIdx = INTEGER_TYPES.indexOf(oldType.getTypeRoot());
        newIdx = INTEGER_TYPES.indexOf(newType.getTypeRoot());
        if (oldIdx >= 0 && newIdx >= 0) {
            return oldIdx <= newIdx ? ConvertAction.CONVERT : ConvertAction.IGNORE;
        }

        oldIdx = FLOATING_POINT_TYPES.indexOf(oldType.getTypeRoot());
        newIdx = FLOATING_POINT_TYPES.indexOf(newType.getTypeRoot());
        if (oldIdx >= 0 && newIdx >= 0) {
            return oldIdx <= newIdx ? ConvertAction.CONVERT : ConvertAction.IGNORE;
        }

        return ConvertAction.EXCEPTION;
    }

    /**
     * Return type of {@link UpdatedDataFieldsProcessFunction#canConvert(DataType, DataType)}. This
     * enum indicates the action to perform.
     */
    public enum ConvertAction {

        /**
         * {@code oldType} can be converted to {@code newType}.
         */
        CONVERT,

        /**
         * {@code oldType} and {@code newType} belongs to the same type family, but old type has
         * higher precision than new type. Ignore this convert request.
         */
        IGNORE,

        /**
         * {@code oldType} and {@code newType} belongs to different type family. Throw an exception
         * indicating that this convert request cannot be handled.
         */
        EXCEPTION
    }
}
