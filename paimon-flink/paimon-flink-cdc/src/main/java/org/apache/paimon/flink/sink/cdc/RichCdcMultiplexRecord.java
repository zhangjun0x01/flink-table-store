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

import org.apache.paimon.types.DataType;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;

/** Compared to {@link CdcMultiplexRecord}, this contains schema information. */
public class RichCdcMultiplexRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    @Nullable private final String databaseName;
    @Nullable
    private final String tableName;
    @Nullable
    private final String tableComment;
    private final LinkedHashMap<String, DataType> fieldTypes;
    private final LinkedHashMap<String, String> fieldComments;
    private final List<String> primaryKeys;
    private final CdcRecord cdcRecord;

    public RichCdcMultiplexRecord(
            @Nullable String databaseName,
            @Nullable String tableName,
            LinkedHashMap<String, DataType> fieldTypes,
            LinkedHashMap<String, String> fieldComments,
            String tableComment,
            List<String> primaryKeys,
            CdcRecord cdcRecord) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.fieldTypes = fieldTypes;
        this.fieldComments = fieldComments;
        this.tableComment = tableComment;
        this.primaryKeys = primaryKeys;
        this.cdcRecord = cdcRecord;
    }

    @Nullable
    public String databaseName() {
        return databaseName;
    }

    @Nullable
    public String tableName() {
        return tableName;
    }

    public String tableComment() {
        return tableComment;
    }

    public LinkedHashMap<String, DataType> fieldTypes() {
        return fieldTypes;
    }

    public LinkedHashMap<String, String> fieldComments() {
        return fieldComments;
    }

    public List<String> primaryKeys() {
        return primaryKeys;
    }

    public RichCdcRecord toRichCdcRecord() {
        return new RichCdcRecord(cdcRecord, fieldTypes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseName, tableName, fieldTypes, primaryKeys, cdcRecord);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RichCdcMultiplexRecord that = (RichCdcMultiplexRecord) o;
        return Objects.equals(databaseName, that.databaseName)
                && Objects.equals(tableName, that.tableName)
                && Objects.equals(fieldTypes, that.fieldTypes)
                && Objects.equals(primaryKeys, that.primaryKeys)
                && Objects.equals(cdcRecord, that.cdcRecord);
    }

    @Override
    public String toString() {
        return "{"
                + "databaseName="
                + databaseName
                + ", tableName="
                + tableName
                + ", fieldTypes="
                + fieldTypes
                + ", primaryKeys="
                + primaryKeys
                + ", cdcRecord="
                + cdcRecord
                + '}';
    }


}
