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

import org.apache.paimon.annotation.Experimental;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** A change message contains schema and data. */
@Experimental
public class RichCdcRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    private final CdcRecord cdcRecord;
    private final LinkedHashMap<String, DataType> fieldTypes;
    private final LinkedHashMap<String, String> fieldComments;

    public RichCdcRecord(
            CdcRecord cdcRecord,
            LinkedHashMap<String, DataType> fieldTypes,
            LinkedHashMap<String, String> fieldComments) {
        this.cdcRecord = cdcRecord;
        this.fieldTypes = fieldTypes;
        this.fieldComments = fieldComments;
    }

    public boolean hasPayload() {
        return !cdcRecord.fields().isEmpty();
    }

    public RowKind kind() {
        return cdcRecord.kind();
    }

    public LinkedHashMap<String, DataType> fieldTypes() {
        return fieldTypes;
    }

    public LinkedHashMap<String, String> fieldComments() {
        return fieldComments;
    }

    public CdcRecord toCdcRecord() {
        return cdcRecord;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RichCdcRecord that = (RichCdcRecord) o;
        return cdcRecord == that.cdcRecord && Objects.equals(fieldTypes, that.fieldTypes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cdcRecord, fieldTypes);
    }

    @Override
    public String toString() {
        return "{" + "cdcRecord=" + cdcRecord + ", fieldTypes=" + fieldTypes + '}';
    }

    public static Builder builder(RowKind kind) {
        return new Builder(kind);
    }

    /** Builder for {@link RichCdcRecord}. */
    public static class Builder {

        private final RowKind kind;
        private final LinkedHashMap<String, DataType> fieldTypes = new LinkedHashMap<>();
        private final LinkedHashMap<String, String> fieldComments = new LinkedHashMap<>();
        private final Map<String, String> fieldValues = new HashMap<>();

        public Builder(RowKind kind) {
            this.kind = kind;
        }

        public Builder field(String name, DataType type, String value, String comment) {
            fieldTypes.put(name, type);
            fieldValues.put(name, value);
            fieldComments.put(name, comment);
            return this;
        }

        public RichCdcRecord build() {
            return new RichCdcRecord(new CdcRecord(kind, fieldValues), fieldTypes, fieldComments);
        }
    }
}
