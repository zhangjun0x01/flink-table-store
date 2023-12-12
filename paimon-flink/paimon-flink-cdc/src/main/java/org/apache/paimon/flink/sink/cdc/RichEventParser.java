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

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** A {@link EventParser} for {@link RichCdcRecord}. */
public class RichEventParser implements EventParser<RichCdcRecord> {

    private RichCdcRecord record;

    private final LinkedHashMap<String, DataType> previousDataFields = new LinkedHashMap<>();
    private final LinkedHashMap<String, String> previousComments = new LinkedHashMap<>();

    @Override
    public void setRawEvent(RichCdcRecord rawEvent) {
        this.record = rawEvent;
    }

    @Override
    public List<DataField> parseSchemaChange() {
        List<DataField> change = new ArrayList<>();
        LinkedHashMap<String, String> comments = record.fieldComments();
        record.fieldTypes()
                .forEach(
                        (field, type) -> {
                            String comment = comments.get(field);
                            String preComment = previousComments.get(field);

                            DataType previous = previousDataFields.get(field);
                            if (!Objects.equals(previous, type)
                                    || !Objects.equals(comment, preComment)) {
                                previousDataFields.put(field, type);
                                previousComments.put(field, comment);
                                change.add(new DataField(0, field, type, comment));
                            }
                        });
        return change;
    }

    @Override
    public Optional<String> parseTableComment() {
        if (record.tableComment() == null) {
            return Optional.empty();
        } else {
            return Optional.of(record.tableComment());
        }
    }

    @Override
    public List<CdcRecord> parseRecords() {
        if (record.hasPayload()) {
            return Collections.singletonList(record.toCdcRecord());
        } else {
            return Collections.emptyList();
        }
    }
}
