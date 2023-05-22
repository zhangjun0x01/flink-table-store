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

package org.apache.paimon.format.parquet.filter;

import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.FunctionVisitor;
import org.apache.paimon.predicate.GreaterOrEqual;
import org.apache.paimon.predicate.GreaterThan;
import org.apache.paimon.predicate.IsNotNull;
import org.apache.paimon.predicate.IsNull;
import org.apache.paimon.predicate.LeafFunction;
import org.apache.paimon.predicate.LessOrEqual;
import org.apache.paimon.predicate.LessThan;
import org.apache.paimon.predicate.NotEqual;
import org.apache.paimon.types.DataTypeRoot;

import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.io.api.Binary;

import java.util.List;
import java.util.Optional;

/** aaaa. */
public class ParquetPredicateFunctionVisitor implements FunctionVisitor<Optional<FilterPredicate>> {
    private final LeafFunction function;

    public ParquetPredicateFunctionVisitor() {
        this(null);
    }

    public ParquetPredicateFunctionVisitor(LeafFunction function) {
        this.function = function;
    }

    @Override
    public Optional<FilterPredicate> visitIsNotNull(FieldRef fieldRef) {
        return Optional.of(predicate(fieldRef, null));
    }

    @Override
    public Optional<FilterPredicate> visitIsNull(FieldRef fieldRef) {
        return Optional.of(predicate(fieldRef, null));
    }

    @Override
    public Optional<FilterPredicate> visitStartsWith(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<FilterPredicate> visitLessThan(FieldRef fieldRef, Object literal) {
        return Optional.of(predicate(fieldRef, literal));
    }

    @Override
    public Optional<FilterPredicate> visitGreaterOrEqual(FieldRef fieldRef, Object literal) {
        return Optional.of(predicate(fieldRef, literal));
    }

    @Override
    public Optional<FilterPredicate> visitNotEqual(FieldRef fieldRef, Object literal) {
        return Optional.of(predicate(fieldRef, literal));
    }

    @Override
    public Optional<FilterPredicate> visitLessOrEqual(FieldRef fieldRef, Object literal) {
        return Optional.of(predicate(fieldRef, literal));
    }

    @Override
    public Optional<FilterPredicate> visitEqual(FieldRef fieldRef, Object literal) {
        return Optional.of(predicate(fieldRef, literal));
    }

    @Override
    public Optional<FilterPredicate> visitGreaterThan(FieldRef fieldRef, Object literal) {
        return Optional.of(predicate(fieldRef, literal));
    }

    @Override
    public Optional<FilterPredicate> visitIn(FieldRef fieldRef, List<Object> literals) {
        return Optional.empty();
    }

    @Override
    public Optional<FilterPredicate> visitNotIn(FieldRef fieldRef, List<Object> literals) {
        return Optional.empty();
    }

    @Override
    public Optional<FilterPredicate> visitAnd(List<Optional<FilterPredicate>> children) {
        return Optional.empty();
    }

    @Override
    public Optional<FilterPredicate> visitOr(List<Optional<FilterPredicate>> children) {
        return Optional.empty();
    }

    private FilterPredicate predicate(FieldRef fieldRef, Object literal) {
        DataTypeRoot type = fieldRef.type().getTypeRoot();
        String name = fieldRef.name();
        switch (type) {
            case BOOLEAN:
                Operators.BooleanColumn col = FilterApi.booleanColumn(name);
                if (function instanceof Equal) {
                    return FilterApi.eq(col, toParquetObject(type, literal));
                } else if (function instanceof NotEqual) {
                    return FilterApi.eq(col, toParquetObject(type, literal));
                }
                break;
            case INTEGER:
            case DATE:
                Operators.IntColumn intColumn = FilterApi.intColumn(name);
                return pred(intColumn, toParquetObject(type, literal));
            case FLOAT:
                return pred(FilterApi.floatColumn(name), toParquetObject(type, literal));
            case DOUBLE:
                return pred(FilterApi.doubleColumn(name), toParquetObject(type, literal));
            case BIGINT:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                Operators.LongColumn longColumn = FilterApi.longColumn(name);
                return pred(longColumn, toParquetObject(type, literal));
            case CHAR:
            case VARCHAR:
            case BINARY:
                Operators.BinaryColumn binaryColumn = FilterApi.binaryColumn(name);
                return pred(binaryColumn, toParquetObject(type, literal));
        }

        throw new UnsupportedOperationException(
                "Cannot convert to Parquet filter: " + fieldRef.name());
    }

    private <C extends Comparable<C>, COL extends Operators.Column<C> & Operators.SupportsLtGt>
            FilterPredicate pred(COL col, C value) {
        if (function instanceof Equal) {
            return FilterApi.eq(col, value);
        } else if (function instanceof NotEqual) {
            return FilterApi.notEq(col, value);
        } else if (function instanceof GreaterThan) {
            return FilterApi.gt(col, value);
        } else if (function instanceof GreaterOrEqual) {
            return FilterApi.gtEq(col, value);
        } else if (function instanceof LessThan) {
            return FilterApi.lt(col, value);
        } else if (function instanceof LessOrEqual) {
            return FilterApi.ltEq(col, value);
        } else if (function instanceof IsNull) {
            return FilterApi.eq(col, value);
        } else if (function instanceof IsNotNull) {
            return FilterApi.notEq(col, value);
        } else {
            throw new UnsupportedOperationException("Unsupported predicate operation: " + function);
        }
    }

    private <C extends Comparable<C>> C toParquetObject(DataTypeRoot type, Object literal) {
        if (null == literal) {
            return null;
        }
        switch (type) {
            case CHAR:
            case VARCHAR:
                return (C) Binary.fromString(literal.toString());
        }

        return (C) literal;
    }
}
