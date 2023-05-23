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

import org.apache.paimon.data.Decimal;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.FunctionVisitor;
import org.apache.paimon.types.DataType;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;

/**
 * Convert {@link org.apache.paimon.predicate.Predicate} to {@link ParquetFilters.Predicate} for orc.
 */
public class ParquetPredicateFunctionVisitor1
        implements FunctionVisitor<Optional<ParquetFilters.Predicate>> {
    public static final ParquetPredicateFunctionVisitor1 VISITOR = new ParquetPredicateFunctionVisitor1();

    private ParquetPredicateFunctionVisitor1() {
    }

    @Override
    public Optional<ParquetFilters.Predicate> visitIsNull(FieldRef fieldRef) {
        return Optional.of(new ParquetFilters.IsNull(fieldRef.name(), fieldRef.type()));
    }

    @Override
    public Optional<ParquetFilters.Predicate> visitIsNotNull(FieldRef fieldRef) {
        Optional<ParquetFilters.Predicate> isNull = visitIsNull(fieldRef);
        return isNull.map(ParquetFilters.Not::new);
    }

    @Override
    public Optional<ParquetFilters.Predicate> visitStartsWith(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<ParquetFilters.Predicate> visitLessThan(FieldRef fieldRef, Object literal) {
        return convertBinary(fieldRef, literal, ParquetFilters.LessThan::new);
    }

    @Override
    public Optional<ParquetFilters.Predicate> visitGreaterOrEqual(FieldRef fieldRef, Object literal) {
        return convertBinary(
                fieldRef,
                literal,
                (colName, litType, serializableLiteral) ->
                        new ParquetFilters.Not(
                                new ParquetFilters.LessThan(colName, litType, serializableLiteral)));
    }

    @Override
    public Optional<ParquetFilters.Predicate> visitNotEqual(FieldRef fieldRef, Object literal) {
        return convertBinary(
                fieldRef,
                literal,
                (colName, litType, serializableLiteral) ->
                        new ParquetFilters.Not(
                                new ParquetFilters.Equals(colName, litType, serializableLiteral)));
    }

    @Override
    public Optional<ParquetFilters.Predicate> visitLessOrEqual(FieldRef fieldRef, Object literal) {
        return convertBinary(fieldRef, literal, ParquetFilters.LessThanEquals::new);
    }

    @Override
    public Optional<ParquetFilters.Predicate> visitEqual(FieldRef fieldRef, Object literal) {
        return convertBinary(fieldRef, literal, ParquetFilters.Equals::new);
    }

    @Override
    public Optional<ParquetFilters.Predicate> visitGreaterThan(FieldRef fieldRef, Object literal) {
        return convertBinary(
                fieldRef,
                literal,
                (colName, litType, serializableLiteral) ->
                        new ParquetFilters.Not(
                                new ParquetFilters.LessThanEquals(
                                        colName, litType, serializableLiteral)));
    }

    @Override
    public Optional<ParquetFilters.Predicate> visitIn(FieldRef fieldRef, List<Object> literals) {
        return Optional.empty();
    }

    @Override
    public Optional<ParquetFilters.Predicate> visitNotIn(FieldRef fieldRef, List<Object> literals) {
        return Optional.empty();
    }

    @Override
    public Optional<ParquetFilters.Predicate> visitAnd(List<Optional<ParquetFilters.Predicate>> children) {
        if (children.size() != 2) {
            throw new RuntimeException("Illegal and children: " + children.size());
        }

        Optional<ParquetFilters.Predicate> c1 = children.get(0);
        if (!c1.isPresent()) {
            return Optional.empty();
        }
        Optional<ParquetFilters.Predicate> c2 = children.get(1);
        return c2.map(value -> new ParquetFilters.And(c1.get(), value));
    }

    @Override
    public Optional<ParquetFilters.Predicate> visitOr(List<Optional<ParquetFilters.Predicate>> children) {
        if (children.size() != 2) {
            throw new RuntimeException("Illegal or children: " + children.size());
        }

        Optional<ParquetFilters.Predicate> c1 = children.get(0);
        if (!c1.isPresent()) {
            return Optional.empty();
        }
        Optional<ParquetFilters.Predicate> c2 = children.get(1);
        return c2.map(value -> new ParquetFilters.Or(c1.get(), value));
    }

    private Optional<ParquetFilters.Predicate> convertBinary(
            FieldRef fieldRef,
            Object literal,
            TriFunction<String, DataType, Serializable, ParquetFilters.Predicate> func) {
//        DataType litType = toParquetType(fieldRef.name(), fieldRef.type());
//        if (litType == null) {
//            return Optional.empty();
//        }
        // fetch literal and ensure it is serializable
        Object orcObj = toParquetObject(fieldRef.type(), literal);
        // validate that literal is serializable
        return orcObj instanceof Serializable
                ? Optional.of(func.apply(fieldRef.name(), fieldRef.type(), (Serializable) orcObj))
                : Optional.empty();
    }

    @Nullable
    private static Object toParquetObject(DataType dataType, Object literalObj) {
        if (literalObj == null) {
            return null;
        }

        switch (dataType.getTypeRoot()) {
            case BINARY:
                return literalObj.toString();
            case DECIMAL:
                return ((Decimal) literalObj).toBigDecimal();
            default:
                return literalObj;
        }
    }

//    @Nullable
//    private static DataType toParquetType(String name, DataType type) {
//        switch (type.getTypeRoot()) {
//            case TINYINT:
//            case SMALLINT:
//            case INTEGER:
//                return new DataType(OPTIONAL, INT32, name);
//            case BIGINT:
//                return new DataType(OPTIONAL, INT64, name);
//            case FLOAT:
//                return new DataType(OPTIONAL, FLOAT, name);
//            case DOUBLE:
//                return new DataType(OPTIONAL, DOUBLE, name);
//            case BOOLEAN:
//                return new DataType(OPTIONAL, BOOLEAN, name);
//            case CHAR:
//            case VARCHAR:
//                return new DataType(OPTIONAL, BINARY, name);
//            default:
//                return null;
//        }
//    }

    /**
     * Function which takes three arguments.
     *
     * @param <S> type of the first argument
     * @param <T> type of the second argument
     * @param <U> type of the third argument
     * @param <R> type of the return value
     */
    @FunctionalInterface
    private interface TriFunction<S, T, U, R> {

        /**
         * Applies this function to the given arguments.
         *
         * @param s the first function argument
         * @param t the second function argument
         * @param u the third function argument
         * @return the function result
         */
        R apply(S s, T t, U u);
    }
}
