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

import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.FunctionVisitor;

import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;

import java.util.List;
import java.util.Optional;

import static org.apache.paimon.types.DataTypeRoot.INTEGER;

/** aaaa. */
public class ParquetPredicateFunctionVisitor implements FunctionVisitor<Optional<FilterPredicate>> {
    public static final ParquetPredicateFunctionVisitor VISITOR =
            new ParquetPredicateFunctionVisitor();

    private ParquetPredicateFunctionVisitor() {}

    @Override
    public Optional<FilterPredicate> visitIsNotNull(FieldRef fieldRef) {
        return Optional.empty();
    }

    @Override
    public Optional<FilterPredicate> visitIsNull(FieldRef fieldRef) {
        return Optional.empty();
    }

    @Override
    public Optional<FilterPredicate> visitStartsWith(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<FilterPredicate> visitLessThan(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<FilterPredicate> visitGreaterOrEqual(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<FilterPredicate> visitNotEqual(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<FilterPredicate> visitLessOrEqual(FieldRef fieldRef, Object literal) {
        return Optional.empty();
    }

    @Override
    public Optional<FilterPredicate> visitEqual(FieldRef fieldRef, Object literal) {
        return convertBinary(fieldRef, literal);
    }

    private <T> Optional<FilterPredicate> convertBinary(FieldRef fieldRef, Object literal) {

        Operators.IntColumn column = null;
        if (fieldRef.type().getTypeRoot().equals(INTEGER)) {
            column = FilterApi.intColumn(fieldRef.name());
        }

        //        PredicateLeaf.Type litType = toParquetType(fieldRef.type());
        //        if (litType == null) {
        //            return Optional.empty();
        //        }
        // fetch literal and ensure it is serializable
        return Optional.of(FilterApi.eq(column, toParquetObject(literal)));

        // validate that literal is serializable
        //        return orcObj instanceof Serializable
        //                ? Optional.of(func.apply(fieldRef.name(), litType, (Serializable) orcObj))
        //                : Optional.empty();
    }

    private <C extends Comparable<C>> C toParquetObject(Object literal) {
        return (C) literal;
    }

    @Override
    public Optional<FilterPredicate> visitGreaterThan(FieldRef fieldRef, Object literal) {
        return Optional.empty();
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
