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

package org.apache.paimon.format.parquet;

import org.apache.paimon.format.orc.filter.OrcPredicateFunctionVisitor;
import org.apache.paimon.format.parquet.filter.ParquetFilters;
import org.apache.paimon.format.parquet.filter.ParquetPredicateFunctionVisitor1;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit Tests for {@link ParquetPredicateFunctionVisitor1}. */
public class ParquetFilterConverterTest {
    @Test
    public void testApplyPredicate() {
        PredicateBuilder builder =
                new PredicateBuilder(
                        new RowType(
                                Collections.singletonList(
                                        new DataField(0, "long1", new BigIntType()))));
        test(builder.isNull(0), new ParquetFilters.IsNull("long1", new BigIntType()));
        test(
                builder.isNotNull(0),
                new ParquetFilters.Not(new ParquetFilters.IsNull("long1", new BigIntType())));
        test(builder.equal(0, 10L), new ParquetFilters.Equals("long1", new BigIntType(), 10));
        test(
                builder.notEqual(0, 10L),
                new ParquetFilters.Not(new ParquetFilters.Equals("long1", new BigIntType(), 10)));
        test(
                builder.lessThan(0, 10L),
                new ParquetFilters.LessThan("long1", new BigIntType(), 10));
        test(
                builder.lessOrEqual(0, 10L),
                new ParquetFilters.LessThanEquals("long1", new BigIntType(), 10));
        test(
                builder.greaterThan(0, 10L),
                new ParquetFilters.Not(
                        new ParquetFilters.LessThanEquals("long1", new BigIntType(), 10)));
        test(
                builder.greaterOrEqual(0, 10L),
                new ParquetFilters.Not(new ParquetFilters.LessThan("long1", new BigIntType(), 10)));

        test(
                builder.in(0, Arrays.asList(1L, 2L, 3L)),
                new ParquetFilters.Or(
                        new ParquetFilters.Or(
                                new ParquetFilters.Equals("long1", new BigIntType(), 1),
                                new ParquetFilters.Equals("long1", new BigIntType(), 2)),
                        new ParquetFilters.Equals("long1", new BigIntType(), 3)));

        test(
                builder.between(0, 1L, 3L),
                new ParquetFilters.And(
                        new ParquetFilters.Not(
                                new ParquetFilters.LessThan("long1", new BigIntType(), 1)),
                        new ParquetFilters.LessThanEquals("long1", new BigIntType(), 3)));

        test(
                builder.notIn(0, Arrays.asList(1L, 2L, 3L)),
                new ParquetFilters.And(
                        new ParquetFilters.And(
                                new ParquetFilters.Not(
                                        new ParquetFilters.Equals("long1", new BigIntType(), 1)),
                                new ParquetFilters.Not(
                                        new ParquetFilters.Equals(
                                                "long1", new BigIntType(), 2))),
                        new ParquetFilters.Not(
                                new ParquetFilters.Equals("long1", new BigIntType(), 3))));

        assertThat(
                builder.in(
                                0,
                                LongStream.range(1L, 22L)
                                        .boxed()
                                        .collect(Collectors.toList()))
                        .visit(OrcPredicateFunctionVisitor.VISITOR)
                        .isPresent())
                .isFalse();

        assertThat(
                builder.notIn(
                                0,
                                LongStream.range(1L, 22L)
                                        .boxed()
                                        .collect(Collectors.toList()))
                        .visit(OrcPredicateFunctionVisitor.VISITOR)
                        .isPresent())
                .isFalse();
    }

    private void test(Predicate predicate, ParquetFilters.Predicate filterPredicate) {
        assertThat(predicate.visit(ParquetPredicateFunctionVisitor1.VISITOR).get())
                .hasToString(filterPredicate.toString());
    }
}
