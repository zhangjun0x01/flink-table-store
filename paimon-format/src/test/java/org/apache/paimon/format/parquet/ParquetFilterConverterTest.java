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

import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.paimon.format.orc.filter.OrcFilters;
import org.apache.paimon.format.parquet.filter.ParquetPredicateFunctionVisitor;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

public class ParquetFilterConverterTest {
    @Test
    public void testApplyPredicate() {
        PredicateBuilder builder =
                new PredicateBuilder(
                        new RowType(
                                Collections.singletonList(
                                        new DataField(0, "long1", new BigIntType()))));
//        test(builder.isNull(0), FilterApi.eq(FilterApi.longColumn("long1"), null));
//        test(builder.isNotNull(0), FilterApi.notEq(FilterApi.longColumn("long1"), null));
//        test(builder.equal(0, 10L), FilterApi.eq(FilterApi.longColumn("long1"), 10L));
//        test(builder.notEqual(0, 10L), FilterApi.notEq(FilterApi.longColumn("long1"), 10L));
//
//        test(builder.lessThan(0, 10L), FilterApi.lt(FilterApi.longColumn("long1"), 10L));
//        test(builder.lessOrEqual(0, 10L), FilterApi.ltEq(FilterApi.longColumn("long1"), 10L));
//        test(builder.greaterThan(0, 10L), FilterApi.gt(FilterApi.longColumn("long1"), 10L));
//        test(builder.greaterOrEqual(0, 10L), FilterApi.gtEq(FilterApi.longColumn("long1"), 10L));

        test(
                builder.in(0, Arrays.asList(1L, 2L, 3L)),
                FilterApi.or(
                        FilterApi.or(
                                FilterApi.gtEq(FilterApi.longColumn("long1"), 1L),
                                FilterApi.gtEq(FilterApi.longColumn("long1"), 2L)),
                        FilterApi.gtEq(FilterApi.longColumn("long1"), 3L)));


        test(
                builder.between(0, 1L, 3L),
                FilterApi.and(
                        FilterApi.gtEq(FilterApi.longColumn("long1"), 1L),
                        FilterApi.ltEq(FilterApi.longColumn("long1"), 3L)));
    }

    private void test(Predicate predicate, FilterPredicate filterPredicate) {
        ParquetPredicateFunctionVisitor visitor = new ParquetPredicateFunctionVisitor();
        if (predicate instanceof LeafPredicate) {
            LeafPredicate leafPredicate = (LeafPredicate) predicate;
            visitor = new ParquetPredicateFunctionVisitor(leafPredicate.function(), null);
        }
        assertThat(predicate.visit(visitor).get()).hasToString(filterPredicate.toString());
    }
}
