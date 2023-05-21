package org.apache.paimon.format.parquet.filter;

import org.apache.paimon.predicate.Predicate;

import org.apache.parquet.filter2.compat.FilterCompat;

import java.util.List;

public class ParquetFilters {

    public static FilterCompat.Filter convert(List<Predicate> filters) {
        return null;
    }
}
