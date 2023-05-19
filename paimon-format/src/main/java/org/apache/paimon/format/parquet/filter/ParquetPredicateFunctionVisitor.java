package org.apache.paimon.format.parquet.filter;

import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.FunctionVisitor;
import org.apache.parquet.filter2.predicate.FilterPredicate;

import java.util.List;
import java.util.Optional;

public class ParquetPredicateFunctionVisitor implements FunctionVisitor<Optional<FilterPredicate>> {
    public static final ParquetPredicateFunctionVisitor VISITOR = new ParquetPredicateFunctionVisitor();

    private ParquetPredicateFunctionVisitor() {

    }

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
        return Optional.empty();
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
}
