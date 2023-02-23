/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.filter;

import io.deephaven.api.RawString;
import io.deephaven.api.expression.Expression;
import io.deephaven.api.value.Literal;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Represents an evaluate-able filter.
 *
 * @see io.deephaven.api.TableOperations#where(Collection)
 */
public interface Filter extends Expression, Serializable {

    static Collection<? extends Filter> from(String... expressions) {
        return from(Arrays.asList(expressions));
    }

    static Collection<? extends Filter> from(Collection<String> expressions) {
        return expressions.stream().map(RawString::of).collect(Collectors.toList());
    }

    static Collection<? extends Filter> from_(String... expressions) {
        // This is for Python to invoke "from" without syntax errors.
        return from(expressions);
    }

    /**
     * Creates an is-null-filter.
     *
     * @param expression the expression
     * @return the is-null-filter
     */
    static FilterIsNull isNull(Expression expression) {
        return FilterIsNull.of(expression);
    }

    /**
     * Creates an is-not-null-filter.
     *
     * @param expression the expression
     * @return the is-not-null-filter
     */
    static FilterIsNotNull isNotNull(Expression expression) {
        return FilterIsNotNull.of(expression);
    }

    /**
     * Creates an is-true-filter.
     *
     * <p>
     * Equivalent to {@code FilterComparison.eq(expression, Literal.of(true))}.
     *
     * @param expression the expression
     * @return the equals-true-filter
     */
    static FilterComparison isTrue(Expression expression) {
        return FilterComparison.eq(expression, Literal.of(true));
    }

    /**
     * Creates an is-false-filter.
     *
     * <p>
     * Equivalent to {@code FilterComparison.eq(expression, Literal.of(false))}.
     *
     * @param expression the expression
     * @return @return the equals-false-filter
     */
    static FilterComparison isFalse(Expression expression) {
        return FilterComparison.eq(expression, Literal.of(false));
    }

    /**
     * Creates a {@link FilterNot not-filter} from {@code filter}. Callers should typically prefer
     * {@link Filter#inverse()}, unless the "not" context needs to be preserved.
     *
     * @param filter the filter
     * @return the not-filter
     */
    static FilterNot not(Filter filter) {
        return FilterNot.of(filter);
    }

    /**
     * Creates an {@link FilterOr or-filter}.
     *
     * @param filters the filters
     * @return the or-filter
     */
    static FilterOr or(Filter... filters) {
        return FilterOr.of(filters);
    }

    /**
     * Creates an {@link FilterOr or-filter}.
     *
     * @param filters the filters
     * @return the or-filter
     */
    static FilterOr or(Iterable<? extends Filter> filters) {
        return FilterOr.of(filters);
    }

    /**
     * Creates an {@link FilterAnd and-filter}.
     *
     * @param filters the filters
     * @return the and-filter
     */
    static FilterAnd and(Filter... filters) {
        return FilterAnd.of(filters);
    }

    /**
     * Creates an {@link FilterAnd and-filter}.
     *
     * @param filters the filters
     * @return the and-filter
     */
    static FilterAnd and(Iterable<? extends Filter> filters) {
        return FilterAnd.of(filters);
    }

    /**
     * The logical inversion of {@code this}.
     *
     * @return the inverse filter
     */
    Filter inverse();

    <V extends Visitor> V walk(V visitor);

    interface Visitor {
        // TODO (deephaven-core#829): Add more table api Filter structuring

        // note: isNull is a "special" case, as the caller doesn't technically need to know the return type of the
        // expression (even though they should technically know, and the engine will implicitly choose the appropriate
        // call).
        //
        // The same can't be said about boolean; if you want to check whether an expression is true, that will be
        // represented with a filter comparison against a literal boolean

        void visit(FilterIsNull isNull);

        void visit(FilterIsNotNull isNotNull);

        void visit(FilterComparison comparison);

        void visit(FilterNot not);

        void visit(FilterOr ors);

        void visit(FilterAnd ands);

        void visit(RawString rawString);
    }
}
