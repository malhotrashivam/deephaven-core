//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.chunkfilter.ChunkFilter;
import io.deephaven.engine.table.impl.chunkfilter.ObjectChunkFilter;
import io.deephaven.util.compare.ObjectComparisons;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.util.annotations.TestUseOnly;
import org.jetbrains.annotations.NotNull;

public class ComparableRangeFilter extends AbstractRangeFilter {
    private final Comparable<?> upper;
    private final Comparable<?> lower;
    private Class<?> columnType;

    ComparableRangeFilter(String columnName, Comparable<?> val1, Comparable<?> val2, boolean lowerInclusive,
            boolean upperInclusive) {
        super(columnName, lowerInclusive, upperInclusive);

        if (ObjectComparisons.compare(val1, val2) > 0) {
            upper = val1;
            lower = val2;
        } else {
            upper = val2;
            lower = val1;
        }
    }

    public final Comparable<?> getUpper() {
        return upper;
    }

    public final Comparable<?> getLower() {
        return lower;
    }

    public Class<?> getColumnType() {
        return columnType;
    }

    @TestUseOnly
    public static ComparableRangeFilter makeForTest(String columnName, Comparable<?> lower, Comparable<?> upper,
            boolean lowerInclusive, boolean upperInclusive) {
        return new ComparableRangeFilter(columnName, lower, upper, lowerInclusive, upperInclusive);
    }

    @Override
    public void init(@NotNull final TableDefinition tableDefinition) {
        if (chunkFilter != null) {
            return;
        }

        final ColumnDefinition<?> def = tableDefinition.getColumn(columnName);
        if (def == null) {
            throw new RuntimeException("Column \"" + columnName + "\" doesn't exist in this table, available columns: "
                    + tableDefinition.getColumnNames());
        }
        columnType = def.getDataType();

        Assert.assertion(Comparable.class.isAssignableFrom(columnType), "Comparable.class.isAssignableFrom(columnType)",
                columnType, "columnType");

        chunkFilter = makeComparableChunkFilter(lower, upper, lowerInclusive, upperInclusive);
    }

    public static ChunkFilter makeComparableChunkFilter(
            Comparable<?> lower, Comparable<?> upper, boolean lowerInclusive, boolean upperInclusive) {
        if (lowerInclusive) {
            if (upperInclusive) {
                return new InclusiveInclusiveComparableChunkFilter(lower, upper);
            } else {
                return new InclusiveExclusiveComparableChunkFilter(lower, upper);
            }
        } else {
            if (upperInclusive) {
                return new ExclusiveInclusiveComparableChunkFilter(lower, upper);
            } else {
                return new ExclusiveExclusiveComparableChunkFilter(lower, upper);
            }
        }
    }

    @Override
    public WhereFilter copy() {
        final ComparableRangeFilter copy =
                new ComparableRangeFilter(columnName, lower, upper, lowerInclusive, upperInclusive);
        copy.chunkFilter = chunkFilter;
        copy.longFilter = longFilter;
        copy.columnType = columnType;
        return copy;
    }

    @Override
    public String toString() {
        return "ComparableRangeFilter(" + columnName + " in " +
                (lowerInclusive ? "[" : "(") + lower + "," + upper +
                (upperInclusive ? "]" : ")") + ")";
    }

    private final static class InclusiveInclusiveComparableChunkFilter
            extends ObjectChunkFilter<Comparable<?>> {
        private final Comparable<?> lower;
        private final Comparable<?> upper;

        private InclusiveInclusiveComparableChunkFilter(Comparable<?> lower, Comparable<?> upper) {
            this.lower = lower;
            this.upper = upper;
        }

        @Override
        public boolean matches(Comparable<?> value) {
            return ObjectComparisons.compare(lower, value) <= 0 && ObjectComparisons.compare(upper, value) >= 0;
        }
    }

    private final static class InclusiveExclusiveComparableChunkFilter
            extends ObjectChunkFilter<Comparable<?>> {
        private final Comparable<?> lower;
        private final Comparable<?> upper;

        private InclusiveExclusiveComparableChunkFilter(Comparable<?> lower, Comparable<?> upper) {
            this.lower = lower;
            this.upper = upper;
        }

        @Override
        public boolean matches(Comparable<?> value) {
            return ObjectComparisons.compare(lower, value) <= 0 && ObjectComparisons.compare(upper, value) > 0;
        }
    }

    private final static class ExclusiveInclusiveComparableChunkFilter
            extends ObjectChunkFilter<Comparable<?>> {
        private final Comparable<?> lower;
        private final Comparable<?> upper;

        private ExclusiveInclusiveComparableChunkFilter(Comparable<?> lower, Comparable<?> upper) {
            this.lower = lower;
            this.upper = upper;
        }

        @Override
        public boolean matches(Comparable<?> value) {
            return ObjectComparisons.compare(lower, value) < 0 && ObjectComparisons.compare(upper, value) >= 0;
        }
    }

    private final static class ExclusiveExclusiveComparableChunkFilter
            extends ObjectChunkFilter<Comparable<?>> {
        private final Comparable<?> lower;
        private final Comparable<?> upper;

        private ExclusiveExclusiveComparableChunkFilter(Comparable<?> lower, Comparable<?> upper) {
            this.lower = lower;
            this.upper = upper;
        }

        @Override
        public boolean matches(Comparable<?> value) {
            return ObjectComparisons.compare(lower, value) < 0 && ObjectComparisons.compare(upper, value) > 0;
        }
    }

    @NotNull
    @Override
    WritableRowSet binarySearch(
            @NotNull final RowSet selection,
            @NotNull final ColumnSource<?> columnSource,
            final boolean usePrev,
            final boolean reverse) {
        if (selection.isEmpty()) {
            return selection.copy();
        }

        // noinspection unchecked
        final ColumnSource<Comparable<?>> comparableColumnSource = (ColumnSource<Comparable<?>>) columnSource;

        final Comparable<?> startValue = reverse ? upper : lower;
        final Comparable<?> endValue = reverse ? lower : upper;
        final boolean startInclusive = reverse ? upperInclusive : lowerInclusive;
        final boolean endInclusive = reverse ? lowerInclusive : upperInclusive;
        final int compareSign = reverse ? -1 : 1;

        long lowerBoundMin = bound(selection, usePrev, comparableColumnSource, 0, selection.size(), startValue,
                startInclusive, compareSign, false);
        long upperBoundMin = bound(selection, usePrev, comparableColumnSource, lowerBoundMin, selection.size(),
                endValue, endInclusive, compareSign, true);

        return selection.subSetByPositionRange(lowerBoundMin, upperBoundMin);
    }


    static long bound(RowSet selection, boolean usePrev, ColumnSource<Comparable<?>> comparableColumnSource,
            long minPosition, long maxPosition, Comparable<?> targetValue, boolean inclusive, int compareSign,
            boolean end) {
        while (minPosition < maxPosition) {
            final long midPos = (minPosition + maxPosition) / 2;
            final long midIdx = selection.get(midPos);

            final Comparable<?> compareValue =
                    usePrev ? comparableColumnSource.getPrev(midIdx) : comparableColumnSource.get(midIdx);
            final int compareResult = compareSign * ObjectComparisons.compare(compareValue, targetValue);

            if (compareResult < 0) {
                minPosition = midPos + 1;
            } else if (compareResult > 0) {
                maxPosition = midPos;
            } else {
                if (end) {
                    if (inclusive) {
                        minPosition = midPos + 1;
                    } else {
                        maxPosition = midPos;
                    }
                } else {
                    if (inclusive) {
                        maxPosition = midPos;
                    } else {
                        minPosition = midPos + 1;
                    }
                }
            }
        }
        return minPosition;
    }
}
