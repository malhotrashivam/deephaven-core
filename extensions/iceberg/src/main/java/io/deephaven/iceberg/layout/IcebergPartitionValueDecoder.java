//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.layout;

import org.apache.iceberg.PartitionData;
import org.apache.iceberg.transforms.PartitionSpecVisitor;
import org.apache.iceberg.util.DateTimeUtil;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.function.UnaryOperator;

/**
 * This class is used to generate {@link UnaryOperator} which take partition value derived from the Iceberg's
 * {@link PartitionData} and convert it to a user-readable value to be used as the partition value in the Deephaven
 * table.
 */
enum IcebergPartitionValueDecoder implements PartitionSpecVisitor<UnaryOperator<Object>> {
    INSTANCE;

    private static final int EPOCH_YEAR = DateTimeUtil.EPOCH.getYear();

    @Override
    public UnaryOperator<Object> identity(final String sourceName, final int sourceId) {
        return UnaryOperator.identity();
    }

    @Override
    public UnaryOperator<Object> truncate(final String sourceName, final int sourceId, final int width) {
        // For truncate transform, we can't fully reverse it, so we return the truncated value
        return UnaryOperator.identity();
    }

    @Override
    public UnaryOperator<Object> bucket(final String sourceName, final int sourceId, final int numBuckets) {
        // For bucket transform, we can't reverse it, just return the bucket number
        return UnaryOperator.identity();
    }

    @Override
    public UnaryOperator<Object> year(final String sourceName, final int sourceId) {
        return value -> {
            if (!(value instanceof Integer)) {
                throw new IllegalArgumentException("Year transform value must be an integer");
            }
            // For year transform, the value is years since Epoch
            final Integer yearsSinceEpoch = (Integer) value;
            return yearsSinceEpoch + EPOCH_YEAR;
        };
    }

    @Override
    public UnaryOperator<Object> month(final String sourceName, final int sourceId) {
        return (value) -> {
            if (!(value instanceof Integer)) {
                throw new IllegalArgumentException("Month transform value must be an integer");
            }
            // For month transform, the value is months since Epoch
            final int monthsSinceEpoch = (Integer) value;
            final int year = EPOCH_YEAR + monthsSinceEpoch / 12;
            final int month = 1 + monthsSinceEpoch % 12;
            return String.format("%04d-%02d", year, month);
        };
    }

    @Override
    public UnaryOperator<Object> day(final String sourceName, final int sourceId) {
        return (value) -> {
            if (!(value instanceof Integer)) {
                throw new IllegalArgumentException("Day transform value must be an integer");
            }
            // For day transform, the value is days since Epoch
            // TODO Maybe we can convert it to a LocalDate string
            final long daysSinceEpoch = (Integer) value;
            return LocalDate.ofEpochDay(daysSinceEpoch).toString();
        };
    }

    @Override
    public UnaryOperator<Object> hour(final String sourceName, final int sourceId) {
        // For hour transform, the value is hours since Epoch
        return (transformedHour) -> {
            if (!(transformedHour instanceof Integer)) {
                throw new IllegalArgumentException("Hour transform value must be an integer");
            }
            // For hour transform, the value is hours since Epoch
            // TODO Maybe we can convert it to a LocalDateTime string
            final long hoursSinceEpoch = (Integer) transformedHour;
            return LocalDateTime.ofEpochSecond(hoursSinceEpoch * 60 * 60, 0, ZoneOffset.UTC)
                    .toString();
        };
    }

    @Override
    public UnaryOperator<Object> alwaysNull(int fieldId, String sourceName, int sourceId) {
        return UnaryOperator.identity();
    }

    @Override
    public UnaryOperator<Object> unknown(int fieldId, String sourceName, int sourceId, String transform) {
        return UnaryOperator.identity();
    }
}
