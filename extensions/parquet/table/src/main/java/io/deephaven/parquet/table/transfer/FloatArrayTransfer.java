/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit IntArrayTransfer and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

import java.nio.FloatBuffer;

final class FloatArrayTransfer extends PrimitiveArrayAndVectorTransfer<float[], float[], FloatBuffer> {
    FloatArrayTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
                     final int targetPageSize) {
        super(columnSource, tableRowSet, targetPageSize / Float.BYTES, targetPageSize,
                FloatBuffer.allocate(targetPageSize / Float.BYTES), Float.BYTES);
    }

    @Override
    int getSize(final float @NotNull [] data) {
        return data.length;
    }

    @Override
    void resizeBuffer(final int length) {
        buffer = FloatBuffer.allocate(length);
    }

    @Override
    void copyToBuffer(final float @NotNull [] data) {
        buffer.put(data);
    }
}
