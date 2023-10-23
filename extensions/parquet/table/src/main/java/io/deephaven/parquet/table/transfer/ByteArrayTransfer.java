/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharArrayTransfer and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.parquet.base.BulkWriter;
import org.apache.parquet.column.statistics.Statistics;
import org.jetbrains.annotations.NotNull;

import java.nio.IntBuffer;

final class ByteArrayTransfer extends PrimitiveArrayAndVectorTransfer<byte[], byte[], IntBuffer> {
    ByteArrayTransfer(@NotNull final ColumnSource<?> columnSource, @NotNull final RowSequence tableRowSet,
                      final int targetPageSize) {
        // We encode primitive bytes as primitive ints
        super(columnSource, tableRowSet, targetPageSize / Integer.BYTES, targetPageSize,
                IntBuffer.allocate(1), Integer.BYTES);
    }

    BulkWriter writer;
    Statistics<?> stats;

    @Override
    public void setWriter(final BulkWriter writer) {
        this.writer = writer;
    }
    @Override
    public void setStats(final Statistics<?> stats) {
        this.stats = stats;
    }


    @Override
    int getSize(final byte @NotNull [] data) {
        return data.length;
    }

    @Override
    void resizeBuffer(final int length) {
//        buffer = IntBuffer.allocate(length);
    }

    @Override
    void copyToBuffer(@NotNull final EncodedData<byte[]> data) {
//        for (byte value : data.encodedValues) {
//            buffer.put(value);
//        }
        writer.clearNullOffsets();
        writer.ensureCapacityFor(data.encodedValues.length);
        for (int i = 0; i < data.encodedValues.length; i++) {
            writer.writeValue(i, data.encodedValues[i], stats);
        }
    }
}
