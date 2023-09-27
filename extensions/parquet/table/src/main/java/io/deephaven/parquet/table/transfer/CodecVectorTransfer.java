/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.util.codec.ObjectCodec;
import org.apache.parquet.io.api.Binary;
import org.jetbrains.annotations.NotNull;

/**
 * Used to encode vectors of objects using a codec provided on construction. The difference between using this class and
 * {@link CodecTransfer} is that this class encodes each element of the vector individually whereas
 * {@link CodecTransfer} will encode the entire vector as a single value.
 */
final class CodecVectorTransfer<T> extends ObjectVectorTransfer<T> {
    private final ObjectCodec<? super T> codec;

    CodecVectorTransfer(final @NotNull ColumnSource<?> columnSource, @NotNull final ObjectCodec<? super T> codec,
            final @NotNull RowSequence tableRowSet, final int targetPageSize) {
        super(columnSource, tableRowSet, targetPageSize);
        this.codec = codec;
    }

    @Override
    Binary encodeToBinary(T value) {
        return Binary.fromConstantByteArray(codec.encode(value));
    }
}
