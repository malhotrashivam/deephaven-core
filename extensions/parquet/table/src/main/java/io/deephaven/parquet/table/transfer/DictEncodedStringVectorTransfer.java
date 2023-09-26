/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.transfer;

import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.vector.ObjectVector;
import org.jetbrains.annotations.NotNull;

import java.util.function.Supplier;

final public class DictEncodedStringVectorTransfer
        extends DictEncodedStringTransferBase<ObjectVector<String>> {
    public DictEncodedStringVectorTransfer(@NotNull ColumnSource<?> columnSource, @NotNull RowSequence tableRowSet,
            int targetPageSize, StringDictionary dictionary, final int nullPos) {
        super(columnSource, tableRowSet, targetPageSize, dictionary, nullPos);
    }

    @Override
    EncodedData encodeDataForBuffering(@NotNull ObjectVector<String> data) {
        try (CloseableIterator<String> iter = data.iterator()) {
            Supplier<String> supplier = iter::next;
            return dictEncodingHelper(supplier, data.intSize());
        }
    }
}
