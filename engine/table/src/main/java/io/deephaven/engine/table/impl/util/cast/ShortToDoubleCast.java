//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharToDoubleCast and run "./gradlew replicateOperators" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.util.cast;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.util.QueryConstants;

/**
 * Cast the values in the input chunk to an int.
 */
public class ShortToDoubleCast implements ToDoubleCast {
    private final WritableDoubleChunk result;

    ShortToDoubleCast(int size) {
        result = WritableDoubleChunk.makeWritableChunk(size);
    }

    @Override
    public <T> DoubleChunk<? extends T> cast(Chunk<? extends T> input) {
        return cast(input.asShortChunk());
    }

    @SuppressWarnings("unchecked")
    private <T extends Any> DoubleChunk<T> cast(ShortChunk<T> input) {
        castInto(input, result);
        return result;
    }

    public static <T2 extends Any> void castInto(ShortChunk<? extends T2> input, WritableDoubleChunk<T2> result) {
        final int size = input.size();
        for (int ii = 0; ii < size; ++ii) {
            final short value = input.get(ii);
            if (value == QueryConstants.NULL_SHORT) {
                result.set(ii, QueryConstants.NULL_DOUBLE);
            } else {
                result.set(ii, value);
            }
        }
        result.setSize(input.size());
    }

    @Override
    public void close() {
        result.close();
    }
}
