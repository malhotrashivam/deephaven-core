//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pagestore.topage;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;

public abstract class ToBigDecimalBase<ATTR extends Any> implements ToPage<ATTR, BigDecimal[]> {
    protected final byte scale;

    ToBigDecimalBase(@NotNull final Class<?> nativeType, final int precision, final int scale) {
        if (!BigDecimal.class.equals(nativeType)) {
            throw new IllegalArgumentException(
                    "The native type for a BigDecimal column is " + nativeType.getCanonicalName());
        }

        this.scale = (byte) scale;
        if (((int) this.scale) != scale) {
            throw new IllegalArgumentException(
                    "precision=" + precision + " and scale=" + scale + " can't be represented");
        }
    }

    @NotNull
    @Override
    public final Class<?> getNativeType() {
        return BigDecimal.class;
    }

    @Override
    @NotNull
    public final ChunkType getChunkType() {
        return ChunkType.Object;
    }
}
