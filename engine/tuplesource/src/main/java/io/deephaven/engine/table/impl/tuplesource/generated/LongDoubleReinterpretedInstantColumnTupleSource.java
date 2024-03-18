//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit TupleSourceCodeGenerator and run "./gradlew replicateTupleSources" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.tuplesource.generated;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TupleSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.tuplesource.AbstractTupleSource;
import io.deephaven.engine.table.impl.tuplesource.ThreeColumnTupleSourceFactory;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.tuple.generated.LongDoubleLongTuple;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;

/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Long, Double, and Long.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class LongDoubleReinterpretedInstantColumnTupleSource extends AbstractTupleSource<LongDoubleLongTuple> {

    /** {@link ThreeColumnTupleSourceFactory} instance to create instances of {@link LongDoubleReinterpretedInstantColumnTupleSource}. **/
    public static final ThreeColumnTupleSourceFactory<LongDoubleLongTuple, Long, Double, Long> FACTORY = new Factory();

    private final ColumnSource<Long> columnSource1;
    private final ColumnSource<Double> columnSource2;
    private final ColumnSource<Long> columnSource3;

    public LongDoubleReinterpretedInstantColumnTupleSource(
            @NotNull final ColumnSource<Long> columnSource1,
            @NotNull final ColumnSource<Double> columnSource2,
            @NotNull final ColumnSource<Long> columnSource3
    ) {
        super(columnSource1, columnSource2, columnSource3);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
        this.columnSource3 = columnSource3;
    }

    @Override
    public final LongDoubleLongTuple createTuple(final long rowKey) {
        return new LongDoubleLongTuple(
                columnSource1.getLong(rowKey),
                columnSource2.getDouble(rowKey),
                columnSource3.getLong(rowKey)
        );
    }

    @Override
    public final LongDoubleLongTuple createPreviousTuple(final long rowKey) {
        return new LongDoubleLongTuple(
                columnSource1.getPrevLong(rowKey),
                columnSource2.getPrevDouble(rowKey),
                columnSource3.getPrevLong(rowKey)
        );
    }

    @Override
    public final LongDoubleLongTuple createTupleFromValues(@NotNull final Object... values) {
        return new LongDoubleLongTuple(
                TypeUtils.unbox((Long)values[0]),
                TypeUtils.unbox((Double)values[1]),
                DateTimeUtils.epochNanos((Instant)values[2])
        );
    }

    @Override
    public final LongDoubleLongTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new LongDoubleLongTuple(
                TypeUtils.unbox((Long)values[0]),
                TypeUtils.unbox((Double)values[1]),
                TypeUtils.unbox((Long)values[2])
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final LongDoubleLongTuple tuple, final int elementIndex, @NotNull final WritableColumnSource<ELEMENT_TYPE> writableSource, final long destinationRowKey) {
        if (elementIndex == 0) {
            writableSource.set(destinationRowKey, tuple.getFirstElement());
            return;
        }
        if (elementIndex == 1) {
            writableSource.set(destinationRowKey, tuple.getSecondElement());
            return;
        }
        if (elementIndex == 2) {
            writableSource.set(destinationRowKey, (ELEMENT_TYPE) DateTimeUtils.epochNanosToInstant(tuple.getThirdElement()));
            return;
        }
        throw new IndexOutOfBoundsException("Invalid element index " + elementIndex + " for export");
    }

    @Override
    public final Object exportElement(@NotNull final LongDoubleLongTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return TypeUtils.box(tuple.getSecondElement());
        }
        if (elementIndex == 2) {
            return DateTimeUtils.epochNanosToInstant(tuple.getThirdElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }

    @Override
    public final Object exportElementReinterpreted(@NotNull final LongDoubleLongTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return TypeUtils.box(tuple.getSecondElement());
        }
        if (elementIndex == 2) {
            return TypeUtils.box(tuple.getThirdElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }

    @Override
    protected void convertChunks(@NotNull WritableChunk<? super Values> destination, int chunkSize, Chunk<? extends Values> [] chunks) {
        WritableObjectChunk<LongDoubleLongTuple, ? super Values> destinationObjectChunk = destination.asWritableObjectChunk();
        LongChunk<? extends Values> chunk1 = chunks[0].asLongChunk();
        DoubleChunk<? extends Values> chunk2 = chunks[1].asDoubleChunk();
        LongChunk<? extends Values> chunk3 = chunks[2].asLongChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new LongDoubleLongTuple(chunk1.get(ii), chunk2.get(ii), chunk3.get(ii)));
        }
        destinationObjectChunk.setSize(chunkSize);
    }

    /** {@link ThreeColumnTupleSourceFactory} for instances of {@link LongDoubleReinterpretedInstantColumnTupleSource}. **/
    private static final class Factory implements ThreeColumnTupleSourceFactory<LongDoubleLongTuple, Long, Double, Long> {

        private Factory() {
        }

        @Override
        public TupleSource<LongDoubleLongTuple> create(
                @NotNull final ColumnSource<Long> columnSource1,
                @NotNull final ColumnSource<Double> columnSource2,
                @NotNull final ColumnSource<Long> columnSource3
        ) {
            return new LongDoubleReinterpretedInstantColumnTupleSource(
                    columnSource1,
                    columnSource2,
                    columnSource3
            );
        }
    }
}
