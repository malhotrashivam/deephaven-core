//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit TupleSourceCodeGenerator and run "./gradlew replicateTupleSources" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.tuplesource.generated;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TupleSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.tuplesource.AbstractTupleSource;
import io.deephaven.engine.table.impl.tuplesource.ThreeColumnTupleSourceFactory;
import io.deephaven.tuple.generated.FloatFloatFloatTuple;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;


/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Float, Float, and Float.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class FloatFloatFloatColumnTupleSource extends AbstractTupleSource<FloatFloatFloatTuple> {

    /** {@link ThreeColumnTupleSourceFactory} instance to create instances of {@link FloatFloatFloatColumnTupleSource}. **/
    public static final ThreeColumnTupleSourceFactory<FloatFloatFloatTuple, Float, Float, Float> FACTORY = new Factory();

    private final ColumnSource<Float> columnSource1;
    private final ColumnSource<Float> columnSource2;
    private final ColumnSource<Float> columnSource3;

    public FloatFloatFloatColumnTupleSource(
            @NotNull final ColumnSource<Float> columnSource1,
            @NotNull final ColumnSource<Float> columnSource2,
            @NotNull final ColumnSource<Float> columnSource3
    ) {
        super(columnSource1, columnSource2, columnSource3);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
        this.columnSource3 = columnSource3;
    }

    @Override
    public final FloatFloatFloatTuple createTuple(final long rowKey) {
        return new FloatFloatFloatTuple(
                columnSource1.getFloat(rowKey),
                columnSource2.getFloat(rowKey),
                columnSource3.getFloat(rowKey)
        );
    }

    @Override
    public final FloatFloatFloatTuple createPreviousTuple(final long rowKey) {
        return new FloatFloatFloatTuple(
                columnSource1.getPrevFloat(rowKey),
                columnSource2.getPrevFloat(rowKey),
                columnSource3.getPrevFloat(rowKey)
        );
    }

    @Override
    public final FloatFloatFloatTuple createTupleFromValues(@NotNull final Object... values) {
        return new FloatFloatFloatTuple(
                TypeUtils.unbox((Float)values[0]),
                TypeUtils.unbox((Float)values[1]),
                TypeUtils.unbox((Float)values[2])
        );
    }

    @Override
    public final FloatFloatFloatTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new FloatFloatFloatTuple(
                TypeUtils.unbox((Float)values[0]),
                TypeUtils.unbox((Float)values[1]),
                TypeUtils.unbox((Float)values[2])
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final FloatFloatFloatTuple tuple, final int elementIndex, @NotNull final WritableColumnSource<ELEMENT_TYPE> writableSource, final long destinationRowKey) {
        if (elementIndex == 0) {
            writableSource.set(destinationRowKey, tuple.getFirstElement());
            return;
        }
        if (elementIndex == 1) {
            writableSource.set(destinationRowKey, tuple.getSecondElement());
            return;
        }
        if (elementIndex == 2) {
            writableSource.set(destinationRowKey, tuple.getThirdElement());
            return;
        }
        throw new IndexOutOfBoundsException("Invalid element index " + elementIndex + " for export");
    }

    @Override
    public final Object exportElement(@NotNull final FloatFloatFloatTuple tuple, int elementIndex) {
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
    public final Object exportElementReinterpreted(@NotNull final FloatFloatFloatTuple tuple, int elementIndex) {
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
        WritableObjectChunk<FloatFloatFloatTuple, ? super Values> destinationObjectChunk = destination.asWritableObjectChunk();
        FloatChunk<? extends Values> chunk1 = chunks[0].asFloatChunk();
        FloatChunk<? extends Values> chunk2 = chunks[1].asFloatChunk();
        FloatChunk<? extends Values> chunk3 = chunks[2].asFloatChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new FloatFloatFloatTuple(chunk1.get(ii), chunk2.get(ii), chunk3.get(ii)));
        }
        destinationObjectChunk.setSize(chunkSize);
    }

    /** {@link ThreeColumnTupleSourceFactory} for instances of {@link FloatFloatFloatColumnTupleSource}. **/
    private static final class Factory implements ThreeColumnTupleSourceFactory<FloatFloatFloatTuple, Float, Float, Float> {

        private Factory() {
        }

        @Override
        public TupleSource<FloatFloatFloatTuple> create(
                @NotNull final ColumnSource<Float> columnSource1,
                @NotNull final ColumnSource<Float> columnSource2,
                @NotNull final ColumnSource<Float> columnSource3
        ) {
            return new FloatFloatFloatColumnTupleSource(
                    columnSource1,
                    columnSource2,
                    columnSource3
            );
        }
    }
}
