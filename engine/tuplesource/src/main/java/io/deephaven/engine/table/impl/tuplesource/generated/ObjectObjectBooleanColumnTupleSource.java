//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit TupleSourceCodeGenerator and run "./gradlew replicateTupleSources" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.tuplesource.generated;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TupleSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.tuplesource.AbstractTupleSource;
import io.deephaven.engine.table.impl.tuplesource.ThreeColumnTupleSourceFactory;
import io.deephaven.tuple.generated.ObjectObjectByteTuple;
import io.deephaven.util.BooleanUtils;
import org.jetbrains.annotations.NotNull;


/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Object, Object, and Boolean.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class ObjectObjectBooleanColumnTupleSource extends AbstractTupleSource<ObjectObjectByteTuple> {

    /** {@link ThreeColumnTupleSourceFactory} instance to create instances of {@link ObjectObjectBooleanColumnTupleSource}. **/
    public static final ThreeColumnTupleSourceFactory<ObjectObjectByteTuple, Object, Object, Boolean> FACTORY = new Factory();

    private final ColumnSource<Object> columnSource1;
    private final ColumnSource<Object> columnSource2;
    private final ColumnSource<Boolean> columnSource3;

    public ObjectObjectBooleanColumnTupleSource(
            @NotNull final ColumnSource<Object> columnSource1,
            @NotNull final ColumnSource<Object> columnSource2,
            @NotNull final ColumnSource<Boolean> columnSource3
    ) {
        super(columnSource1, columnSource2, columnSource3);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
        this.columnSource3 = columnSource3;
    }

    @Override
    public final ObjectObjectByteTuple createTuple(final long rowKey) {
        return new ObjectObjectByteTuple(
                columnSource1.get(rowKey),
                columnSource2.get(rowKey),
                BooleanUtils.booleanAsByte(columnSource3.getBoolean(rowKey))
        );
    }

    @Override
    public final ObjectObjectByteTuple createPreviousTuple(final long rowKey) {
        return new ObjectObjectByteTuple(
                columnSource1.getPrev(rowKey),
                columnSource2.getPrev(rowKey),
                BooleanUtils.booleanAsByte(columnSource3.getPrevBoolean(rowKey))
        );
    }

    @Override
    public final ObjectObjectByteTuple createTupleFromValues(@NotNull final Object... values) {
        return new ObjectObjectByteTuple(
                values[0],
                values[1],
                BooleanUtils.booleanAsByte((Boolean)values[2])
        );
    }

    @Override
    public final ObjectObjectByteTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new ObjectObjectByteTuple(
                values[0],
                values[1],
                BooleanUtils.booleanAsByte((Boolean)values[2])
        );
    }

    @Override
    public final int tupleLength() {
        return 3;
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final ObjectObjectByteTuple tuple, final int elementIndex, @NotNull final WritableColumnSource<ELEMENT_TYPE> writableSource, final long destinationRowKey) {
        if (elementIndex == 0) {
            writableSource.set(destinationRowKey, (ELEMENT_TYPE) tuple.getFirstElement());
            return;
        }
        if (elementIndex == 1) {
            writableSource.set(destinationRowKey, (ELEMENT_TYPE) tuple.getSecondElement());
            return;
        }
        if (elementIndex == 2) {
            writableSource.set(destinationRowKey, (ELEMENT_TYPE) BooleanUtils.byteAsBoolean(tuple.getThirdElement()));
            return;
        }
        throw new IndexOutOfBoundsException("Invalid element index " + elementIndex + " for export");
    }

    @Override
    public final Object exportElement(@NotNull final ObjectObjectByteTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return tuple.getFirstElement();
        }
        if (elementIndex == 1) {
            return tuple.getSecondElement();
        }
        if (elementIndex == 2) {
            return BooleanUtils.byteAsBoolean(tuple.getThirdElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }

    @Override
    public final void exportAllTo(final Object @NotNull [] dest, @NotNull final ObjectObjectByteTuple tuple) {
        dest[0] = tuple.getFirstElement();
        dest[1] = tuple.getSecondElement();
        dest[2] = BooleanUtils.byteAsBoolean(tuple.getThirdElement());
    }

    @Override
    public final void exportAllTo(final Object @NotNull [] dest, @NotNull final ObjectObjectByteTuple tuple, final int @NotNull [] map) {
        dest[map[0]] = tuple.getFirstElement();
        dest[map[1]] = tuple.getSecondElement();
        dest[map[2]] = BooleanUtils.byteAsBoolean(tuple.getThirdElement());
    }

    @Override
    public final Object exportElementReinterpreted(@NotNull final ObjectObjectByteTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return tuple.getFirstElement();
        }
        if (elementIndex == 1) {
            return tuple.getSecondElement();
        }
        if (elementIndex == 2) {
            return BooleanUtils.byteAsBoolean(tuple.getThirdElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }
    @Override
    public final void exportAllReinterpretedTo(final Object @NotNull [] dest, @NotNull final ObjectObjectByteTuple tuple) {
        dest[0] = tuple.getFirstElement();
        dest[1] = tuple.getSecondElement();
        dest[2] = BooleanUtils.byteAsBoolean(tuple.getThirdElement());
    }

    @Override
    public final void exportAllReinterpretedTo(final Object @NotNull [] dest, @NotNull final ObjectObjectByteTuple tuple, final int @NotNull [] map) {
        dest[map[0]] = tuple.getFirstElement();
        dest[map[1]] = tuple.getSecondElement();
        dest[map[2]] = BooleanUtils.byteAsBoolean(tuple.getThirdElement());
    }


    @Override
    protected void convertChunks(@NotNull WritableChunk<? super Values> destination, int chunkSize, Chunk<? extends Values> [] chunks) {
        WritableObjectChunk<ObjectObjectByteTuple, ? super Values> destinationObjectChunk = destination.asWritableObjectChunk();
        ObjectChunk<Object, ? extends Values> chunk1 = chunks[0].asObjectChunk();
        ObjectChunk<Object, ? extends Values> chunk2 = chunks[1].asObjectChunk();
        ObjectChunk<Boolean, ? extends Values> chunk3 = chunks[2].asObjectChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new ObjectObjectByteTuple(chunk1.get(ii), chunk2.get(ii), BooleanUtils.booleanAsByte(chunk3.get(ii))));
        }
        destinationObjectChunk.setSize(chunkSize);
    }

    /** {@link ThreeColumnTupleSourceFactory} for instances of {@link ObjectObjectBooleanColumnTupleSource}. **/
    private static final class Factory implements ThreeColumnTupleSourceFactory<ObjectObjectByteTuple, Object, Object, Boolean> {

        private Factory() {
        }

        @Override
        public TupleSource<ObjectObjectByteTuple> create(
                @NotNull final ColumnSource<Object> columnSource1,
                @NotNull final ColumnSource<Object> columnSource2,
                @NotNull final ColumnSource<Boolean> columnSource3
        ) {
            return new ObjectObjectBooleanColumnTupleSource(
                    columnSource1,
                    columnSource2,
                    columnSource3
            );
        }
    }
}
