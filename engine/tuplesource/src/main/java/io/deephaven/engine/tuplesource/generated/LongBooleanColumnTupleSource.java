package io.deephaven.engine.tuplesource.generated;

import io.deephaven.datastructures.util.SmartKey;
import io.deephaven.engine.chunk.Attributes;
import io.deephaven.engine.chunk.Chunk;
import io.deephaven.engine.chunk.ObjectChunk;
import io.deephaven.engine.chunk.WritableChunk;
import io.deephaven.engine.chunk.WritableObjectChunk;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.tuple.generated.LongByteTuple;
import io.deephaven.engine.tuplesource.AbstractTupleSource;
import io.deephaven.engine.tuplesource.TupleSource;
import io.deephaven.engine.tuplesource.TwoColumnTupleSourceFactory;
import io.deephaven.util.BooleanUtils;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;


/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Long and Boolean.
 * <p>Generated by io.deephaven.replicators.TupleSourceCodeGenerator.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class LongBooleanColumnTupleSource extends AbstractTupleSource<LongByteTuple> {

    /** {@link TwoColumnTupleSourceFactory} instance to create instances of {@link LongBooleanColumnTupleSource}. **/
    public static final TwoColumnTupleSourceFactory<LongByteTuple, Long, Boolean> FACTORY = new Factory();

    private final ColumnSource<Long> columnSource1;
    private final ColumnSource<Boolean> columnSource2;

    public LongBooleanColumnTupleSource(
            @NotNull final ColumnSource<Long> columnSource1,
            @NotNull final ColumnSource<Boolean> columnSource2
    ) {
        super(columnSource1, columnSource2);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
    }

    @Override
    public final LongByteTuple createTuple(final long indexKey) {
        return new LongByteTuple(
                columnSource1.getLong(indexKey),
                BooleanUtils.booleanAsByte(columnSource2.getBoolean(indexKey))
        );
    }

    @Override
    public final LongByteTuple createPreviousTuple(final long indexKey) {
        return new LongByteTuple(
                columnSource1.getPrevLong(indexKey),
                BooleanUtils.booleanAsByte(columnSource2.getPrevBoolean(indexKey))
        );
    }

    @Override
    public final LongByteTuple createTupleFromValues(@NotNull final Object... values) {
        return new LongByteTuple(
                TypeUtils.unbox((Long)values[0]),
                BooleanUtils.booleanAsByte((Boolean)values[1])
        );
    }

    @Override
    public final LongByteTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new LongByteTuple(
                TypeUtils.unbox((Long)values[0]),
                BooleanUtils.booleanAsByte((Boolean)values[1])
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final LongByteTuple tuple, final int elementIndex, @NotNull final WritableColumnSource<ELEMENT_TYPE> writableSource, final long destinationIndexKey) {
        if (elementIndex == 0) {
            writableSource.set(destinationIndexKey, tuple.getFirstElement());
            return;
        }
        if (elementIndex == 1) {
            writableSource.set(destinationIndexKey, (ELEMENT_TYPE) BooleanUtils.byteAsBoolean(tuple.getSecondElement()));
            return;
        }
        throw new IndexOutOfBoundsException("Invalid element index " + elementIndex + " for export");
    }

    @Override
    public final Object exportToExternalKey(@NotNull final LongByteTuple tuple) {
        return new SmartKey(
                TypeUtils.box(tuple.getFirstElement()),
                BooleanUtils.byteAsBoolean(tuple.getSecondElement())
        );
    }

    @Override
    public final Object exportElement(@NotNull final LongByteTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return BooleanUtils.byteAsBoolean(tuple.getSecondElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 2 element tuple: " + elementIndex);
    }

    @Override
    public final Object exportElementReinterpreted(@NotNull final LongByteTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return BooleanUtils.byteAsBoolean(tuple.getSecondElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 2 element tuple: " + elementIndex);
    }

    protected void convertChunks(@NotNull WritableChunk<? super Attributes.Values> destination, int chunkSize, Chunk<Attributes.Values> [] chunks) {
        WritableObjectChunk<LongByteTuple, ? super Attributes.Values> destinationObjectChunk = destination.asWritableObjectChunk();
        LongChunk<Attributes.Values> chunk1 = chunks[0].asLongChunk();
        ObjectChunk<Boolean, Attributes.Values> chunk2 = chunks[1].asObjectChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new LongByteTuple(chunk1.get(ii), BooleanUtils.booleanAsByte(chunk2.get(ii))));
        }
        destination.setSize(chunkSize);
    }

    /** {@link TwoColumnTupleSourceFactory} for instances of {@link LongBooleanColumnTupleSource}. **/
    private static final class Factory implements TwoColumnTupleSourceFactory<LongByteTuple, Long, Boolean> {

        private Factory() {
        }

        @Override
        public TupleSource<LongByteTuple> create(
                @NotNull final ColumnSource<Long> columnSource1,
                @NotNull final ColumnSource<Boolean> columnSource2
        ) {
            return new LongBooleanColumnTupleSource(
                    columnSource1,
                    columnSource2
            );
        }
    }
}
