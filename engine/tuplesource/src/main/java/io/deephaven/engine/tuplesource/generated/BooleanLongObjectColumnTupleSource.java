package io.deephaven.engine.tuplesource.generated;

import io.deephaven.datastructures.util.SmartKey;
import io.deephaven.engine.chunk.Attributes;
import io.deephaven.engine.chunk.Chunk;
import io.deephaven.engine.chunk.ObjectChunk;
import io.deephaven.engine.chunk.WritableChunk;
import io.deephaven.engine.chunk.WritableObjectChunk;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.tuple.generated.ByteLongObjectTuple;
import io.deephaven.engine.tuplesource.AbstractTupleSource;
import io.deephaven.engine.tuplesource.ThreeColumnTupleSourceFactory;
import io.deephaven.engine.tuplesource.TupleSource;
import io.deephaven.util.BooleanUtils;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;


/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Boolean, Long, and Object.
 * <p>Generated by io.deephaven.replicators.TupleSourceCodeGenerator.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class BooleanLongObjectColumnTupleSource extends AbstractTupleSource<ByteLongObjectTuple> {

    /** {@link ThreeColumnTupleSourceFactory} instance to create instances of {@link BooleanLongObjectColumnTupleSource}. **/
    public static final ThreeColumnTupleSourceFactory<ByteLongObjectTuple, Boolean, Long, Object> FACTORY = new Factory();

    private final ColumnSource<Boolean> columnSource1;
    private final ColumnSource<Long> columnSource2;
    private final ColumnSource<Object> columnSource3;

    public BooleanLongObjectColumnTupleSource(
            @NotNull final ColumnSource<Boolean> columnSource1,
            @NotNull final ColumnSource<Long> columnSource2,
            @NotNull final ColumnSource<Object> columnSource3
    ) {
        super(columnSource1, columnSource2, columnSource3);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
        this.columnSource3 = columnSource3;
    }

    @Override
    public final ByteLongObjectTuple createTuple(final long indexKey) {
        return new ByteLongObjectTuple(
                BooleanUtils.booleanAsByte(columnSource1.getBoolean(indexKey)),
                columnSource2.getLong(indexKey),
                columnSource3.get(indexKey)
        );
    }

    @Override
    public final ByteLongObjectTuple createPreviousTuple(final long indexKey) {
        return new ByteLongObjectTuple(
                BooleanUtils.booleanAsByte(columnSource1.getPrevBoolean(indexKey)),
                columnSource2.getPrevLong(indexKey),
                columnSource3.getPrev(indexKey)
        );
    }

    @Override
    public final ByteLongObjectTuple createTupleFromValues(@NotNull final Object... values) {
        return new ByteLongObjectTuple(
                BooleanUtils.booleanAsByte((Boolean)values[0]),
                TypeUtils.unbox((Long)values[1]),
                values[2]
        );
    }

    @Override
    public final ByteLongObjectTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new ByteLongObjectTuple(
                BooleanUtils.booleanAsByte((Boolean)values[0]),
                TypeUtils.unbox((Long)values[1]),
                values[2]
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final ByteLongObjectTuple tuple, final int elementIndex, @NotNull final WritableColumnSource<ELEMENT_TYPE> writableSource, final long destinationIndexKey) {
        if (elementIndex == 0) {
            writableSource.set(destinationIndexKey, (ELEMENT_TYPE) BooleanUtils.byteAsBoolean(tuple.getFirstElement()));
            return;
        }
        if (elementIndex == 1) {
            writableSource.set(destinationIndexKey, tuple.getSecondElement());
            return;
        }
        if (elementIndex == 2) {
            writableSource.set(destinationIndexKey, (ELEMENT_TYPE) tuple.getThirdElement());
            return;
        }
        throw new IndexOutOfBoundsException("Invalid element index " + elementIndex + " for export");
    }

    @Override
    public final Object exportToExternalKey(@NotNull final ByteLongObjectTuple tuple) {
        return new SmartKey(
                BooleanUtils.byteAsBoolean(tuple.getFirstElement()),
                TypeUtils.box(tuple.getSecondElement()),
                tuple.getThirdElement()
        );
    }

    @Override
    public final Object exportElement(@NotNull final ByteLongObjectTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return BooleanUtils.byteAsBoolean(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return TypeUtils.box(tuple.getSecondElement());
        }
        if (elementIndex == 2) {
            return tuple.getThirdElement();
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }

    @Override
    public final Object exportElementReinterpreted(@NotNull final ByteLongObjectTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return BooleanUtils.byteAsBoolean(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return TypeUtils.box(tuple.getSecondElement());
        }
        if (elementIndex == 2) {
            return tuple.getThirdElement();
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }

    @Override
    protected void convertChunks(@NotNull WritableChunk<? super Attributes.Values> destination, int chunkSize, Chunk<Attributes.Values> [] chunks) {
        WritableObjectChunk<ByteLongObjectTuple, ? super Attributes.Values> destinationObjectChunk = destination.asWritableObjectChunk();
        ObjectChunk<Boolean, Attributes.Values> chunk1 = chunks[0].asObjectChunk();
        LongChunk<Attributes.Values> chunk2 = chunks[1].asLongChunk();
        ObjectChunk<Object, Attributes.Values> chunk3 = chunks[2].asObjectChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new ByteLongObjectTuple(BooleanUtils.booleanAsByte(chunk1.get(ii)), chunk2.get(ii), chunk3.get(ii)));
        }
        destinationObjectChunk.setSize(chunkSize);
    }

    /** {@link ThreeColumnTupleSourceFactory} for instances of {@link BooleanLongObjectColumnTupleSource}. **/
    private static final class Factory implements ThreeColumnTupleSourceFactory<ByteLongObjectTuple, Boolean, Long, Object> {

        private Factory() {
        }

        @Override
        public TupleSource<ByteLongObjectTuple> create(
                @NotNull final ColumnSource<Boolean> columnSource1,
                @NotNull final ColumnSource<Long> columnSource2,
                @NotNull final ColumnSource<Object> columnSource3
        ) {
            return new BooleanLongObjectColumnTupleSource(
                    columnSource1,
                    columnSource2,
                    columnSource3
            );
        }
    }
}
