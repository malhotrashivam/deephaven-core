package io.deephaven.engine.tuplesource.generated;

import io.deephaven.datastructures.util.SmartKey;
import io.deephaven.engine.chunk.Attributes;
import io.deephaven.engine.chunk.Chunk;
import io.deephaven.engine.chunk.ObjectChunk;
import io.deephaven.engine.chunk.WritableChunk;
import io.deephaven.engine.chunk.WritableObjectChunk;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.tuple.generated.CharObjectFloatTuple;
import io.deephaven.engine.tuplesource.AbstractTupleSource;
import io.deephaven.engine.tuplesource.ThreeColumnTupleSourceFactory;
import io.deephaven.engine.tuplesource.TupleSource;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;


/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Char, Object, and Float.
 * <p>Generated by io.deephaven.replicators.TupleSourceCodeGenerator.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class CharacterObjectFloatColumnTupleSource extends AbstractTupleSource<CharObjectFloatTuple> {

    /** {@link ThreeColumnTupleSourceFactory} instance to create instances of {@link CharacterObjectFloatColumnTupleSource}. **/
    public static final ThreeColumnTupleSourceFactory<CharObjectFloatTuple, Char, Object, Float> FACTORY = new Factory();

    private final ColumnSource<Char> columnSource1;
    private final ColumnSource<Object> columnSource2;
    private final ColumnSource<Float> columnSource3;

    public CharacterObjectFloatColumnTupleSource(
            @NotNull final ColumnSource<Char> columnSource1,
            @NotNull final ColumnSource<Object> columnSource2,
            @NotNull final ColumnSource<Float> columnSource3
    ) {
        super(columnSource1, columnSource2, columnSource3);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
        this.columnSource3 = columnSource3;
    }

    @Override
    public final CharObjectFloatTuple createTuple(final long indexKey) {
        return new CharObjectFloatTuple(
                columnSource1.getChar(indexKey),
                columnSource2.get(indexKey),
                columnSource3.getFloat(indexKey)
        );
    }

    @Override
    public final CharObjectFloatTuple createPreviousTuple(final long indexKey) {
        return new CharObjectFloatTuple(
                columnSource1.getPrevChar(indexKey),
                columnSource2.getPrev(indexKey),
                columnSource3.getPrevFloat(indexKey)
        );
    }

    @Override
    public final CharObjectFloatTuple createTupleFromValues(@NotNull final Object... values) {
        return new CharObjectFloatTuple(
                TypeUtils.unbox((Character)values[0]),
                values[1],
                TypeUtils.unbox((Float)values[2])
        );
    }

    @Override
    public final CharObjectFloatTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new CharObjectFloatTuple(
                TypeUtils.unbox((Character)values[0]),
                values[1],
                TypeUtils.unbox((Float)values[2])
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final CharObjectFloatTuple tuple, final int elementIndex, @NotNull final WritableColumnSource<ELEMENT_TYPE> writableSource, final long destinationIndexKey) {
        if (elementIndex == 0) {
            writableSource.set(destinationIndexKey, tuple.getFirstElement());
            return;
        }
        if (elementIndex == 1) {
            writableSource.set(destinationIndexKey, (ELEMENT_TYPE) tuple.getSecondElement());
            return;
        }
        if (elementIndex == 2) {
            writableSource.set(destinationIndexKey, tuple.getThirdElement());
            return;
        }
        throw new IndexOutOfBoundsException("Invalid element index " + elementIndex + " for export");
    }

    @Override
    public final Object exportToExternalKey(@NotNull final CharObjectFloatTuple tuple) {
        return new SmartKey(
                TypeUtils.box(tuple.getFirstElement()),
                tuple.getSecondElement(),
                TypeUtils.box(tuple.getThirdElement())
        );
    }

    @Override
    public final Object exportElement(@NotNull final CharObjectFloatTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return tuple.getSecondElement();
        }
        if (elementIndex == 2) {
            return TypeUtils.box(tuple.getThirdElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }

    @Override
    public final Object exportElementReinterpreted(@NotNull final CharObjectFloatTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return tuple.getSecondElement();
        }
        if (elementIndex == 2) {
            return TypeUtils.box(tuple.getThirdElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }

    @Override
    protected void convertChunks(@NotNull WritableChunk<? super Attributes.Values> destination, int chunkSize, Chunk<Attributes.Values> [] chunks) {
        WritableObjectChunk<CharObjectFloatTuple, ? super Attributes.Values> destinationObjectChunk = destination.asWritableObjectChunk();
        CharChunk<Attributes.Values> chunk1 = chunks[0].asCharChunk();
        ObjectChunk<Object, Attributes.Values> chunk2 = chunks[1].asObjectChunk();
        FloatChunk<Attributes.Values> chunk3 = chunks[2].asFloatChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new CharObjectFloatTuple(chunk1.get(ii), chunk2.get(ii), chunk3.get(ii)));
        }
        destinationObjectChunk.setSize(chunkSize);
    }

    /** {@link ThreeColumnTupleSourceFactory} for instances of {@link CharacterObjectFloatColumnTupleSource}. **/
    private static final class Factory implements ThreeColumnTupleSourceFactory<CharObjectFloatTuple, Char, Object, Float> {

        private Factory() {
        }

        @Override
        public TupleSource<CharObjectFloatTuple> create(
                @NotNull final ColumnSource<Char> columnSource1,
                @NotNull final ColumnSource<Object> columnSource2,
                @NotNull final ColumnSource<Float> columnSource3
        ) {
            return new CharacterObjectFloatColumnTupleSource(
                    columnSource1,
                    columnSource2,
                    columnSource3
            );
        }
    }
}
