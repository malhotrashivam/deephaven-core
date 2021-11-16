package io.deephaven.engine.tuplesource.generated;

import io.deephaven.datastructures.util.SmartKey;
import io.deephaven.engine.chunk.Attributes;
import io.deephaven.engine.chunk.Chunk;
import io.deephaven.engine.chunk.ObjectChunk;
import io.deephaven.engine.chunk.WritableChunk;
import io.deephaven.engine.chunk.WritableObjectChunk;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.time.DateTime;
import io.deephaven.engine.time.DateTimeUtils;
import io.deephaven.engine.tuple.generated.ShortLongTuple;
import io.deephaven.engine.tuplesource.AbstractTupleSource;
import io.deephaven.engine.tuplesource.TupleSource;
import io.deephaven.engine.tuplesource.TwoColumnTupleSourceFactory;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;


/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Short and DateTime.
 * <p>Generated by io.deephaven.replicators.TupleSourceCodeGenerator.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class ShortDateTimeColumnTupleSource extends AbstractTupleSource<ShortLongTuple> {

    /** {@link TwoColumnTupleSourceFactory} instance to create instances of {@link ShortDateTimeColumnTupleSource}. **/
    public static final TwoColumnTupleSourceFactory<ShortLongTuple, Short, DateTime> FACTORY = new Factory();

    private final ColumnSource<Short> columnSource1;
    private final ColumnSource<DateTime> columnSource2;

    public ShortDateTimeColumnTupleSource(
            @NotNull final ColumnSource<Short> columnSource1,
            @NotNull final ColumnSource<DateTime> columnSource2
    ) {
        super(columnSource1, columnSource2);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
    }

    @Override
    public final ShortLongTuple createTuple(final long indexKey) {
        return new ShortLongTuple(
                columnSource1.getShort(indexKey),
                DateTimeUtils.nanos(columnSource2.get(indexKey))
        );
    }

    @Override
    public final ShortLongTuple createPreviousTuple(final long indexKey) {
        return new ShortLongTuple(
                columnSource1.getPrevShort(indexKey),
                DateTimeUtils.nanos(columnSource2.getPrev(indexKey))
        );
    }

    @Override
    public final ShortLongTuple createTupleFromValues(@NotNull final Object... values) {
        return new ShortLongTuple(
                TypeUtils.unbox((Short)values[0]),
                DateTimeUtils.nanos((DateTime)values[1])
        );
    }

    @Override
    public final ShortLongTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new ShortLongTuple(
                TypeUtils.unbox((Short)values[0]),
                DateTimeUtils.nanos((DateTime)values[1])
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final ShortLongTuple tuple, final int elementIndex, @NotNull final WritableColumnSource<ELEMENT_TYPE> writableSource, final long destinationIndexKey) {
        if (elementIndex == 0) {
            writableSource.set(destinationIndexKey, tuple.getFirstElement());
            return;
        }
        if (elementIndex == 1) {
            writableSource.set(destinationIndexKey, (ELEMENT_TYPE) DateTimeUtils.nanosToTime(tuple.getSecondElement()));
            return;
        }
        throw new IndexOutOfBoundsException("Invalid element index " + elementIndex + " for export");
    }

    @Override
    public final Object exportToExternalKey(@NotNull final ShortLongTuple tuple) {
        return new SmartKey(
                TypeUtils.box(tuple.getFirstElement()),
                DateTimeUtils.nanosToTime(tuple.getSecondElement())
        );
    }

    @Override
    public final Object exportElement(@NotNull final ShortLongTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return DateTimeUtils.nanosToTime(tuple.getSecondElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 2 element tuple: " + elementIndex);
    }

    @Override
    public final Object exportElementReinterpreted(@NotNull final ShortLongTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return DateTimeUtils.nanosToTime(tuple.getSecondElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 2 element tuple: " + elementIndex);
    }

    protected void convertChunks(@NotNull WritableChunk<? super Attributes.Values> destination, int chunkSize, Chunk<Attributes.Values> [] chunks) {
        WritableObjectChunk<ShortLongTuple, ? super Attributes.Values> destinationObjectChunk = destination.asWritableObjectChunk();
        ShortChunk<Attributes.Values> chunk1 = chunks[0].asShortChunk();
        ObjectChunk<DateTime, Attributes.Values> chunk2 = chunks[1].asObjectChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new ShortLongTuple(chunk1.get(ii), DateTimeUtils.nanos(chunk2.get(ii))));
        }
        destination.setSize(chunkSize);
    }

    /** {@link TwoColumnTupleSourceFactory} for instances of {@link ShortDateTimeColumnTupleSource}. **/
    private static final class Factory implements TwoColumnTupleSourceFactory<ShortLongTuple, Short, DateTime> {

        private Factory() {
        }

        @Override
        public TupleSource<ShortLongTuple> create(
                @NotNull final ColumnSource<Short> columnSource1,
                @NotNull final ColumnSource<DateTime> columnSource2
        ) {
            return new ShortDateTimeColumnTupleSource(
                    columnSource1,
                    columnSource2
            );
        }
    }
}
