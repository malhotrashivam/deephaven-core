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
import io.deephaven.engine.tuple.generated.LongLongByteTuple;
import io.deephaven.engine.tuplesource.AbstractTupleSource;
import io.deephaven.engine.tuplesource.ThreeColumnTupleSourceFactory;
import io.deephaven.engine.tuplesource.TupleSource;
import io.deephaven.util.BooleanUtils;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;


/**
 * <p>{@link TupleSource} that produces key column values from {@link ColumnSource} types Long, DateTime, and Byte.
 * <p>Generated by io.deephaven.replicators.TupleSourceCodeGenerator.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class ReinterpretedDateTimeDateTimeReinterpretedBooleanColumnTupleSource extends AbstractTupleSource<LongLongByteTuple> {

    /** {@link ThreeColumnTupleSourceFactory} instance to create instances of {@link ReinterpretedDateTimeDateTimeReinterpretedBooleanColumnTupleSource}. **/
    public static final ThreeColumnTupleSourceFactory<LongLongByteTuple, Long, DateTime, Byte> FACTORY = new Factory();

    private final ColumnSource<Long> columnSource1;
    private final ColumnSource<DateTime> columnSource2;
    private final ColumnSource<Byte> columnSource3;

    public ReinterpretedDateTimeDateTimeReinterpretedBooleanColumnTupleSource(
            @NotNull final ColumnSource<Long> columnSource1,
            @NotNull final ColumnSource<DateTime> columnSource2,
            @NotNull final ColumnSource<Byte> columnSource3
    ) {
        super(columnSource1, columnSource2, columnSource3);
        this.columnSource1 = columnSource1;
        this.columnSource2 = columnSource2;
        this.columnSource3 = columnSource3;
    }

    @Override
    public final LongLongByteTuple createTuple(final long indexKey) {
        return new LongLongByteTuple(
                columnSource1.getLong(indexKey),
                DateTimeUtils.nanos(columnSource2.get(indexKey)),
                columnSource3.getByte(indexKey)
        );
    }

    @Override
    public final LongLongByteTuple createPreviousTuple(final long indexKey) {
        return new LongLongByteTuple(
                columnSource1.getPrevLong(indexKey),
                DateTimeUtils.nanos(columnSource2.getPrev(indexKey)),
                columnSource3.getPrevByte(indexKey)
        );
    }

    @Override
    public final LongLongByteTuple createTupleFromValues(@NotNull final Object... values) {
        return new LongLongByteTuple(
                DateTimeUtils.nanos((DateTime)values[0]),
                DateTimeUtils.nanos((DateTime)values[1]),
                BooleanUtils.booleanAsByte((Boolean)values[2])
        );
    }

    @Override
    public final LongLongByteTuple createTupleFromReinterpretedValues(@NotNull final Object... values) {
        return new LongLongByteTuple(
                TypeUtils.unbox((Long)values[0]),
                DateTimeUtils.nanos((DateTime)values[1]),
                TypeUtils.unbox((Byte)values[2])
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <ELEMENT_TYPE> void exportElement(@NotNull final LongLongByteTuple tuple, final int elementIndex, @NotNull final WritableColumnSource<ELEMENT_TYPE> writableSource, final long destinationIndexKey) {
        if (elementIndex == 0) {
            writableSource.set(destinationIndexKey, (ELEMENT_TYPE) DateTimeUtils.nanosToTime(tuple.getFirstElement()));
            return;
        }
        if (elementIndex == 1) {
            writableSource.set(destinationIndexKey, (ELEMENT_TYPE) DateTimeUtils.nanosToTime(tuple.getSecondElement()));
            return;
        }
        if (elementIndex == 2) {
            writableSource.set(destinationIndexKey, (ELEMENT_TYPE) BooleanUtils.byteAsBoolean(tuple.getThirdElement()));
            return;
        }
        throw new IndexOutOfBoundsException("Invalid element index " + elementIndex + " for export");
    }

    @Override
    public final Object exportToExternalKey(@NotNull final LongLongByteTuple tuple) {
        return new SmartKey(
                DateTimeUtils.nanosToTime(tuple.getFirstElement()),
                DateTimeUtils.nanosToTime(tuple.getSecondElement()),
                BooleanUtils.byteAsBoolean(tuple.getThirdElement())
        );
    }

    @Override
    public final Object exportElement(@NotNull final LongLongByteTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return DateTimeUtils.nanosToTime(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return DateTimeUtils.nanosToTime(tuple.getSecondElement());
        }
        if (elementIndex == 2) {
            return BooleanUtils.byteAsBoolean(tuple.getThirdElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }

    @Override
    public final Object exportElementReinterpreted(@NotNull final LongLongByteTuple tuple, int elementIndex) {
        if (elementIndex == 0) {
            return TypeUtils.box(tuple.getFirstElement());
        }
        if (elementIndex == 1) {
            return DateTimeUtils.nanosToTime(tuple.getSecondElement());
        }
        if (elementIndex == 2) {
            return TypeUtils.box(tuple.getThirdElement());
        }
        throw new IllegalArgumentException("Bad elementIndex for 3 element tuple: " + elementIndex);
    }

    @Override
    protected void convertChunks(@NotNull WritableChunk<? super Attributes.Values> destination, int chunkSize, Chunk<Attributes.Values> [] chunks) {
        WritableObjectChunk<LongLongByteTuple, ? super Attributes.Values> destinationObjectChunk = destination.asWritableObjectChunk();
        LongChunk<Attributes.Values> chunk1 = chunks[0].asLongChunk();
        ObjectChunk<DateTime, Attributes.Values> chunk2 = chunks[1].asObjectChunk();
        ByteChunk<Attributes.Values> chunk3 = chunks[2].asByteChunk();
        for (int ii = 0; ii < chunkSize; ++ii) {
            destinationObjectChunk.set(ii, new LongLongByteTuple(chunk1.get(ii), DateTimeUtils.nanos(chunk2.get(ii)), chunk3.get(ii)));
        }
        destinationObjectChunk.setSize(chunkSize);
    }

    /** {@link ThreeColumnTupleSourceFactory} for instances of {@link ReinterpretedDateTimeDateTimeReinterpretedBooleanColumnTupleSource}. **/
    private static final class Factory implements ThreeColumnTupleSourceFactory<LongLongByteTuple, Long, DateTime, Byte> {

        private Factory() {
        }

        @Override
        public TupleSource<LongLongByteTuple> create(
                @NotNull final ColumnSource<Long> columnSource1,
                @NotNull final ColumnSource<DateTime> columnSource2,
                @NotNull final ColumnSource<Byte> columnSource3
        ) {
            return new ReinterpretedDateTimeDateTimeReinterpretedBooleanColumnTupleSource(
                    columnSource1,
                    columnSource2,
                    columnSource3
            );
        }
    }
}
