package io.deephaven.engine.v2.by;

import io.deephaven.engine.tables.Table;
import io.deephaven.engine.tables.select.MatchPair;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.v2.sources.LongArraySource;
import io.deephaven.engine.v2.sources.RedirectedColumnSource;
import io.deephaven.engine.v2.sources.chunk.*;
import io.deephaven.engine.v2.sources.chunk.Attributes.ChunkLengths;
import io.deephaven.engine.v2.sources.chunk.Attributes.ChunkPositions;
import io.deephaven.engine.v2.sources.chunk.Attributes.RowKeys;
import io.deephaven.engine.v2.sources.chunk.Attributes.Values;
import io.deephaven.engine.v2.utils.LongColumnSourceMutableRowRedirection;

import java.util.LinkedHashMap;
import java.util.Map;

abstract class BaseAddOnlyFirstOrLastChunkedOperator implements IterativeChunkedAggregationOperator {
    final boolean isFirst;
    final LongArraySource redirections;
    private final LongColumnSourceMutableRowRedirection rowRedirection;
    private final Map<String, ColumnSource<?>> resultColumns;

    BaseAddOnlyFirstOrLastChunkedOperator(boolean isFirst, MatchPair[] resultPairs, Table originalTable,
            String exposeRedirectionAs) {
        this.isFirst = isFirst;
        this.redirections = new LongArraySource();
        this.rowRedirection = new LongColumnSourceMutableRowRedirection(redirections);

        this.resultColumns = new LinkedHashMap<>(resultPairs.length);
        for (final MatchPair mp : resultPairs) {
            // noinspection unchecked
            resultColumns.put(mp.left(),
                    new RedirectedColumnSource(rowRedirection, originalTable.getColumnSource(mp.right())));
        }
        if (exposeRedirectionAs != null) {
            resultColumns.put(exposeRedirectionAs, redirections);
        }
    }

    @Override
    public void removeChunk(BucketedContext bucketedContext, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputIndices, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void modifyChunk(BucketedContext bucketedContext, Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues, LongChunk<? extends RowKeys> postShiftIndices,
            IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        // we have no inputs, so should never get here
        throw new IllegalStateException();
    }

    @Override
    public void shiftChunk(BucketedContext bucketedContext, Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues, LongChunk<? extends Attributes.RowKeys> preShiftIndices,
            LongChunk<? extends RowKeys> postShiftIndices, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void modifyIndices(BucketedContext context, LongChunk<? extends RowKeys> inputIndices,
            IntChunk<Attributes.RowKeys> destinations, IntChunk<ChunkPositions> startPositions,
            IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends Attributes.RowKeys> inputIndices, long destination) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean modifyChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues, LongChunk<? extends RowKeys> postShiftIndices, long destination) {
        // we have no inputs, so should never get here
        throw new IllegalStateException();
    }

    @Override
    public boolean shiftChunk(SingletonContext singletonContext, Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues, LongChunk<? extends RowKeys> preInputIndices,
            LongChunk<? extends RowKeys> postInputIndices, long destination) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean modifyIndices(SingletonContext context, LongChunk<? extends RowKeys> indices, long destination) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void ensureCapacity(long tableSize) {
        redirections.ensureCapacity(tableSize);
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        return resultColumns;
    }

    @Override
    public void startTrackingPrevValues() {
        rowRedirection.startTrackingPrevValues();
    }

    @Override
    public boolean requiresIndices() {
        return true;
    }
}
