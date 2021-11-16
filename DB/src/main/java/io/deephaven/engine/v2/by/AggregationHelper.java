package io.deephaven.engine.v2.by;

import io.deephaven.base.Pair;
import io.deephaven.base.verify.Assert;
import io.deephaven.datastructures.util.SmartKey;
import io.deephaven.engine.rowset.impl.RowSetFactory;
import io.deephaven.engine.table.*;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.impl.RowSequenceFactory;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.v2.*;
import io.deephaven.engine.v2.select.SelectColumn;
import io.deephaven.engine.v2.sources.*;
import io.deephaven.engine.v2.sources.aggregate.AggregateColumnSource;
import io.deephaven.engine.chunk.Attributes.OrderedRowKeys;
import io.deephaven.engine.chunk.Attributes.Values;
import io.deephaven.engine.chunk.*;
import io.deephaven.engine.tuplesource.SmartKeySource;
import io.deephaven.engine.v2.utils.*;
import io.deephaven.util.annotations.VisibleForTesting;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.stream.Collectors;

import static io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_STRING_ARRAY;

/**
 * Implementation for chunk-oriented aggregation operations, including {@link Table#groupBy} and {@link Table#partitionBy}.
 */
public class AggregationHelper {

    /**
     * Static-use only.
     */
    private AggregationHelper() {}

    public static QueryTable by(@NotNull final QueryTable inputTable,
            @NotNull final SelectColumn... keyColumns) {
        return by(AggregationControl.DEFAULT, inputTable, keyColumns);
    }

    @VisibleForTesting
    public static QueryTable by(@NotNull final AggregationControl aggregationControl,
            @NotNull final QueryTable inputTable,
            @NotNull final SelectColumn... keyColumns) {
        // If we have no key columns aggregate all columns with no hashing
        if (keyColumns.length == 0) {
            return noKeyBy(inputTable);
        }

        // Compute our key column sources
        final Map<String, ColumnSource<?>> existingColumnSourceMap = inputTable.getColumnSourceMap();
        final Set<String> keyColumnUpstreamInputColumnNames = new HashSet<>(keyColumns.length);
        final String[] keyColumnNames;
        final String[] aggregatedColumnNames;
        final ColumnSource<?>[] keyColumnSources;
        {
            final Map<String, ColumnSource<?>> keyColumnSourceMap = new LinkedHashMap<>(keyColumns.length);
            final Map<String, ColumnSource<?>> fullColumnSourceMap = new LinkedHashMap<>(existingColumnSourceMap);
            Arrays.stream(keyColumns).forEachOrdered((final SelectColumn keyColumn) -> {
                keyColumn.initInputs(inputTable.getRowSet(), fullColumnSourceMap);

                // Accumulate our key column inputs
                final Set<String> thisKeyColumnUpstreamInputColumnNames = new HashSet<>();
                thisKeyColumnUpstreamInputColumnNames.addAll(keyColumn.getColumns());
                thisKeyColumnUpstreamInputColumnNames.addAll(keyColumn.getColumnArrays());
                thisKeyColumnUpstreamInputColumnNames.removeAll(keyColumnSourceMap.keySet());
                keyColumnUpstreamInputColumnNames.addAll(thisKeyColumnUpstreamInputColumnNames);

                // Accumulate our column source maps
                final ColumnSource<?> keyColumnSource = keyColumn.getDataView();
                fullColumnSourceMap.put(keyColumn.getName(), keyColumnSource);
                keyColumnSourceMap.put(keyColumn.getName(), keyColumnSource);
            });
            keyColumnNames = keyColumnSourceMap.keySet().toArray(ZERO_LENGTH_STRING_ARRAY);
            aggregatedColumnNames = existingColumnSourceMap.keySet().stream()
                    .filter(columnSource -> !keyColumnSourceMap.containsKey(columnSource)).toArray(String[]::new);
            keyColumnSources = keyColumnSourceMap.values().toArray(ColumnSource.ZERO_LENGTH_COLUMN_SOURCE_ARRAY);
        }

        // If we can use an existing static grouping, convert that to a table
        final Map<Object, RowSet> groupingForAggregation =
                maybeGetGroupingForAggregation(aggregationControl, inputTable, keyColumnSources);
        if (groupingForAggregation != null) {
            // noinspection unchecked
            return staticGroupedBy(existingColumnSourceMap, keyColumnNames[0],
                    (ColumnSource<Object>) keyColumnSources[0], groupingForAggregation);
        }

        // Perform a full hashtable backed aggregation
        if (inputTable.isRefreshing()) {
            return incrementalHashedBy(aggregationControl, inputTable, existingColumnSourceMap, keyColumnNames,
                    aggregatedColumnNames, keyColumnSources, keyColumnUpstreamInputColumnNames);
        }
        return staticHashedBy(aggregationControl, inputTable, existingColumnSourceMap, keyColumnNames,
                aggregatedColumnNames, keyColumnSources);
    }

    @NotNull
    private static QueryTable noKeyBy(@NotNull final QueryTable inputTable) {
        final Mutable<QueryTable> resultHolder = new MutableObject<>();
        final SwapListener swapListener =
                inputTable.createSwapListenerIfRefreshing(SwapListener::new);
        inputTable.initializeWithSnapshot("groupBy()-Snapshot", swapListener,
                (final boolean usePrev, final long beforeClockValue) -> {
                    final ColumnSource<TrackingWritableRowSet> resultIndexColumnSource =
                            new SingleValueObjectColumnSource<>((TrackingWritableRowSet) inputTable.getRowSet());
                    final boolean empty =
                            usePrev ? inputTable.getRowSet().firstRowKeyPrev() == RowSequence.NULL_ROW_KEY
                                    : inputTable.isEmpty();
                    final QueryTable resultTable = new QueryTable(
                            RowSetFactory.flat(empty ? 0 : 1).toTracking(),
                            inputTable.getColumnSourceMap().entrySet().stream().collect(Collectors.toMap(
                                    Map.Entry::getKey,
                                    (final Map.Entry<String, ColumnSource<?>> columnNameToSourceEntry) -> {
                                        final AggregateColumnSource<?, ?> aggregateColumnSource = AggregateColumnSource
                                                .make(columnNameToSourceEntry.getValue(), resultIndexColumnSource);
                                        aggregateColumnSource.startTrackingPrevValues();
                                        return aggregateColumnSource;
                                    },
                                    Assert::neverInvoked,
                                    LinkedHashMap::new)));
                    if (swapListener != null) {
                        final ModifiedColumnSet.Transformer transformer = inputTable.newModifiedColumnSetTransformer(
                                inputTable.getDefinition().getColumnNamesArray(),
                                resultTable.getDefinition().getColumnNames().stream()
                                        .map(resultTable::newModifiedColumnSet).toArray(ModifiedColumnSet[]::new));
                        final TableUpdateListener aggregationUpdateListener =
                                new BaseTable.ListenerImpl("groupBy()", inputTable, resultTable) {
                                    @Override
                                    public void onUpdate(@NotNull final TableUpdate upstream) {
                                        final boolean wasEmpty =
                                                inputTable.getRowSet().firstRowKeyPrev() == RowSequence.NULL_ROW_KEY;
                                        final boolean isEmpty = inputTable.getRowSet().isEmpty();
                                        final RowSet added;
                                        final RowSet removed;
                                        final RowSet modified;
                                        final ModifiedColumnSet modifiedColumnSet;
                                        if (wasEmpty) {
                                            if (isEmpty) {
                                                // empty -> empty: No change to report, we probably shouldn't even be
                                                // notified
                                                return;
                                            }
                                            resultTable.getRowSet().writableCast().insert(0);
                                            added = RowSetFactory.flat(1);
                                            removed = RowSetFactory.empty();
                                            modified = RowSetFactory.empty();
                                            modifiedColumnSet = ModifiedColumnSet.EMPTY;
                                        } else if (isEmpty) {
                                            resultTable.getRowSet().writableCast().remove(0);
                                            added = RowSetFactory.empty();
                                            removed = RowSetFactory.flat(1);
                                            modified = RowSetFactory.empty();
                                            modifiedColumnSet = ModifiedColumnSet.EMPTY;
                                        } else if (upstream.added().isNonempty() || upstream.removed().isNonempty()) {
                                            added = RowSetFactory.empty();
                                            removed = RowSetFactory.empty();
                                            modified = RowSetFactory.flat(1);
                                            modifiedColumnSet = ModifiedColumnSet.ALL;
                                        } else if (upstream.modified().isNonempty()) {
                                            added = RowSetFactory.empty();
                                            removed = RowSetFactory.empty();
                                            modified = RowSetFactory.flat(1);
                                            transformer.clearAndTransform(upstream.modifiedColumnSet(),
                                                    modifiedColumnSet = resultTable.getModifiedColumnSetForUpdates());
                                        } else {
                                            // Only shifts: Nothing to report downstream, our data has not changed
                                            return;
                                        }
                                        final TableUpdate downstream = new TableUpdateImpl(added, removed, modified,
                                                RowSetShiftData.EMPTY, modifiedColumnSet);
                                        resultTable.notifyListeners(downstream);
                                    }
                                };
                        swapListener.setListenerAndResult(aggregationUpdateListener, resultTable);
                        resultTable.addParentReference(swapListener);
                    }
                    resultHolder.setValue(resultTable);
                    return true;
                });


        return resultHolder.getValue();
    }

    @NotNull
    private static <T> QueryTable staticGroupedBy(@NotNull final Map<String, ColumnSource<?>> existingColumnSourceMap,
            @NotNull final String keyColumnName,
            @NotNull final ColumnSource<T> keyColumnSource,
            @NotNull final Map<T, RowSet> groupToIndex) {
        final Pair<ArrayBackedColumnSource<T>, ObjectArraySource<TrackingWritableRowSet>> flatResultColumnSources =
                AbstractColumnSource.groupingToFlatSources(keyColumnSource, groupToIndex);
        final ArrayBackedColumnSource<?> resultKeyColumnSource = flatResultColumnSources.getFirst();
        final ObjectArraySource<TrackingWritableRowSet> resultIndexColumnSource = flatResultColumnSources.getSecond();

        final TrackingWritableRowSet resultRowSet = RowSetFactory.flat(groupToIndex.size()).toTracking();
        final Map<String, ColumnSource<?>> resultColumnSourceMap = new LinkedHashMap<>();
        resultColumnSourceMap.put(keyColumnName, resultKeyColumnSource);
        existingColumnSourceMap.entrySet().stream()
                .filter((final Map.Entry<String, ColumnSource<?>> columnNameToSourceEntry) -> !columnNameToSourceEntry
                        .getKey().equals(keyColumnName))
                .forEachOrdered(
                        (final Map.Entry<String, ColumnSource<?>> columnNameToSourceEntry) -> resultColumnSourceMap
                                .put(columnNameToSourceEntry.getKey(), AggregateColumnSource
                                        .make(columnNameToSourceEntry.getValue(), resultIndexColumnSource)));

        return new QueryTable(resultRowSet, resultColumnSourceMap);
    }

    @NotNull
    private static QueryTable staticHashedBy(@NotNull final AggregationControl aggregationControl,
            @NotNull final QueryTable inputTable,
            @NotNull final Map<String, ColumnSource<?>> existingColumnSourceMap,
            @NotNull final String[] keyColumnNames,
            @NotNull final String[] aggregatedColumnNames,
            @NotNull final ColumnSource<?>[] keyColumnSources) {
        // Reinterpret key column sources as primitives where possible
        final ColumnSource<?>[] maybeReinterpretedKeyColumnSources = maybeReinterpretKeyColumnSources(keyColumnSources);

        // Prepare our state manager
        final StaticChunkedByAggregationStateManager stateManager =
                new StaticChunkedByAggregationStateManager(maybeReinterpretedKeyColumnSources,
                        aggregationControl.initialHashTableSize(inputTable), aggregationControl.getTargetLoadFactor(),
                        aggregationControl.getMaximumLoadFactor());

        // Do the actual aggregation hashing and convert the results
        final IntegerArraySource groupIndexToHashSlot = new IntegerArraySource();
        final int numGroups =
                stateManager.buildTable(inputTable, maybeReinterpretedKeyColumnSources, groupIndexToHashSlot);
        stateManager.convertBuildersToIndexes(groupIndexToHashSlot, numGroups);

        // TODO: Consider selecting the hash inputTable sources, in order to truncate them to size and improve density

        // Compute result rowSet and redirection to hash slots
        final TrackingRowSet resultRowSet = RowSetFactory.flat(numGroups).toTracking();
        final WritableRowRedirection resultIndexToHashSlot =
                new IntColumnSourceWritableRowRedirection(groupIndexToHashSlot);

        // Construct result column sources
        final ColumnSource<?>[] keyHashTableSources = stateManager.getKeyHashTableSources();
        final Map<String, ColumnSource<?>> resultColumnSourceMap = new LinkedHashMap<>();

        // Gather the result key columns
        for (int kci = 0; kci < keyHashTableSources.length; ++kci) {
            ColumnSource<?> resultKeyColumnSource = keyHashTableSources[kci];
            if (keyColumnSources[kci] != maybeReinterpretedKeyColumnSources[kci]) {
                resultKeyColumnSource =
                        ReinterpretUtilities.convertToOriginal(keyColumnSources[kci].getType(), resultKeyColumnSource);
            }
            resultColumnSourceMap.put(keyColumnNames[kci],
                    new RedirectedColumnSource<>(resultIndexToHashSlot, resultKeyColumnSource));
        }

        // Gather the result aggregate columns
        final ColumnSource<TrackingWritableRowSet> resultIndexColumnSource =
                new RedirectedColumnSource<>(resultIndexToHashSlot, stateManager.getIndexHashTableSource());
        Arrays.stream(aggregatedColumnNames)
                .forEachOrdered((final String aggregatedColumnName) -> resultColumnSourceMap.put(aggregatedColumnName,
                        AggregateColumnSource.make(existingColumnSourceMap.get(aggregatedColumnName),
                                resultIndexColumnSource)));

        // Construct the result table
        return new QueryTable(resultRowSet, resultColumnSourceMap);
    }

    @NotNull
    private static QueryTable incrementalHashedBy(@NotNull final AggregationControl aggregationControl,
            @NotNull final QueryTable inputTable,
            @NotNull final Map<String, ColumnSource<?>> existingColumnSourceMap,
            @NotNull final String[] keyColumnNames,
            @NotNull final String[] aggregatedColumnNames,
            @NotNull final ColumnSource<?>[] keyColumnSources,
            @NotNull final Set<String> keyColumnUpstreamInputColumnNames) {
        final Mutable<QueryTable> resultHolder = new MutableObject<>();
        final SwapListener swapListener =
                inputTable.createSwapListenerIfRefreshing(SwapListener::new);
        assert swapListener != null;
        inputTable.initializeWithSnapshot("groupBy(" + String.join(",", keyColumnNames) + "-Snapshot", swapListener,
                (final boolean usePrev, final long beforeClockValue) -> {
                    // Reinterpret key column sources as primitives where possible
                    final ColumnSource<?>[] maybeReinterpretedKeyColumnSources =
                            maybeReinterpretKeyColumnSources(keyColumnSources);

                    // Prepare our state manager
                    final IncrementalChunkedByAggregationStateManager stateManager =
                            new IncrementalChunkedByAggregationStateManager(maybeReinterpretedKeyColumnSources,
                                    aggregationControl.initialHashTableSize(inputTable),
                                    aggregationControl.getTargetLoadFactor(),
                                    aggregationControl.getMaximumLoadFactor());

                    // Prepare our update tracker
                    final IncrementalByAggregationUpdateTracker updateTracker =
                            new IncrementalByAggregationUpdateTracker();

                    // Perform the initial aggregation pass
                    if (usePrev) {
                        stateManager.buildInitialTableFromPrevious(inputTable, maybeReinterpretedKeyColumnSources,
                                updateTracker);
                    } else {
                        stateManager.buildInitialTableFromCurrent(inputTable, maybeReinterpretedKeyColumnSources,
                                updateTracker);

                    }
                    // Compute result rowSet and redirection to hash slots
                    final WritableRowRedirection resultIndexToHashSlot =
                            WritableRowRedirectionLockFree.FACTORY.createRowRedirection(updateTracker.size());
                    final TrackingWritableRowSet resultRowSet = updateTracker.applyAddsAndMakeInitialRowSet(
                            stateManager.getRowSetSource(), stateManager.getOverflowRowSetSource(),
                            resultIndexToHashSlot);

                    // Construct result column sources
                    final Map<String, ColumnSource<?>> resultColumnSourceMap = new LinkedHashMap<>();

                    // Gather the result key columns
                    for (int kci = 0; kci < keyColumnSources.length; ++kci) {
                        resultColumnSourceMap.put(keyColumnNames[kci], keyColumnSources[kci]);
                    }

                    // Gather the result aggregate columns
                    final ColumnSource<TrackingWritableRowSet> resultIndexColumnSource =
                            new RedirectedColumnSource<>(
                                    resultIndexToHashSlot, stateManager.getRowSetHashTableSource());
                    Arrays.stream(aggregatedColumnNames)
                            .forEachOrdered((final String aggregatedColumnName) -> {
                                final AggregateColumnSource<?, ?> aggregatedColumnSource = AggregateColumnSource.make(
                                        existingColumnSourceMap.get(aggregatedColumnName), resultIndexColumnSource);
                                aggregatedColumnSource.startTrackingPrevValues();
                                resultColumnSourceMap.put(aggregatedColumnName, aggregatedColumnSource);
                            });

                    // Construct the result table
                    final QueryTable resultTable = new QueryTable(resultRowSet, resultColumnSourceMap);
                    resultIndexToHashSlot.startTrackingPrevValues();

                    // Categorize modified column sets
                    final ModifiedColumnSet upstreamKeyColumnInputs = inputTable
                            .newModifiedColumnSet(keyColumnUpstreamInputColumnNames.toArray(ZERO_LENGTH_STRING_ARRAY));
                    final ModifiedColumnSet downstreamAllAggregatedColumns =
                            resultTable.newModifiedColumnSet(aggregatedColumnNames);
                    final ModifiedColumnSet.Transformer aggregatedColumnsTransformer =
                            inputTable.newModifiedColumnSetTransformer(
                                    aggregatedColumnNames,
                                    Arrays.stream(aggregatedColumnNames).map(resultTable::newModifiedColumnSet)
                                            .toArray(ModifiedColumnSet[]::new));

                    // Handle updates
                    final TableUpdateListener aggregationUpdateListener = new BaseTable.ListenerImpl(
                            "groupBy(" + String.join(",", keyColumnNames) + ')', inputTable, resultTable) {
                        @Override
                        public void onUpdate(@NotNull final TableUpdate upstream) {
                            if (updateTracker.clear()) {
                                stateManager.clearCookies();
                            }

                            final boolean keyColumnsModified =
                                    upstream.modifiedColumnSet().containsAny(upstreamKeyColumnInputs);

                            if (keyColumnsModified) {
                                try (final RowSet toRemove = upstream.removed().union(upstream.getModifiedPreShift())) {
                                    stateManager.processRemoves(maybeReinterpretedKeyColumnSources, toRemove,
                                            updateTracker);
                                }
                            } else {
                                stateManager.processRemoves(maybeReinterpretedKeyColumnSources, upstream.removed(),
                                        updateTracker);
                            }
                            updateTracker.applyRemovesToStates(stateManager.getRowSetSource(),
                                    stateManager.getOverflowRowSetSource());

                            if (upstream.shifted().nonempty()) {
                                upstream.shifted()
                                        .apply((final long beginRange, final long endRange, final long shiftDelta) -> {
                                            final RowSet shiftedPreviousRowSet;
                                            try (final RowSet previousIndex = inputTable.getRowSet().getPrevRowSet()) {
                                                shiftedPreviousRowSet =
                                                        previousIndex.subSetByKeyRange(beginRange, endRange);
                                            }
                                            try {
                                                if (aggregationControl.shouldProbeShift(shiftedPreviousRowSet.size(),
                                                        resultRowSet.intSize())) {
                                                    stateManager.processShift(maybeReinterpretedKeyColumnSources,
                                                            shiftedPreviousRowSet, updateTracker);
                                                    updateTracker.applyShiftToStates(stateManager.getRowSetSource(),
                                                            stateManager.getOverflowRowSetSource(), beginRange,
                                                            endRange,
                                                            shiftDelta);
                                                } else {
                                                    resultRowSet.forAllRowKeys((final long stateKey) -> {
                                                        final int stateSlot = (int) resultIndexToHashSlot.get(stateKey);
                                                        stateManager.applyShift(stateSlot, beginRange, endRange,
                                                                shiftDelta, updateTracker::processAppliedShift);
                                                    });
                                                }
                                            } finally {
                                                shiftedPreviousRowSet.close();
                                            }
                                        });
                            }

                            if (keyColumnsModified) {
                                try (final RowSet toAdd = upstream.added().union(upstream.modified())) {
                                    stateManager.processAdds(maybeReinterpretedKeyColumnSources, toAdd, updateTracker);
                                }
                            } else {
                                stateManager.processModifies(maybeReinterpretedKeyColumnSources, upstream.modified(),
                                        updateTracker);
                                stateManager.processAdds(maybeReinterpretedKeyColumnSources, upstream.added(),
                                        updateTracker);
                            }
                            updateTracker.applyAddsToStates(stateManager.getRowSetSource(),
                                    stateManager.getOverflowRowSetSource());

                            final TableUpdate downstream = updateTracker.makeUpdateFromStates(
                                    stateManager.getRowSetSource(), stateManager.getOverflowRowSetSource(),
                                    resultRowSet,
                                    resultIndexToHashSlot,
                                    (final boolean someKeyHasAddsOrRemoves, final boolean someKeyHasModifies) -> {
                                        if (someKeyHasAddsOrRemoves) {
                                            return downstreamAllAggregatedColumns;
                                        }
                                        if (someKeyHasModifies) {
                                            aggregatedColumnsTransformer.clearAndTransform(upstream.modifiedColumnSet(),
                                                    resultTable.getModifiedColumnSetForUpdates());
                                            return resultTable.getModifiedColumnSetForUpdates();
                                        }
                                        return ModifiedColumnSet.EMPTY;
                                    });
                            resultTable.notifyListeners(downstream);
                        }
                    };
                    swapListener.setListenerAndResult(aggregationUpdateListener, resultTable);
                    resultTable.addParentReference(swapListener);

                    resultHolder.setValue(resultTable);
                    return true;
                });

        return resultHolder.getValue();
    }

    public static LocalTableMap partitionBy(@NotNull final QueryTable inputTable,
            final boolean dropKeyColumns,
            @NotNull final String... keyColumnNames) {
        return partitionBy(AggregationControl.DEFAULT, inputTable, dropKeyColumns, keyColumnNames);
    }

    @VisibleForTesting
    public static LocalTableMap partitionBy(@NotNull final AggregationControl aggregationControl,
            @NotNull final QueryTable inputTable,
            final boolean dropKeyColumns,
            @NotNull final String... keyColumnNames) {
        // If there are no key columns, return a map with just the input table; there's nothing to be aggregated
        if (keyColumnNames.length == 0) {
            final LocalTableMap noKeyResult = new LocalTableMap(null, inputTable.getDefinition());
            noKeyResult.put(SmartKey.EMPTY, inputTable);
            return noKeyResult;
        }

        final ColumnSource<?>[] keyColumnSources =
                Arrays.stream(keyColumnNames).map(inputTable::getColumnSource).toArray(ColumnSource[]::new);
        final QueryTable subTableSource =
                dropKeyColumns ? (QueryTable) inputTable.dropColumns(keyColumnNames) : inputTable;

        // If we can use an existing static grouping, trivially convert that to a table map
        final Map<Object, RowSet> groupingForAggregation =
                maybeGetGroupingForAggregation(aggregationControl, inputTable, keyColumnSources);
        if (groupingForAggregation != null) {
            final LocalTableMap staticGroupedResult = new LocalTableMap(null, inputTable.getDefinition());
            AbstractColumnSource.forEachResponsiveGroup(groupingForAggregation, inputTable.getRowSet(),
                    (final Object key, final WritableRowSet rowSet) -> staticGroupedResult.put(key,
                            subTableSource.getSubTable(rowSet.toTracking())));
            return staticGroupedResult;
        }

        if (inputTable.isRefreshing()) {
            return incrementalHashedPartitionBy(aggregationControl, inputTable, keyColumnSources, subTableSource);
        }

        return staticHashedPartitionBy(aggregationControl, inputTable, keyColumnSources, subTableSource);
    }

    @NotNull
    private static LocalTableMap staticHashedPartitionBy(@NotNull final AggregationControl aggregationControl,
            @NotNull final QueryTable inputTable,
            @NotNull final ColumnSource<?>[] keyColumnSources,
            @NotNull final QueryTable subTableSource) {
        // Reinterpret key column sources as primitives where possible
        final ColumnSource<?>[] maybeReinterpretedKeyColumnSources = maybeReinterpretKeyColumnSources(keyColumnSources);

        // Prepare our state manager
        final StaticChunkedByAggregationStateManager stateManager =
                new StaticChunkedByAggregationStateManager(maybeReinterpretedKeyColumnSources,
                        aggregationControl.initialHashTableSize(inputTable), aggregationControl.getTargetLoadFactor(),
                        aggregationControl.getMaximumLoadFactor());

        // Do the actual aggregation hashing and convert the results
        final IntegerArraySource groupIndexToHashSlot = new IntegerArraySource();
        final int numGroups =
                stateManager.buildTable(inputTable, maybeReinterpretedKeyColumnSources, groupIndexToHashSlot);
        stateManager.convertBuildersToIndexes(groupIndexToHashSlot, numGroups);

        // Build our table map
        final LocalTableMap staticHashedResult = new LocalTableMap(null, inputTable.getDefinition());

        final TupleSource<?> inputKeyIndexToMapKeySource =
                keyColumnSources.length == 1 ? keyColumnSources[0] : new SmartKeySource(keyColumnSources);
        final ColumnSource<TrackingWritableRowSet> hashSlotToRowSetSource = stateManager.getIndexHashTableSource();
        final int chunkSize = Math.min(numGroups, IncrementalChunkedByAggregationStateManager.CHUNK_SIZE);

        try (final RowSequence groupIndices = RowSetFactory.flat(numGroups);
             final RowSequence.Iterator groupIndicesIterator = groupIndices.getRowSequenceIterator();
             final ChunkSource.GetContext hashSlotGetContext = groupIndexToHashSlot.makeGetContext(chunkSize);
             final WritableObjectChunk<TrackingWritableRowSet, Values> aggregatedIndexes =
                        WritableObjectChunk.makeWritableChunk(chunkSize);
             final WritableLongChunk<OrderedRowKeys> mapKeySourceIndices =
                        WritableLongChunk.makeWritableChunk(chunkSize);
             final ChunkSource.GetContext mapKeyGetContext = inputKeyIndexToMapKeySource.makeGetContext(chunkSize)) {
            while (groupIndicesIterator.hasMore()) {
                final RowSequence groupIndexesForThisChunk =
                        groupIndicesIterator.getNextRowSequenceWithLength(chunkSize);
                final int groupsInThisChunk = groupIndexesForThisChunk.intSize();
                final LongChunk<Values> hashSlots =
                        groupIndexToHashSlot.getChunk(hashSlotGetContext, groupIndexesForThisChunk).asLongChunk();
                for (int gi = 0; gi < groupsInThisChunk; ++gi) {
                    final TrackingWritableRowSet rowSet = hashSlotToRowSetSource.get(hashSlots.get(gi));
                    aggregatedIndexes.set(gi, rowSet);
                    mapKeySourceIndices.set(gi, rowSet.firstRowKey());
                }
                aggregatedIndexes.setSize(groupsInThisChunk);
                mapKeySourceIndices.setSize(groupsInThisChunk);
                final ObjectChunk<?, ? extends Values> mapKeys;
                try (final RowSequence inputKeyIndices =
                        RowSequenceFactory.wrapRowKeysChunkAsRowSequence(mapKeySourceIndices)) {
                    mapKeys = inputKeyIndexToMapKeySource.getChunk(mapKeyGetContext, inputKeyIndices).asObjectChunk();
                }
                for (int gi = 0; gi < groupsInThisChunk; ++gi) {
                    staticHashedResult.put(mapKeys.get(gi), subTableSource.getSubTable(aggregatedIndexes.get(gi)));
                }
            }
        }
        return staticHashedResult;
    }

    @NotNull
    private static LocalTableMap incrementalHashedPartitionBy(@NotNull final AggregationControl aggregationControl,
            @NotNull final QueryTable inputTable,
            @NotNull final ColumnSource<?>[] keyColumnSources,
            @NotNull final QueryTable subTableSource) {
        throw new UnsupportedOperationException("Never developed");
    }

    @Nullable
    private static Map<Object, RowSet> maybeGetGroupingForAggregation(
            @NotNull final AggregationControl aggregationControl,
            @NotNull final QueryTable inputTable,
            @NotNull final ColumnSource<?>[] keyColumnSources) {
        // If we have one grouped key column and the input table is not refreshing use the existing grouping
        if (!aggregationControl.considerGrouping(inputTable, keyColumnSources)) {
            return null;
        }
        // noinspection unchecked
        final ColumnSource<Object> keyColumnSource = (ColumnSource<Object>) keyColumnSources[0];
        if (inputTable.getRowSet().hasGrouping(keyColumnSource)) {
            return inputTable.getRowSet().getGrouping(keyColumnSource);
        }
        return null;
    }

    @NotNull
    private static ColumnSource<?>[] maybeReinterpretKeyColumnSources(
            @NotNull final ColumnSource<?>[] keyColumnSources) {
        // TODO: Support symbol tables in reinterpret and re-boxing
        final ColumnSource<?>[] maybeReinterpretedKeyColumnSources = new ColumnSource[keyColumnSources.length];
        for (int kci = 0; kci < keyColumnSources.length; ++kci) {
            maybeReinterpretedKeyColumnSources[kci] =
                    ReinterpretUtilities.maybeConvertToPrimitive(keyColumnSources[kci]);
        }
        return maybeReinterpretedKeyColumnSources;
    }
}
