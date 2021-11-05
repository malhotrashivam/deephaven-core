package io.deephaven.engine.v2.snapshot;

import io.deephaven.engine.tables.Table;
import io.deephaven.engine.tables.live.NotificationQueue;
import io.deephaven.engine.v2.BaseTable;
import io.deephaven.engine.v2.LazySnapshotTable;
import io.deephaven.engine.v2.QueryTable;
import io.deephaven.engine.v2.sources.ArrayBackedColumnSource;
import io.deephaven.engine.v2.sources.SingleValueColumnSource;
import io.deephaven.engine.v2.utils.RowSet;
import io.deephaven.engine.v2.utils.RowSetFactoryImpl;
import io.deephaven.engine.v2.utils.TrackingMutableRowSet;
import io.deephaven.engine.v2.utils.TrackingRowSet;

import java.util.Map;

public class SnapshotInternalListener extends BaseTable.ListenerImpl {
    private final QueryTable triggerTable;
    private final boolean lazySnapshot;
    private final Table snapshotTable;
    private long snapshotPrevLength;
    private final QueryTable result;
    private final Map<String, SingleValueColumnSource<?>> resultLeftColumns;
    private final Map<String, ArrayBackedColumnSource<?>> resultRightColumns;
    private final TrackingMutableRowSet resultRowSet;

    public SnapshotInternalListener(QueryTable triggerTable,
            boolean lazySnapshot,
            Table snapshotTable,
            QueryTable result,
            Map<String, SingleValueColumnSource<?>> resultLeftColumns,
            Map<String, ArrayBackedColumnSource<?>> resultRightColumns,
            TrackingMutableRowSet resultRowSet) {
        super("snapshot " + result.getColumnSourceMap().keySet().toString(), triggerTable, result);
        this.triggerTable = triggerTable;
        this.result = result;
        this.lazySnapshot = lazySnapshot;
        this.snapshotTable = snapshotTable;
        this.snapshotPrevLength = 0;
        this.resultLeftColumns = resultLeftColumns;
        this.resultRightColumns = resultRightColumns;
        this.resultRowSet = resultRowSet;
        manage(snapshotTable);
    }

    @Override
    public void onUpdate(final Update upstream) {
        doSnapshot(true, false);
    }

    public void doSnapshot(final boolean notifyListeners, final boolean usePrev) {
        if (lazySnapshot) {
            ((LazySnapshotTable) snapshotTable).refreshForSnapshot();
        }

        // Populate stamp columns from the triggering table
        if (!triggerTable.getRowSet().isEmpty()) {
            SnapshotUtils.copyStampColumns(triggerTable.getColumnSourceMap(), triggerTable.getRowSet().lastRowKey(),
                    resultLeftColumns, 0);
        }
        final TrackingRowSet currentRowSet = snapshotTable.getRowSet();
        final long snapshotSize;
        try (final RowSet prevRowSet = usePrev ? currentRowSet.getPrevRowSet() : null) {
            final RowSet snapshotRowSet = prevRowSet != null ? prevRowSet : currentRowSet;
            snapshotSize = snapshotRowSet.size();
            if (!snapshotRowSet.isEmpty()) {
                try (final RowSet destRowSet = RowSetFactoryImpl.INSTANCE.fromRange(0, snapshotRowSet.size() - 1)) {
                    SnapshotUtils.copyDataColumns(snapshotTable.getColumnSourceMap(),
                            snapshotRowSet, resultRightColumns, destRowSet, usePrev);
                }
            }
        }
        if (snapshotPrevLength < snapshotSize) {
            // If the table got larger then:
            // - added is (the suffix)
            // - modified is (the old rowSet)
            // resultRowSet updated (by including added) for next time
            final RowSet modifiedRange = resultRowSet.clone();
            final RowSet addedRange = RowSetFactoryImpl.INSTANCE.fromRange(snapshotPrevLength, snapshotSize - 1);
            resultRowSet.insert(addedRange);
            if (notifyListeners) {
                result.notifyListeners(addedRange, RowSetFactoryImpl.INSTANCE.empty(), modifiedRange);
            }
        } else if (snapshotPrevLength > snapshotSize) {
            // If the table got smaller, then:
            // - removed is (the suffix)
            // - resultRowSet updated (by removing 'removed') for next time
            // modified is (just use the new rowSet)
            final RowSet removedRange = RowSetFactoryImpl.INSTANCE.fromRange(snapshotSize, snapshotPrevLength - 1);
            resultRowSet.remove(removedRange);
            if (notifyListeners) {
                result.notifyListeners(RowSetFactoryImpl.INSTANCE.empty(), removedRange, resultRowSet);
            }
        } else if (notifyListeners) {
            // If the table stayed the same size, then modified = the rowSet
            result.notifyListeners(RowSetFactoryImpl.INSTANCE.empty(), RowSetFactoryImpl.INSTANCE.empty(), resultRowSet.clone());
        }
        snapshotPrevLength = snapshotTable.size();
    }

    @Override
    public boolean canExecute(final long step) {
        if (!lazySnapshot && snapshotTable instanceof NotificationQueue.Dependency) {
            return ((NotificationQueue.Dependency) snapshotTable).satisfied(step) && super.canExecute(step);
        }
        return super.canExecute(step);
    }
}
