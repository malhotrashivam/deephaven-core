/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.impl.BlinkTableTools;
import io.deephaven.engine.table.impl.sources.ring.RingTableTools;

import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.TableUpdateListener;

public class SliceLikeOperation implements QueryTable.Operation<QueryTable> {

    public static SliceLikeOperation slice(final QueryTable parent, final long firstPositionInclusive,
            final long lastPositionExclusive, final String op) {

        if (firstPositionInclusive < 0 && lastPositionExclusive > 0) {
            throw new IllegalArgumentException("Can not slice with a negative first position (" + firstPositionInclusive
                    + ") and positive last position (" + lastPositionExclusive + ")");
        }
        // note: first >= 0 && last < 0 is allowed, otherwise first must be less than last
        if ((firstPositionInclusive < 0 || lastPositionExclusive >= 0)
                && lastPositionExclusive < firstPositionInclusive) {
            throw new IllegalArgumentException("Can not slice with a first position (" + firstPositionInclusive
                    + ") after last position (" + lastPositionExclusive + ")");
        }

        return new SliceLikeOperation(op, op + "(" + firstPositionInclusive + ", " + lastPositionExclusive + ")",
                parent, firstPositionInclusive, lastPositionExclusive, firstPositionInclusive == 0);
    }

    public static SliceLikeOperation headPct(final QueryTable parent, final double percent) {
        return new SliceLikeOperation("headPct", "headPct(" + percent + ")", parent,
                0, 0, true) {
            @Override
            protected long getLastPositionExclusive() {
                // Already verified percent is between [0,1] here
                return (long) Math.ceil(percent * parent.size());
            }
        };
    }

    public static SliceLikeOperation tailPct(final QueryTable parent, final double percent) {
        return new SliceLikeOperation("tailPct", "tailPct(" + percent + ")", parent,
                0, 0, false) {
            @Override
            protected long getFirstPositionInclusive() {
                // Already verified percent is between [0,1] here
                return -(long) Math.ceil(percent * parent.size());
            }
        };
    }

    private final String operation;
    private final String description;
    private final QueryTable parent;
    private final long _firstPositionInclusive; // use the accessor
    private final long _lastPositionExclusive; // use the accessor
    private final boolean isFlat;
    private QueryTable resultTable;

    private SliceLikeOperation(final String operation, final String description, final QueryTable parent,
            final long firstPositionInclusive, final long lastPositionExclusive,
            final boolean mayBeFlat) {
        this.operation = operation;
        this.description = description;
        this.parent = parent;
        this._firstPositionInclusive = firstPositionInclusive;
        this._lastPositionExclusive = lastPositionExclusive;
        this.isFlat = parent.isFlat() && mayBeFlat;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public String getLogPrefix() {
        return operation;
    }

    protected long getFirstPositionInclusive() {
        return _firstPositionInclusive;
    }

    protected long getLastPositionExclusive() {
        return _lastPositionExclusive;
    }

    @Override
    public Result<QueryTable> initialize(boolean usePrev, long beforeClock) {
        if (!parent.isBlink()) {
            final TrackingRowSet resultRowSet;
            final TrackingRowSet parentRowSet = parent.getRowSet();
            try (final WritableRowSet parentPrev = usePrev ? parentRowSet.copyPrev() : null) {
                resultRowSet = computeSliceIndex(usePrev ? parentPrev : parentRowSet).toTracking();
            }
            // result table must be a sub-table so we can pass ModifiedColumnSet to listeners when possible
            resultTable = parent.getSubTable(resultRowSet);
            if (isFlat) {
                resultTable.setFlat();
            }

            if (operation.equals("headPct")) {
                // headPct has a floating tail, so we can only propagate if append-only
                if (parent.isAppendOnly()) {
                    resultTable.setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, true);
                    resultTable.setAttribute(Table.APPEND_ONLY_TABLE_ATTRIBUTE, true);
                }
            } else if (!operation.equals("tailPct") && getFirstPositionInclusive() >= 0) {
                // tailPct has a floating head, so we can't propagate either property
                // otherwise, if the first row is fixed (not negative), then we can propagate add-only/append-only
                if (parent.isAddOnly()) {
                    resultTable.setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, true);
                }
                if (parent.isAppendOnly()) {
                    resultTable.setAttribute(Table.APPEND_ONLY_TABLE_ATTRIBUTE, true);
                }
            }
        } else {
            // Parent is BLINK table here
            if (operation.equals("head")) {
                long size = getLastPositionExclusive();
                resultTable = (QueryTable) BlinkTableTools.blinkToAppendOnly(parent, size);
            } else if (operation.equals("tail")) {
                long size = -1 * getFirstPositionInclusive(); // TODO Verify if -1 is correct
                resultTable = (QueryTable) RingTableTools.of(parent, (int) size); // TODO Do I need to do something for
                                                                                  // this type cast from long to int?
            }
        }

        TableUpdateListener resultListener = null;
        if (parent.isRefreshing()) {
            resultListener = new BaseTable.ListenerImpl(getDescription(), parent, resultTable) {
                @Override
                public void onUpdate(TableUpdate upstream) {
                    SliceLikeOperation.this.onUpdate(upstream);
                }
            };
        }

        return new Result<>(resultTable, resultListener);
    }

    private void onUpdate(final TableUpdate upstream) {
        // For Blink table, we would ensure nothing shifted or modified
        // For head, we would check if the table is full and do nothing in that case. Otherwise, we would send
        // only send additions notification to the downstream, no deletion

        if (!parent.isBlink()) {
            final TrackingWritableRowSet rowSet = resultTable.getRowSet().writableCast();
            final RowSet sliceRowSet = computeSliceIndex(parent.getRowSet());
            final TableUpdateImpl downstream = new TableUpdateImpl();
            downstream.removed = upstream.removed().intersect(rowSet);
            rowSet.remove(downstream.removed());

            downstream.shifted = upstream.shifted().intersect(rowSet);
            downstream.shifted().apply(rowSet);

            // Must calculate in post-shift space what indices were removed by the slice operation.
            final WritableRowSet opRemoved = rowSet.minus(sliceRowSet);
            rowSet.remove(opRemoved);
            downstream.shifted().unapply(opRemoved);
            downstream.removed().writableCast().insert(opRemoved);

            // Must intersect against modified set before adding the new rows to result rowSet.
            downstream.modified = upstream.modified().intersect(rowSet);


            downstream.added = sliceRowSet.minus(rowSet);
            rowSet.insert(downstream.added());

            // propagate an empty MCS if modified is empty
            downstream.modifiedColumnSet = upstream.modifiedColumnSet();
            if (downstream.modified().isEmpty()) {
                downstream.modifiedColumnSet = resultTable.getModifiedColumnSetForUpdates();
                downstream.modifiedColumnSet.clear();
            }
            resultTable.notifyListeners(downstream);
        } else {
            // Parent is a Blink table
            // TODO Add a comment here explaining why this is empty
        }
    }

    private WritableRowSet computeSliceIndex(RowSet useRowSet) {
        final long size = parent.size();
        long startSlice = getFirstPositionInclusive();
        long endSlice = getLastPositionExclusive();

        if (startSlice < 0) {
            if (endSlice == 0) { // special code for tail!
                endSlice = size;
            }
            startSlice = Math.max(0, startSlice + size);
        }
        if (endSlice < 0) {
            endSlice = Math.max(0, endSlice + size);
        }
        // allow [firstPos,-lastPos] by being tolerant of overlap
        endSlice = Math.max(startSlice, endSlice);

        return useRowSet.subSetByPositionRange(startSlice, endSlice);
    }
}
