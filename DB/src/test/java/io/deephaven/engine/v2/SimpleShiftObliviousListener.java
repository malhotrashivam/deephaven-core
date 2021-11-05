package io.deephaven.engine.v2;

import io.deephaven.engine.tables.Table;
import io.deephaven.engine.v2.utils.RowSet;

class SimpleShiftObliviousListener extends ShiftObliviousInstrumentedListenerAdapter {
    protected SimpleShiftObliviousListener(Table source) {
        super(source, false);
        reset();
    }

    public int getCount() {
        return count;
    }

    int count;
    RowSet added, removed, modified;

    void reset() {
        freeResources();
        count = 0;
        added = null;
        removed = null;
        modified = null;
    }

    @Override
    public void onUpdate(RowSet added, RowSet removed, RowSet modified) {
        freeResources();
        // Need to copy to save RowSetShiftDataExpander indices that are destroyed at the end of the LTM cycle.
        this.added = added.copy();
        this.removed = removed.copy();
        this.modified = modified.copy();
        ++count;
    }

    @Override
    public String toString() {
        return "SimpleListener{" +
                "count=" + count +
                ", added=" + added +
                ", removed=" + removed +
                ", modified=" + modified +
                '}';
    }

    public void freeResources() {
        if (added != null) {
            added.close();
        }
        if (removed != null) {
            removed.close();
        }
        if (modified != null) {
            modified.close();
        }
    }
}
