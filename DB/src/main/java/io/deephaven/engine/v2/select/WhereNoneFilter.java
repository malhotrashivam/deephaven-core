package io.deephaven.engine.v2.select;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.rowset.MutableRowSet;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;

import java.util.Collections;
import java.util.List;

/**
 * A Select filter that always returns an empty rowSet.
 */
public class WhereNoneFilter extends WhereFilterImpl {

    public static final WhereNoneFilter INSTANCE = new WhereNoneFilter();

    private WhereNoneFilter() {}

    @Override
    public List<String> getColumns() {
        return Collections.emptyList();
    }

    @Override
    public List<String> getColumnArrays() {
        return Collections.emptyList();
    }

    @Override
    public void init(TableDefinition tableDefinition) {}

    @Override
    public MutableRowSet filter(RowSet selection, RowSet fullSet, Table table, boolean usePrev) {
        return RowSetFactory.empty();
    }

    @Override
    public boolean isSimpleFilter() {
        return true;
    }

    @Override
    public void setRecomputeListener(RecomputeListener result) {}

    @Override
    public WhereFilter copy() {
        return INSTANCE;
    }
}
