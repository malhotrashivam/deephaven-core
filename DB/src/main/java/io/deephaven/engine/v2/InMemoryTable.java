/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2;

import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.tables.TableDefinition;
import io.deephaven.engine.v2.sources.ArrayBackedColumnSource;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.v2.utils.RowSetFactoryImpl;
import io.deephaven.engine.v2.utils.TrackingRowSet;
import io.deephaven.qst.column.Column;
import io.deephaven.qst.table.NewTable;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

public class InMemoryTable extends QueryTable {

    /**
     * Defers to {@link ArrayBackedColumnSource#from(io.deephaven.qst.array.Array)} to construct the appropriate
     * {@link ColumnSource column sources} (this involves copying the data).
     *
     * @param table the new table qst
     * @return the in memory table
     */
    public static InMemoryTable from(NewTable table) {
        final Map<String, ColumnSource<?>> columns = new LinkedHashMap<>(table.numColumns());
        for (Column<?> column : table) {
            final ColumnSource<?> source = ArrayBackedColumnSource.from(column.array());
            columns.put(column.name(), source);
        }
        return new InMemoryTable(
                TableDefinition.from(table.header()),
                RowSetFactoryImpl.INSTANCE.flat(table.size()).convertToTracking(),
                columns);
    }

    public InMemoryTable(String[] columnNames, Object[] arrayValues) {
        super(RowSetFactoryImpl.INSTANCE.flat(Array.getLength(arrayValues[0])).convertToTracking(),
                createColumnsMap(columnNames, arrayValues));
    }

    public InMemoryTable(TableDefinition definition, final int size) {
        super(RowSetFactoryImpl.INSTANCE.flat(size).convertToTracking(),
                createColumnsMap(
                        definition.getColumnNames().toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY),
                        Arrays.stream(definition.getColumns()).map(
                                x -> Array.newInstance(x.getDataType(), size)).toArray(Object[]::new)));
    }

    private InMemoryTable(TableDefinition definition, TrackingRowSet rowSet, Map<String, ? extends ColumnSource<?>> columns) {
        super(definition, rowSet, columns);
    }

    private static Map<String, ColumnSource<?>> createColumnsMap(String[] columnNames, Object[] arrayValues) {
        Map<String, ColumnSource<?>> map = new LinkedHashMap<>();
        for (int i = 0; i < columnNames.length; i++) {
            map.put(columnNames[i], ArrayBackedColumnSource.getMemoryColumnSourceUntyped((arrayValues[i])));
        }
        return map;
    }
}
