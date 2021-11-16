/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.plot.datasets.xy;

import io.deephaven.engine.plot.AxesImpl;
import io.deephaven.engine.plot.TableSnapshotSeries;
import io.deephaven.engine.plot.datasets.ColumnNameConstants;
import io.deephaven.engine.plot.datasets.data.IndexableDataSwappableTable;
import io.deephaven.engine.plot.datasets.data.IndexableNumericDataSwappableTable;
import io.deephaven.engine.plot.errors.PlotInfo;
import io.deephaven.engine.plot.util.ArgumentValidations;
import io.deephaven.engine.plot.util.functions.FigureImplFunction;
import io.deephaven.engine.plot.util.tables.SwappableTable;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.tables.libs.QueryLibrary;
import io.deephaven.engine.tables.select.QueryScope;
import io.deephaven.gui.color.Paint;

import java.util.function.Function;

public class XYDataSeriesSwappableTableArray extends XYDataSeriesArray implements TableSnapshotSeries {
    private static final long serialVersionUID = 1L;

    private final SwappableTable swappableTable;
    private final String x;
    private final String y;
    private Table localTable;

    public XYDataSeriesSwappableTableArray(final AxesImpl axes, final int id, final Comparable name,
            final SwappableTable swappableTable, final String x, final String y) {
        super(axes, id, name, new IndexableNumericDataSwappableTable(swappableTable, x, new PlotInfo(axes, name)),
                new IndexableNumericDataSwappableTable(swappableTable, y, new PlotInfo(axes, name)));

        this.swappableTable = swappableTable;
        this.x = x;
        this.y = y;
    }

    @Override
    public <T extends Paint> AbstractXYDataSeries pointColorByY(Function<Double, T> colors) {
        final String colName = ColumnNameConstants.POINT_COLOR + this.hashCode();
        chart().figure().registerTableMapFunction(swappableTable.getTableMapHandle(),
                constructTableMapFromFunction(colors, Paint.class, y, colName));
        swappableTable.getTableMapHandle().addColumn(colName);
        chart().figure().registerFigureFunction(new FigureImplFunction(figImpl -> {
            ((XYDataSeriesSwappableTableArray) figImpl.getFigure().getCharts().getChart(chart().row(), chart().column())
                    .axes(axes().id()).series(id()))
                            .colorsSetSpecific(
                                    new IndexableDataSwappableTable<>(swappableTable, colName, getPlotInfo()));
            return figImpl;
        }, this));
        return this;
    }

    @Override
    public XYDataSeriesArray copy(AxesImpl axes) {
        return new XYDataSeriesSwappableTableArray(this, axes);
    }

    private XYDataSeriesSwappableTableArray(final XYDataSeriesSwappableTableArray series, final AxesImpl axes) {
        super(series, axes);
        this.swappableTable = series.swappableTable;
        this.x = series.x;
        this.y = series.y;
    }

    private <S, T> Function<Table, Table> constructTableMapFromFunction(final Function<S, T> function,
            final Class resultClass, final String onColumn, final String columnName) {
        ArgumentValidations.assertNotNull(function, "function", getPlotInfo());
        final String queryFunction = columnName + "Function";
        return t -> {
            QueryScope.addParam(queryFunction, function);
            QueryLibrary.importClass(resultClass);
            return t.update(columnName + " = (" + resultClass.getSimpleName() + ") " + queryFunction + ".apply("
                    + onColumn + ")");
        };
    }
}
