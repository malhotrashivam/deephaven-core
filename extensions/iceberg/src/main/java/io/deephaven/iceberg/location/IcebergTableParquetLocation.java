//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.location;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.locations.TableKey;
import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.iceberg.util.IcebergTableAdapter;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.parquet.table.location.ParquetTableLocation;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class IcebergTableParquetLocation extends ParquetTableLocation implements TableLocation {

    @Nullable
    private final List<SortColumn> sortedColumns;

    public IcebergTableParquetLocation(
            @NotNull final IcebergTableAdapter tableAdapter,
            @NotNull final TableKey tableKey,
            @NotNull final IcebergTableParquetLocationKey tableLocationKey,
            @NotNull final ParquetInstructions readInstructions) {
        super(tableKey, tableLocationKey, readInstructions);
        sortedColumns = computeSortedColumns(tableAdapter, tableLocationKey.dataFile(), readInstructions);
    }

    @Override
    @NotNull
    public List<SortColumn> getSortedColumns() {
        return sortedColumns == null ? super.getSortedColumns() : sortedColumns;
    }

    @Nullable
    private static List<SortColumn> computeSortedColumns(
            @NotNull final IcebergTableAdapter tableAdapter,
            @NotNull final DataFile dataFile,
            @NotNull final ParquetInstructions readInstructions) {
        final Integer sortOrderId = dataFile.sortOrderId();
        // If sort order is missing or unknown, we cannot determine the sorted columns from the metadata and will
        // check the underlying parquet file for the sorted columns, when the user asks for them.
        if (sortOrderId == null) {
            return null;
        }
        final SortOrder sortOrder = tableAdapter.icebergTable().sortOrders().get(sortOrderId);
        if (sortOrder == null) {
            return null;
        }
        if (sortOrder.isUnsorted()) {
            return Collections.emptyList();
        }
        final Schema schema = sortOrder.schema();
        final List<SortColumn> sortColumns = new ArrayList<>(sortOrder.fields().size());
        for (final SortField field : sortOrder.fields()) {
            if (!field.transform().isIdentity()) {
                // TODO (DH-18160): Improve support for handling non-identity transforms
                break;
            }
            final String icebergColName = schema.findColumnName(field.sourceId());
            final String dhColName = readInstructions.getColumnNameFromParquetColumnNameOrDefault(icebergColName);
            final TableDefinition tableDefinition = readInstructions.getTableDefinition().orElseThrow(
                    () -> new IllegalStateException("Table definition is required for reading from Iceberg tables"));
            final ColumnDefinition<?> columnDef = tableDefinition.getColumn(dhColName);
            if (columnDef == null) {
                // Table definition provided by the user doesn't have this column, so stop here
                break;
            }
            final SortColumn sortColumn;
            if (field.nullOrder() == NullOrder.NULLS_FIRST && field.direction() == SortDirection.ASC) {
                sortColumn = SortColumn.asc(ColumnName.of(dhColName));
            } else if (field.nullOrder() == NullOrder.NULLS_LAST && field.direction() == SortDirection.DESC) {
                sortColumn = SortColumn.desc(ColumnName.of(dhColName));
            } else {
                break;
            }
            sortColumns.add(sortColumn);
        }
        return Collections.unmodifiableList(sortColumns);
    }
}
