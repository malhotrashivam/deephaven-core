//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.TableKey;
import io.deephaven.engine.table.impl.locations.impl.KnownLocationKeyFinder;
import io.deephaven.engine.table.impl.locations.impl.StandaloneTableKey;
import io.deephaven.engine.table.impl.locations.util.ExecutorTableDataRefreshService;
import io.deephaven.engine.table.impl.locations.util.TableDataRefreshService;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.engine.table.impl.sources.regioned.RegionedTableComponentFactoryImpl;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.iceberg.layout.*;
import io.deephaven.iceberg.location.IcebergTableLocationFactory;
import io.deephaven.iceberg.location.IcebergTableLocationKey;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

public class IcebergCatalogAdapter {
    private final Catalog catalog;
    private final FileIO fileIO;

    /**
     * Construct an IcebergCatalogAdapter from a catalog and file IO.
     */
    IcebergCatalogAdapter(
            @NotNull final Catalog catalog,
            @NotNull final FileIO fileIO) {
        this.catalog = catalog;
        this.fileIO = fileIO;
    }

    /**
     * Create a single {@link TableDefinition} from a given Schema, PartitionSpec, and TableDefinition. Takes into
     * account {@link Map<> column rename instructions}
     *
     * @param schema The schema of the table.
     * @param partitionSpec The partition specification of the table.
     * @param tableDefinition The table definition.
     * @param columnRename The map for renaming columns.
     * @return The generated TableDefinition.
     */
    private static TableDefinition fromSchema(
            @NotNull final Schema schema,
            @NotNull final PartitionSpec partitionSpec,
            @Nullable final TableDefinition tableDefinition,
            @NotNull final Map<String, String> columnRename) {

        final Set<String> columnNames = tableDefinition != null
                ? tableDefinition.getColumnNameSet()
                : null;

        final Set<String> partitionNames =
                partitionSpec.fields().stream()
                        .map(PartitionField::name)
                        .map(colName -> columnRename.getOrDefault(colName, colName))
                        .collect(Collectors.toSet());

        final List<ColumnDefinition<?>> columns = new ArrayList<>();

        for (final Types.NestedField field : schema.columns()) {
            final String name = columnRename.getOrDefault(field.name(), field.name());
            // Skip columns that are not in the provided table definition.
            if (columnNames != null && !columnNames.contains(name)) {
                continue;
            }
            final Type type = field.type();
            final io.deephaven.qst.type.Type<?> qstType = convertPrimitiveType(type);
            final ColumnDefinition<?> column;
            if (partitionNames.contains(name)) {
                column = ColumnDefinition.of(name, qstType).withPartitioning();
            } else {
                column = ColumnDefinition.of(name, qstType);
            }
            columns.add(column);
        }

        return TableDefinition.of(columns);
    }

    /**
     * Convert an Iceberg data type to a Deephaven type.
     *
     * @param icebergType The Iceberg data type to be converted.
     * @return The converted Deephaven type.
     */
    static io.deephaven.qst.type.Type<?> convertPrimitiveType(@NotNull final Type icebergType) {
        final Type.TypeID typeId = icebergType.typeId();
        switch (typeId) {
            case BOOLEAN:
                return io.deephaven.qst.type.Type.booleanType().boxedType();
            case DOUBLE:
                return io.deephaven.qst.type.Type.doubleType();
            case FLOAT:
                return io.deephaven.qst.type.Type.floatType();
            case INTEGER:
                return io.deephaven.qst.type.Type.intType();
            case LONG:
                return io.deephaven.qst.type.Type.longType();
            case STRING:
                return io.deephaven.qst.type.Type.stringType();
            case TIMESTAMP:
                final Types.TimestampType timestampType = (Types.TimestampType) icebergType;
                return timestampType.shouldAdjustToUTC()
                        ? io.deephaven.qst.type.Type.find(Instant.class)
                        : io.deephaven.qst.type.Type.find(LocalDateTime.class);
            case DATE:
                return io.deephaven.qst.type.Type.find(java.time.LocalDate.class);
            case TIME:
                return io.deephaven.qst.type.Type.find(java.time.LocalTime.class);
            case DECIMAL:
                return io.deephaven.qst.type.Type.find(java.math.BigDecimal.class);
            case FIXED: // Fall through
            case BINARY:
                return io.deephaven.qst.type.Type.find(byte[].class);
            case UUID: // Fall through
            case STRUCT: // Fall through
            case LIST: // Fall through
            case MAP: // Fall through
            default:
                throw new TableDataException("Unsupported iceberg column type " + typeId.name());
        }
    }

    /**
     * List all {@link Namespace namespaces} in the catalog. This method is only supported if the catalog implements
     * {@link SupportsNamespaces} for namespace discovery. See {@link SupportsNamespaces#listNamespaces(Namespace)}.
     *
     * @return A list of all namespaces.
     */
    public List<Namespace> listNamespaces() {
        return listNamespaces(Namespace.empty());
    }

    /**
     * List all {@link Namespace namespaces} in a given namespace. This method is only supported if the catalog
     * implements {@link SupportsNamespaces} for namespace discovery. See
     * {@link SupportsNamespaces#listNamespaces(Namespace)}.
     *
     * @param namespace The namespace to list namespaces in.
     * @return A list of all namespaces in the given namespace.
     */
    public List<Namespace> listNamespaces(@NotNull final Namespace namespace) {
        if (catalog instanceof SupportsNamespaces) {
            final SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;
            return nsCatalog.listNamespaces(namespace);
        }
        throw new UnsupportedOperationException(String.format(
                "%s does not implement org.apache.iceberg.catalog.SupportsNamespaces", catalog.getClass().getName()));
    }

    /**
     * List all {@link Namespace namespaces} in the catalog as a Deephaven {@link Table table}. The resulting table will
     * be static and contain the same information as {@link #listNamespaces()}.
     *
     * @return A {@link Table table} of all namespaces.
     */
    public Table listNamespacesAsTable() {
        return listNamespacesAsTable(Namespace.empty());
    }

    /**
     * List all {@link Namespace namespaces} in a given namespace as a Deephaven {@link Table table}. The resulting
     * table will be static and contain the same information as {@link #listNamespaces(Namespace)}.
     *
     * @return A {@link Table table} of all namespaces.
     */
    public Table listNamespacesAsTable(@NotNull final Namespace namespace) {
        final List<Namespace> namespaces = listNamespaces(namespace);
        final long size = namespaces.size();

        // Create and return a table containing the namespaces as strings
        final Map<String, ColumnSource<?>> columnSourceMap = new LinkedHashMap<>();

        // Create the column source(s)
        final String[] namespaceArr = new String[(int) size];
        columnSourceMap.put("Namespace",
                InMemoryColumnSource.getImmutableMemoryColumnSource(namespaceArr, String.class, null));

        final Namespace[] namespaceObjectArr = new Namespace[(int) size];
        columnSourceMap.put("NamespaceObject",
                InMemoryColumnSource.getImmutableMemoryColumnSource(namespaceObjectArr, Namespace.class, null));

        // Populate the column source arrays
        for (int i = 0; i < size; i++) {
            final Namespace ns = namespaces.get(i);
            namespaceArr[i] = ns.toString();
            namespaceObjectArr[i] = ns;
        }

        // Create and return the table
        return new QueryTable(RowSetFactory.flat(size).toTracking(), columnSourceMap);
    }

    /**
     * List all {@link Namespace namespaces} in a given namespace as a Deephaven {@link Table table}. The resulting
     * table will be static and contain the same information as {@link #listNamespaces(Namespace)}.
     *
     * @return A {@link Table table} of all namespaces.
     */
    public Table listNamespacesAsTable(@NotNull final String... namespace) {
        return listNamespacesAsTable(Namespace.of(namespace));
    }

    /**
     * List all Iceberg {@link TableIdentifier tables} in a given namespace.
     *
     * @param namespace The namespace to list tables in.
     * @return A list of all tables in the given namespace.
     */
    public List<TableIdentifier> listTables(@NotNull final Namespace namespace) {
        return catalog.listTables(namespace);
    }

    /**
     * List all Iceberg {@link TableIdentifier tables} in a given namespace as a Deephaven {@link Table table}. The
     * resulting table will be static and contain the same information as {@link #listTables(Namespace)}.
     *
     * @param namespace The namespace from which to gather the tables
     * @return A list of all tables in the given namespace.
     */
    public Table listTablesAsTable(@NotNull final Namespace namespace) {
        final List<TableIdentifier> tableIdentifiers = listTables(namespace);
        final long size = tableIdentifiers.size();

        // Create and return a table containing the namespaces as strings
        final Map<String, ColumnSource<?>> columnSourceMap = new LinkedHashMap<>();

        // Create the column source(s)
        final String[] namespaceArr = new String[(int) size];
        columnSourceMap.put("Namespace",
                InMemoryColumnSource.getImmutableMemoryColumnSource(namespaceArr, String.class, null));

        final String[] tableNameArr = new String[(int) size];
        columnSourceMap.put("TableName",
                InMemoryColumnSource.getImmutableMemoryColumnSource(tableNameArr, String.class, null));

        final TableIdentifier[] tableIdentifierArr = new TableIdentifier[(int) size];
        columnSourceMap.put("TableIdentifierObject",
                InMemoryColumnSource.getImmutableMemoryColumnSource(tableIdentifierArr, TableIdentifier.class, null));

        // Populate the column source arrays
        for (int i = 0; i < size; i++) {
            final TableIdentifier tableIdentifier = tableIdentifiers.get(i);
            namespaceArr[i] = tableIdentifier.namespace().toString();
            tableNameArr[i] = tableIdentifier.name();
            tableIdentifierArr[i] = tableIdentifier;
        }

        // Create and return the table
        return new QueryTable(RowSetFactory.flat(size).toTracking(), columnSourceMap);
    }

    public Table listTablesAsTable(@NotNull final String... namespace) {
        return listTablesAsTable(Namespace.of(namespace));
    }

    /**
     * List all {@link Snapshot snapshots} of a given Iceberg table.
     *
     * @param tableIdentifier The identifier of the table from which to gather snapshots.
     * @return A list of all snapshots of the given table.
     */
    public List<Snapshot> listSnapshots(@NotNull final TableIdentifier tableIdentifier) {
        final List<Snapshot> snapshots = new ArrayList<>();
        catalog.loadTable(tableIdentifier).snapshots().forEach(snapshots::add);
        return snapshots;
    }

    /**
     * List all {@link Snapshot snapshots} of a given Iceberg table as a Deephaven {@link Table table}. The resulting
     * table will be static and contain the same information as {@link #listSnapshots(TableIdentifier)}.
     *
     * @param tableIdentifier The identifier of the table from which to gather snapshots.
     * @return A list of all tables in the given namespace.
     */
    public Table listSnapshotsAsTable(@NotNull final TableIdentifier tableIdentifier) {
        final List<Snapshot> snapshots = listSnapshots(tableIdentifier);
        final long size = snapshots.size();

        // Create and return a table containing the namespaces as strings
        final Map<String, ColumnSource<?>> columnSourceMap = new LinkedHashMap<>();

        // Create the column source(s)
        final long[] idArr = new long[(int) size];
        columnSourceMap.put("Id", InMemoryColumnSource.getImmutableMemoryColumnSource(idArr, long.class, null));

        final long[] timestampArr = new long[(int) size];
        columnSourceMap.put("TimestampMs",
                InMemoryColumnSource.getImmutableMemoryColumnSource(timestampArr, long.class, null));

        final String[] operatorArr = new String[(int) size];
        columnSourceMap.put("Operation",
                InMemoryColumnSource.getImmutableMemoryColumnSource(operatorArr, String.class, null));

        final Map<String, String>[] summaryArr = new Map[(int) size];
        columnSourceMap.put("Summary",
                InMemoryColumnSource.getImmutableMemoryColumnSource(summaryArr, Map.class, null));

        final Snapshot[] snapshotArr = new Snapshot[(int) size];
        columnSourceMap.put("SnapshotObject",
                InMemoryColumnSource.getImmutableMemoryColumnSource(snapshotArr, Snapshot.class, null));

        // Populate the column source(s)
        for (int i = 0; i < size; i++) {
            final Snapshot snapshot = snapshots.get(i);
            idArr[i] = snapshot.snapshotId();
            timestampArr[i] = snapshot.timestampMillis();
            operatorArr[i] = snapshot.operation();
            summaryArr[i] = snapshot.summary();
            snapshotArr[i] = snapshot;
        }

        // Create and return the table
        return new QueryTable(RowSetFactory.flat(size).toTracking(), columnSourceMap);
    }

    /**
     * List all {@link Snapshot snapshots} of a given Iceberg table as a Deephaven {@link Table table}. The resulting
     * table will be static and contain the same information as {@link #listSnapshots(TableIdentifier)}.
     *
     * @param tableIdentifier The identifier of the table from which to gather snapshots.
     * @return A list of all tables in the given namespace.
     */
    public Table listSnapshotsAsTable(@NotNull final String tableIdentifier) {
        return listSnapshotsAsTable(TableIdentifier.parse(tableIdentifier));
    }

    /**
     * Retrieve a specific {@link Snapshot snapshot} of an Iceberg table.
     *
     * @param tableIdentifier The identifier of the table from which to load the snapshot.
     * @param snapshotId The identifier of the snapshot to load.
     *
     * @return An Optional<Snapshot> containing the requested snapshot if it exists.
     */
    Optional<Snapshot> getSnapshot(@NotNull final TableIdentifier tableIdentifier, final long snapshotId) {
        return listSnapshots(tableIdentifier).stream()
                .filter(snapshot -> snapshot.snapshotId() == snapshotId)
                .findFirst();
    }

    /**
     * Get the current {@link Snapshot snapshot} of a given Iceberg table.
     *
     * @param tableIdentifier The identifier of the table.
     * @return The current snapshot of the table.
     */
    public Snapshot getCurrentSnapshot(@NotNull final TableIdentifier tableIdentifier) {
        final List<Snapshot> snapshots = listSnapshots(tableIdentifier);
        return snapshots.get(snapshots.size() - 1);
    }

    /**
     * Read the latest static snapshot of an Iceberg table from the Iceberg catalog.
     *
     * @param tableIdentifier The table identifier to load
     * @param instructions The instructions for customizations while reading
     * @return The loaded table
     */
    @SuppressWarnings("unused")
    public IcebergTable readTable(
            @NotNull final TableIdentifier tableIdentifier,
            @Nullable final IcebergInstructions instructions) {
        return readTableInternal(tableIdentifier, null, instructions);
    }

    /**
     * Read the latest static snapshot of an Iceberg table from the Iceberg catalog.
     *
     * @param tableIdentifier The table identifier to load
     * @param instructions The instructions for customizations while reading
     * @return The loaded table
     */
    @SuppressWarnings("unused")
    public IcebergTable readTable(
            @NotNull final String tableIdentifier,
            @Nullable final IcebergInstructions instructions) {
        return readTable(TableIdentifier.parse(tableIdentifier), instructions);
    }

    /**
     * Read a static snapshot of an Iceberg table from the Iceberg catalog.
     *
     * @param tableIdentifier The table identifier to load
     * @param tableSnapshotId The snapshot id to load
     * @param instructions The instructions for customizations while reading
     * @return The loaded table
     */
    @SuppressWarnings("unused")
    public IcebergTable readTable(
            @NotNull final TableIdentifier tableIdentifier,
            final long tableSnapshotId,
            @Nullable final IcebergInstructions instructions) {

        // Find the snapshot with the given snapshot id
        final Snapshot tableSnapshot =
                getSnapshot(tableIdentifier, tableSnapshotId).orElseThrow(() -> new IllegalArgumentException(
                        "Snapshot with id " + tableSnapshotId + " not found for table " + tableIdentifier));

        return readTableInternal(tableIdentifier, tableSnapshot, instructions);
    }

    /**
     * Read a static snapshot of an Iceberg table from the Iceberg catalog.
     *
     * @param tableIdentifier The table identifier to load
     * @param tableSnapshotId The snapshot id to load
     * @param instructions The instructions for customizations while reading
     * @return The loaded table
     */
    @SuppressWarnings("unused")
    public IcebergTable readTable(
            @NotNull final String tableIdentifier,
            final long tableSnapshotId,
            @Nullable final IcebergInstructions instructions) {
        return readTable(TableIdentifier.parse(tableIdentifier), tableSnapshotId, instructions);
    }

    /**
     * Read a static snapshot of an Iceberg table from the Iceberg catalog.
     *
     * @param tableIdentifier The table identifier to load
     * @param tableSnapshot The {@link Snapshot snapshot} to load
     * @param instructions The instructions for customizations while reading
     * @return The loaded table
     */
    @SuppressWarnings("unused")
    public IcebergTable readTable(
            @NotNull final TableIdentifier tableIdentifier,
            @NotNull final Snapshot tableSnapshot,
            @Nullable final IcebergInstructions instructions) {
        return readTableInternal(tableIdentifier, tableSnapshot, instructions);
    }

    private IcebergTable readTableInternal(
            @NotNull final TableIdentifier tableIdentifier,
            @Nullable final Snapshot tableSnapshot,
            @Nullable final IcebergInstructions instructions) {
        // Load the table from the catalog.
        final org.apache.iceberg.Table table = catalog.loadTable(tableIdentifier);

        // Do we want the latest or a specific snapshot?
        final Snapshot snapshot = tableSnapshot != null ? tableSnapshot : table.currentSnapshot();
        final Schema schema = table.schemas().get(snapshot.schemaId());

        // Load the partitioning schema.
        final org.apache.iceberg.PartitionSpec partitionSpec = table.spec();

        // Get default instructions if none are provided
        final IcebergInstructions userInstructions = instructions == null ? IcebergInstructions.DEFAULT : instructions;

        // Get the user supplied table definition.
        final TableDefinition userTableDef = userInstructions.tableDefinition().orElse(null);

        // Get the table definition from the schema (potentially limited by the user supplied table definition and
        // applying column renames).
        final TableDefinition icebergTableDef =
                fromSchema(schema, partitionSpec, userTableDef, userInstructions.columnRenames());

        // If the user supplied a table definition, make sure it's fully compatible.
        final TableDefinition tableDef;
        if (userTableDef != null) {
            tableDef = icebergTableDef.checkCompatibility(userTableDef);

            // Ensure that the user has not marked non-partitioned columns as partitioned.
            final Set<String> userPartitionColumns = userTableDef.getPartitioningColumns().stream()
                    .map(ColumnDefinition::getName)
                    .collect(Collectors.toSet());
            final Set<String> partitionColumns = tableDef.getPartitioningColumns().stream()
                    .map(ColumnDefinition::getName)
                    .collect(Collectors.toSet());

            // The working partitioning column set must be a super-set of the user-supplied set.
            if (!partitionColumns.containsAll(userPartitionColumns)) {
                final Set<String> invalidColumns = new HashSet<>(userPartitionColumns);
                invalidColumns.removeAll(partitionColumns);

                throw new TableDataException("The following columns are not partitioned in the Iceberg table: " +
                        invalidColumns);
            }
        } else {
            // Use the snapshot schema as the table definition.
            tableDef = icebergTableDef;
        }

        final IcebergBaseLayout keyFinder;

        if (partitionSpec.isUnpartitioned()) {
            // Create the flat layout location key finder
            keyFinder = new IcebergFlatLayout(tableDef, table, snapshot, fileIO, userInstructions);
        } else {
            // Create the partitioning column location key finder
            keyFinder = new IcebergKeyValuePartitionedLayout(tableDef, table, snapshot, fileIO, partitionSpec,
                    userInstructions);
        }

        if (instructions.refreshing() == IcebergInstructions.IcebergRefreshing.STATIC) {
            final IcebergTableLocationProviderBase<TableKey, IcebergTableLocationKey> locationProvider =
                    new IcebergStaticTableLocationProvider<>(
                            StandaloneTableKey.getInstance(),
                            keyFinder,
                            new IcebergTableLocationFactory(),
                            this,
                            tableIdentifier);

            return new IcebergTableStatic(
                    tableDef,
                    tableIdentifier.toString(),
                    RegionedTableComponentFactoryImpl.INSTANCE,
                    locationProvider);
        }

        final UpdateSourceRegistrar updateSourceRegistrar;
        final IcebergRefreshingTableLocationProvider<TableKey, IcebergTableLocationKey> locationProvider;

        if (instructions.refreshing() == IcebergInstructions.IcebergRefreshing.MANUAL_REFRESHING) {
            updateSourceRegistrar = ExecutionContext.getContext().getUpdateGraph();

            locationProvider = new IcebergRefreshingTableLocationProvider<>(
                    StandaloneTableKey.getInstance(),
                    keyFinder,
                    new IcebergTableLocationFactory(),
                    TableDataRefreshService.getSharedRefreshService(),
                    this,
                    tableIdentifier,
                    false);
        } else {
            updateSourceRegistrar = ExecutionContext.getContext().getUpdateGraph();

            final TableDataRefreshService refreshService =
                    new ExecutorTableDataRefreshService("Local", instructions.autoRefreshMs(), 30_000L, 10);

            locationProvider = new IcebergRefreshingTableLocationProvider<>(
                    StandaloneTableKey.getInstance(),
                    keyFinder,
                    new IcebergTableLocationFactory(),
                    refreshService,
                    this,
                    tableIdentifier,
                    true);
        }

        return new IcebergTableRefreshing(
                tableDef,
                tableIdentifier.toString(),
                RegionedTableComponentFactoryImpl.INSTANCE,
                locationProvider,
                updateSourceRegistrar);
    }

    private static KnownLocationKeyFinder<IcebergTableLocationKey> toKnownKeys(
            final IcebergBaseLayout keyFinder) {
        return KnownLocationKeyFinder.copyFrom(keyFinder, Comparator.naturalOrder());
    }

    /**
     * Returns the underlying Iceberg {@link Catalog catalog} used by this adapter.
     */
    @SuppressWarnings("unused")
    public Catalog catalog() {
        return catalog;
    }

    /**
     * Returns the underlying Iceberg {@link FileIO fileIO} used by this adapter.
     */
    @SuppressWarnings("unused")
    public FileIO fileIO() {
        return fileIO;
    }

}
