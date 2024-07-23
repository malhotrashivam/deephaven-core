//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.PartitionAwareSourceTable;
import io.deephaven.engine.table.impl.SourceTableComponentFactory;
import io.deephaven.engine.table.impl.locations.TableKey;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.iceberg.layout.IcebergRefreshingTableLocationProvider;
import io.deephaven.iceberg.location.IcebergTableLocationKey;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.catalog.TableIdentifier;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Table wrapper for refreshing Iceberg tables.
 */
public class IcebergTable extends PartitionAwareSourceTable {
    /**
     * Location discovery.
     */
    final IcebergRefreshingTableLocationProvider<TableKey, IcebergTableLocationKey> locationProvider;

    /**
     *
     *
     * @param tableDefinition A TableDefinition
     * @param description A human-readable description for this table
     * @param componentFactory A component factory for creating column source managers
     * @param locationProvider A TableLocationProvider, for use in discovering the locations that compose this table
     * @param updateSourceRegistrar Callback for registering live tables for refreshes, null if this table is not live
     */
    IcebergTable(
            @NotNull TableDefinition tableDefinition,
            @NotNull String description,
            @NotNull SourceTableComponentFactory componentFactory,
            @NotNull IcebergRefreshingTableLocationProvider<TableKey, IcebergTableLocationKey> locationProvider,
            @Nullable UpdateSourceRegistrar updateSourceRegistrar) {
        super(tableDefinition, description, componentFactory, locationProvider, updateSourceRegistrar);
        this.locationProvider = locationProvider;
    }

    @SuppressWarnings("unused")
    public void update() {
        locationProvider.update();
    }

    @SuppressWarnings("unused")
    public void update(final long snapshotId) {
        locationProvider.update(snapshotId);
    }

    @SuppressWarnings("unused")
    public void update(final @NotNull Snapshot snapshot) {
        locationProvider.update(snapshot);
    }
}
