//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.annotations.BuildableStyle;
import org.apache.iceberg.Snapshot;
import org.immutables.value.Value;

import java.util.Optional;
import java.util.OptionalLong;

/**
 * Instructions for reading a {@link Table table} containing the {@link TableDefinition definition} corresponding to an
 * Iceberg table.
 */
@Value.Immutable
@BuildableStyle
public abstract class IcebergDefinitionTable {
    /**
     * The instructions for customizations while reading, defaults to {@link IcebergReadInstructions#DEFAULT}.
     */
    @Value.Default
    public IcebergReadInstructions instructions() {
        return IcebergReadInstructions.DEFAULT;
    }

    /**
     * The identifier of the snapshot to load. If both this and {@link #snapshot()} are provided, the
     * {@link Snapshot#snapshotId()} should match this. Otherwise, only one of them should be provided. If neither is
     * provided, the latest snapshot will be loaded.
     */
    public abstract OptionalLong tableSnapshotId();

    /**
     * The snapshot to load. If both this and {@link #tableSnapshotId()} are provided, the {@link Snapshot#snapshotId()}
     * should match the {@link #tableSnapshotId()}. Otherwise, only one of them should be provided. If neither is
     * provided, the latest snapshot will be loaded.
     */
    public abstract Optional<Snapshot> snapshot();

    public static Builder builder() {
        return ImmutableIcebergDefinitionTable.builder();
    }

    public interface Builder {
        Builder instructions(IcebergReadInstructions instructions);

        Builder tableSnapshotId(long tableSnapshotId);

        Builder snapshot(Snapshot snapshot);

        IcebergDefinitionTable build();
    }

    @Value.Check
    final void checkSnapshotId() {
        if (tableSnapshotId().isPresent() && snapshot().isPresent() &&
                tableSnapshotId().getAsLong() != snapshot().get().snapshotId()) {
            throw new IllegalArgumentException("If both tableSnapshotId and snapshot are provided, the snapshotId " +
                    "must match");
        }
    }
}
