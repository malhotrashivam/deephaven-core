//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.base.verify.Assert;
import io.deephaven.iceberg.internal.Inference;
import io.deephaven.iceberg.internal.NameMappingUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Objects;
import java.util.Optional;

import static io.deephaven.iceberg.internal.PartitionSpecHelper.partitionForInference;


final class ResolverAndSnapshot {

    static ResolverAndSnapshot create(
            @NotNull final Table table,
            @Nullable final Resolver explicitResolver,
            @Nullable final Snapshot explicitSnapshot,
            final boolean withPartitionInference,
            final boolean withNameMapping) {
        if (explicitResolver != null && explicitSnapshot != null) {
            return new ResolverAndSnapshot(explicitResolver, explicitSnapshot);
        }
        final Resolver resolver;
        final Snapshot snapshot;
        if (explicitResolver == null) {
            final Schema schema;
            if (explicitSnapshot == null) {
                schema = table.schema();
                snapshot = table.currentSnapshot();
            } else {
                schema = table.schemas().get(explicitSnapshot.schemaId());
                snapshot = explicitSnapshot;
            }
            resolver = infer(table, schema, snapshot, withPartitionInference, withNameMapping);
        } else {
            Assert.eqNull(explicitSnapshot, "explicitSnapshot");
            resolver = explicitResolver;
            snapshot = table.currentSnapshot();
        }
        return new ResolverAndSnapshot(resolver, snapshot);
    }

    static Resolver infer(
            @NotNull final Table table,
            @NotNull final Schema schema,
            @Nullable final Snapshot snapshot,
            final boolean withPartitionInference,
            final boolean withNameMapping) {
        final InferenceInstructions instructions =
                inferenceInstructions(table, schema, snapshot, withPartitionInference);
        final Resolver.Builder builder;
        try {
            builder = Resolver.inferBuilder(instructions);
        } catch (Inference.UnsupportedType e) {
            throw new RuntimeException(e);
        }
        if (withNameMapping) {
            NameMappingUtil.readNameMappingDefault(table).ifPresent(builder::nameMapping);
        }
        return builder.build();
    }

    private static InferenceInstructions inferenceInstructions(
            @NotNull final Table table,
            @NotNull final Schema schema,
            @Nullable final Snapshot snapshot,
            final boolean withPartitionInference) {
        if (!withPartitionInference) {
            return InferenceInstructions.of(schema);
        } else {
            final PartitionSpec partitionSpec = partitionForInference(table, snapshot);
            if (partitionSpec.isUnpartitioned()) {
                return InferenceInstructions.of(schema);
            } else {
                return InferenceInstructions.builder()
                        .schema(schema)
                        .spec(partitionSpec)
                        .build();
            }
        }
    }

    private final Resolver resolver;
    private final Snapshot snapshot;

    private ResolverAndSnapshot(final Resolver di, final Snapshot snapshot) {
        this.resolver = Objects.requireNonNull(di);
        this.snapshot = snapshot;
    }

    public Resolver resolver() {
        return resolver;
    }

    public Optional<Snapshot> snapshot() {
        return Optional.ofNullable(snapshot);
    }
}
