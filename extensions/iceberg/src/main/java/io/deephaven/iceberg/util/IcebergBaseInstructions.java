//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.engine.table.TableDefinition;

import java.util.Map;
import java.util.Optional;

public interface IcebergBaseInstructions {

    /**
     * The {@link TableDefinition} to use when reading/writing Iceberg data files.
     */
    Optional<TableDefinition> tableDefinition();

    /**
     * The data instructions to use for reading/writing the Iceberg data files (might be S3Instructions or other cloud
     * provider-specific instructions).
     */
    Optional<Object> dataInstructions();

    /**
     * A {@link Map map} of rename instructions from Iceberg to Deephaven column names to use when reading/writing the
     * Iceberg data files.
     */
    Map<String, String> columnRenames();

    interface Builder {
        @SuppressWarnings("unused")
        Builder tableDefinition(TableDefinition tableDefinition);

        @SuppressWarnings("unused")
        Builder dataInstructions(Object s3Instructions);

        @SuppressWarnings("unused")
        Builder putColumnRenames(String key, String value);

        @SuppressWarnings("unused")
        Builder putAllColumnRenames(Map<String, ? extends String> entries);

        IcebergBaseInstructions build();
    }
}
