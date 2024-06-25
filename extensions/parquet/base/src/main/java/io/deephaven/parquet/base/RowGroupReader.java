//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import org.apache.parquet.format.RowGroup;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

/**
 * Provides read access to a parquet Row Group
 */
public interface RowGroupReader {
    /**
     * Returns the accessor to a given Column Chunk
     *
     * @param columnName the name of the column
     * @param path the full column path
     * @param useCodec whether a codec is required to read the column
     * @return the accessor to a given Column Chunk, or null if the column is not present in this Row Group
     */
    @Nullable
    ColumnChunkReader getColumnChunk(@NotNull String columnName, @NotNull List<String> path, @NotNull boolean useCodec);

    long numRows();

    RowGroup getRowGroup();
}
