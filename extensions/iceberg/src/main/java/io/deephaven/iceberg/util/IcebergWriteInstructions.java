//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import org.immutables.value.Value.Default;

import java.util.Optional;

/**
 * This class provides instructions intended for writing Iceberg tables. The default values documented in this class may
 * change in the future. As such, callers may wish to explicitly set the values.
 */
public abstract class IcebergWriteInstructions implements IcebergBaseInstructions {
    /**
     * While writing to an iceberg table, whether to create the iceberg table if it does not exist, defaults to
     * {@code false}.
     */
    @Default
    public boolean createTableIfNotExist() {
        return false;
    }

    // @formatter:off
    /**
     * Specifies whether to verify that the partition spec and schema of the table being written are consistent with the
     * Iceberg table.
     *
     * <p>Verification behavior differs based on the operation type:</p>
     * <ul>
     *   <li><strong>Appending Data or Writing Data Files:</strong> Verification is enabled by default. It ensures that:
     *     <ul>
     *       <li>All columns from the Deephaven table are present in the Iceberg table and have compatible types.</li>
     *       <li>All required columns in the Iceberg table are present in the Deephaven table.</li>
     *       <li>The set of partitioning columns in both the Iceberg and Deephaven tables are identical.</li>
     *     </ul>
     *   </li>
     *   <li><strong>Overwriting Data:</strong> Verification is disabled by default. When enabled, it ensures that the
     *   schema and partition spec of the table being written are identical to those of the Iceberg table.</li>
     * </ul>
     */
    public abstract Optional<Boolean> verifySchema();
    // @formatter:on

    public interface Builder<INSTRUCTIONS_BUILDER> extends IcebergBaseInstructions.Builder<INSTRUCTIONS_BUILDER> {
        @SuppressWarnings("unused")
        INSTRUCTIONS_BUILDER createTableIfNotExist(boolean createTableIfNotExist);

        @SuppressWarnings("unused")
        INSTRUCTIONS_BUILDER verifySchema(boolean verifySchema);
    }
}
