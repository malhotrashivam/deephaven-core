//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.layout;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.impl.TableLocationKeyFinder;
import io.deephaven.hash.KeyedIntObjectHashMap;
import io.deephaven.hash.KeyedIntObjectKey;
import io.deephaven.iceberg.location.IcebergTableLocationKey;
import io.deephaven.iceberg.util.IcebergReadInstructions;
import io.deephaven.iceberg.util.IcebergTableAdapter;
import io.deephaven.iceberg.internal.DataInstructionsProviderLoader;
import io.deephaven.util.channel.SeekableChannelsProvider;
import io.deephaven.util.type.TypeUtils;
import org.apache.iceberg.*;
import org.apache.iceberg.data.IdentityPartitionConverters;
import org.apache.iceberg.transforms.PartitionSpecVisitor;
import org.apache.iceberg.types.Types;
import org.jetbrains.annotations.NotNull;

import java.net.URI;
import java.util.*;

/**
 * Iceberg {@link TableLocationKeyFinder location finder} for tables with partitions that will discover data files from
 * a {@link Snapshot}
 */
public final class IcebergKeyValuePartitionedLayout extends IcebergBaseLayout {
    private static class PartitioningColInfo {
        final String dhColName;
        final Class<?> dhColType;
        final PartitionField partitionField;

        private PartitioningColInfo(
                @NotNull final PartitionField partitionField,
                @NotNull final String dhColName,
                @NotNull final Class<?> dhColType) {
            this.partitionField = partitionField;
            this.dhColName = dhColName;
            this.dhColType = dhColType;
        }
    }

    private final Schema schema;

    /**
     * Map from the Iceberg partition field ID to the corresponding column name and type in the Deephaven table. This is
     * used to convert the Iceberg partition data to Deephaven column values for all the partitioning columns.
     */
    private final KeyedIntObjectHashMap<PartitioningColInfo> dhPartitioningColumnsInfo;

    /**
     * @param tableAdapter The {@link IcebergTableAdapter} that will be used to access the table.
     * @param partitionSpec The Iceberg {@link PartitionSpec partition spec} to use for processing the partitions.
     * @param schema The Iceberg {@link Schema schema} for processing the partitions.
     * @param instructions The instructions for customizations while reading.
     * @param dataInstructionsProvider The provider for special instructions, to be used if special instructions not
     *        provided in the {@code instructions}.
     */
    public IcebergKeyValuePartitionedLayout(
            @NotNull final IcebergTableAdapter tableAdapter,
            @NotNull final PartitionSpec partitionSpec,
            @NotNull final Schema schema,
            @NotNull final IcebergReadInstructions instructions,
            @NotNull final DataInstructionsProviderLoader dataInstructionsProvider) {
        super(tableAdapter, instructions, dataInstructionsProvider);

        this.schema = schema;

        // We can assume due to upstream validation that there are no duplicate names (after renaming) that are included
        // in the output definition, so we can ignore duplicates.
        final List<PartitionField> partitionFields = partitionSpec.fields();
        final int numPartitionFields = partitionFields.size();
        dhPartitioningColumnsInfo = new KeyedIntObjectHashMap<>(
                numPartitionFields,
                new KeyedIntObjectKey.BasicStrict<>() {
                    @Override
                    public int getIntKey(@NotNull final IcebergKeyValuePartitionedLayout.PartitioningColInfo colInfo) {
                        return colInfo.partitionField.fieldId();
                    }
                });

        for (final PartitionField partitionField : partitionFields) {
            final String icebergColName = partitionField.name();
            final String dhColName = instructions.columnRenames().getOrDefault(icebergColName, icebergColName);
            final ColumnDefinition<?> columnDef = tableDef.getColumn(dhColName);
            if (columnDef == null) {
                // Table definition to be used doesn't have this column, so skip.
                continue;
            }
            dhPartitioningColumnsInfo.put(partitionField.fieldId(),
                    new PartitioningColInfo(
                            partitionField, dhColName, TypeUtils.getBoxedType(columnDef.getDataType())));
        }
    }

    @Override
    public String toString() {
        return IcebergKeyValuePartitionedLayout.class.getSimpleName() + '[' + tableAdapter + ']';
    }

    @Override
    IcebergTableLocationKey keyFromDataFile(
            @NotNull final ManifestFile manifestFile,
            @NotNull final DataFile dataFile,
            @NotNull final URI fileUri,
            @NotNull final SeekableChannelsProvider channelsProvider) {
        final Map<String, Comparable<?>> partitions = new LinkedHashMap<>();
        final PartitionData partitionData = (PartitionData) dataFile.partition();
        final List<Types.NestedField> partitionDataFields = partitionData.getPartitionType().fields();
        for (int pos = 0; pos < partitionDataFields.size(); ++pos) {
            final Types.NestedField partitionDataField = partitionDataFields.get(pos);
            final int partitionFieldId = partitionDataField.fieldId();
            if (!dhPartitioningColumnsInfo.containsKey(partitionFieldId)) {
                // This partition field is not in the Deephaven table, so skip.
                continue;
            }
            final PartitioningColInfo partitioningColInfo = dhPartitioningColumnsInfo.get(partitionFieldId);
            final String dhColName = partitioningColInfo.dhColName;
            final PartitionField partitionField = partitioningColInfo.partitionField;
            final Object icebergPartitionValue = partitionData.get(pos, partitionDataField.type().typeId().javaClass());
            final Object dhPartitionValue;
            if (partitionField.transform().isIdentity()) {
                dhPartitionValue = IdentityPartitionConverters.convertConstant(
                        partitionDataField.type(), icebergPartitionValue);
            } else {
                dhPartitionValue = PartitionSpecVisitor
                        .visit(schema, partitionField, IcebergPartitionValueDecoder.INSTANCE)
                        .apply(icebergPartitionValue);
            }
            if (!partitioningColInfo.dhColType.isAssignableFrom(dhPartitionValue.getClass())) {
                throw new TableDataException("Partitioning column " + dhColName
                        + " has type " + dhPartitionValue.getClass().getName()
                        + " but expected " + partitioningColInfo.dhColType.getName());
            }
            partitions.put(dhColName, (Comparable<?>) dhPartitionValue);
        }
        if (partitions.size() != dhPartitioningColumnsInfo.size()) {
            // TODO Better exception
            throw new IllegalStateException("Expected " + dhPartitioningColumnsInfo.size()
                    + " partitioning columns but found " + partitions.size());
        }
        return locationKey(manifestFile, dataFile, fileUri, partitions, channelsProvider);
    }
}
