//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.layout;

import io.deephaven.api.util.NameValidator;
import io.deephaven.csv.CsvTools;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.impl.TableLocationKeyFinder;
import io.deephaven.engine.table.impl.locations.local.LocationTableBuilderDefinition;
import io.deephaven.engine.table.impl.locations.local.URIStreamKeyValuePartitionLayout;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.parquet.table.location.ParquetTableLocationKey;
import io.deephaven.util.channel.SeekableChannelsProvider;
import io.deephaven.util.channel.SeekableChannelsProviderLoader;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static io.deephaven.base.FileUtils.convertToURI;

/**
 * Key-Value partitioned layout for Parquet data.
 * 
 * @implNote
 *           <ul>
 *           <li>Unless table definition is provided, type inference for partitioning column uses
 *           {@link CsvTools#readCsv(java.io.InputStream) CsvTools.readCsv} as a conversion tool, and hence follows the
 *           same rules.</li>
 *           <li>Column names will be legalized via {@link NameValidator#legalizeColumnName(String, Set)
 *           NameValidator.legalizeColumnName}.</li>
 *           </ul>
 */
public class ParquetKeyValuePartitionedLayout
        extends URIStreamKeyValuePartitionLayout<ParquetTableLocationKey>
        implements TableLocationKeyFinder<ParquetTableLocationKey> {

    private final ParquetInstructions readInstructions;

    public ParquetKeyValuePartitionedLayout(
            @NotNull final File tableRootDirectory,
            @NotNull final TableDefinition tableDefinition,
            @NotNull final ParquetInstructions readInstructions) {
        this(convertToURI(tableRootDirectory, true), tableDefinition, readInstructions);
    }

    public ParquetKeyValuePartitionedLayout(
            @NotNull final URI tableRootDirectory,
            @NotNull final TableDefinition tableDefinition,
            @NotNull final ParquetInstructions readInstructions) {
        super(tableRootDirectory,
                ParquetFileHelper::isVisibleParquetURI,
                () -> new LocationTableBuilderDefinition(tableDefinition),
                (uri, partitions) -> new ParquetTableLocationKey(uri, 0, partitions, readInstructions),
                Math.toIntExact(tableDefinition.getColumnStream().filter(ColumnDefinition::isPartitioning).count()));
        this.readInstructions = readInstructions;
    }

    public ParquetKeyValuePartitionedLayout(
            @NotNull final File tableRootDirectory,
            final int maxPartitioningLevels,
            @NotNull final ParquetInstructions readInstructions) {
        this(convertToURI(tableRootDirectory, true), maxPartitioningLevels, readInstructions);
    }

    public ParquetKeyValuePartitionedLayout(
            @NotNull final URI tableRootDirectory,
            final int maxPartitioningLevels,
            @NotNull final ParquetInstructions readInstructions) {
        super(tableRootDirectory,
                ParquetFileHelper::isVisibleParquetURI,
                () -> new LocationTableBuilderCsv(tableRootDirectory),
                (uri, partitions) -> new ParquetTableLocationKey(uri, 0, partitions, readInstructions),
                maxPartitioningLevels);
        this.readInstructions = readInstructions;
    }

    public String toString() {
        return ParquetKeyValuePartitionedLayout.class.getSimpleName() + '[' + tableRootDirectory + ']';
    }

    @Override
    public final void findKeys(@NotNull final Consumer<ParquetTableLocationKey> locationKeyObserver) {
        final SeekableChannelsProvider provider = SeekableChannelsProviderLoader.getInstance().fromServiceLoader(
                tableRootDirectory, readInstructions.getSpecialInstructions());
        try (final Stream<URI> uriStream = provider.walk(tableRootDirectory)) {
            final Stream<URI> filteredStream = uriStream.filter(ParquetFileHelper::isVisibleParquetURI);
            findKeys(filteredStream, locationKeyObserver);
        } catch (final IOException e) {
            throw new TableDataException("Error finding parquet locations under " + tableRootDirectory, e);
        }
    }
}
