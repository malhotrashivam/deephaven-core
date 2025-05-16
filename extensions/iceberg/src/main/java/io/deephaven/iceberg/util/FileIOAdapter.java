//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.util.channel.SeekableChannelsProvider;
import org.apache.iceberg.io.FileIO;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Optional;
import java.util.ServiceLoader;

/**
 * A plugin interface for providing {@link SeekableChannelsProvider} implementations for different URI schemes using a
 * given {@link FileIO} implementation.
 */
public interface FileIOAdapter {

    /**
     * Create a new {@link SeekableChannelsProvider} compatible for reading from and writing to the given URI scheme
     * using the given {@link FileIO} implementation. For example, for an "s3" URI, we will create a
     * {@link SeekableChannelsProvider} which can read files from S3.
     *
     * @param uriScheme The URI scheme
     * @param io The {@link FileIO} implementation to use.
     * @param specialInstructions An optional object to pass special instructions to the provider.
     */
    static SeekableChannelsProvider fromServiceLoader(
            @NotNull final String uriScheme,
            @NotNull final FileIO io,
            @Nullable final Object specialInstructions) {
        for (final FileIOAdapter adapter : ServiceLoader.load(FileIOAdapter.class)) {
            if (adapter.isCompatible(uriScheme, io, specialInstructions)) {
                final SeekableChannelsProvider provider =
                        adapter.createProvider(uriScheme, io, specialInstructions).orElse(null);
                if (provider != null) {
                    return provider;
                }
            }
        }
        throw new UnsupportedOperationException("No provider found for FileIO " + io.getClass());
    }

    /**
     * Check if this adapter is compatible with the given URI scheme, file IO and config object.
     */
    boolean isCompatible(@NotNull String uriScheme, @NotNull final FileIO io, @Nullable Object config);

    /**
     * Create a {@link SeekableChannelsProvider} for the given URI scheme, file IO and config object.
     */
    Optional<SeekableChannelsProvider> createProvider(
            @NotNull String uriScheme,
            @NotNull FileIO io,
            @Nullable Object specialInstructions);
}
