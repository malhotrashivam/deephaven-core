//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.util.channel.SeekableChannelsProvider;
import org.apache.iceberg.io.FileIO;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Optional;

public abstract class FileIOAdapterBase implements FileIOAdapter {

    @Override
    public final Optional<SeekableChannelsProvider> createProvider(
            @NotNull final String uriScheme,
            @NotNull final FileIO io,
            @Nullable final Object object) {
        if (!isCompatible(uriScheme, io, object)) {
            return Optional.empty();
        }
        return Optional.ofNullable(createProviderImpl(uriScheme, io, object));
    }

    /**
     * Create a {@link SeekableChannelsProvider} for the given URI scheme, file IO and config object.
     */
    protected abstract SeekableChannelsProvider createProviderImpl(
            @NotNull final String uriScheme,
            @NotNull final FileIO io,
            @Nullable final Object specialInstructions);
}
