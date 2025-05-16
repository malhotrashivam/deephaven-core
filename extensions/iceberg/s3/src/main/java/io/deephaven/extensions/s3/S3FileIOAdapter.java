//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import com.google.auto.service.AutoService;
import io.deephaven.iceberg.util.FileIOAdapter;
import io.deephaven.iceberg.util.FileIOAdapterBase;
import io.deephaven.util.channel.SeekableChannelsProvider;
import io.deephaven.util.channel.SeekableChannelsProviderLoader;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.io.FileIO;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link FileIOAdapter} implementation used for reading/writing files to S3.
 */
@AutoService(FileIOAdapter.class)
public final class S3FileIOAdapter extends FileIOAdapterBase {

    @Override
    public boolean isCompatible(
            @NotNull final String uriScheme,
            @NotNull final FileIO io,
            @Nullable final Object specialInstructions) {
        final boolean compatibleScheme = S3Constants.S3_URI_SCHEME.equals(uriScheme)
                || S3Constants.S3A_URI_SCHEME.equals(uriScheme)
                || S3Constants.S3N_URI_SCHEME.equals(uriScheme);
        final boolean compatibleInstructions = specialInstructions == null
                || specialInstructions instanceof S3Instructions;
        final boolean compatibleIO = io instanceof S3FileIO;
        return compatibleScheme && compatibleInstructions && compatibleIO;
    }

    @Override
    protected SeekableChannelsProvider createProviderImpl(
            @NotNull final String uriScheme,
            @NotNull final FileIO io,
            @Nullable final Object specialInstructions) {
        if (!isCompatible(uriScheme, io, specialInstructions)) {
            throw new IllegalArgumentException("Arguments not compatible, provided uri scheme " + uriScheme +
                    ", io " + io.getClass().getName() + ", special instructions " + specialInstructions);
        }
        final S3FileIO s3FileIO = (S3FileIO) io;
        final S3Instructions s3Instructions =
                specialInstructions == null ? S3Instructions.DEFAULT : (S3Instructions) specialInstructions;

        // Use the S3 clients from the S3FileIO
        final S3Instructions useInstructions =
                s3Instructions.withS3AsyncClient(s3FileIO.asyncClient()).withS3Client(s3FileIO.client());
        return SeekableChannelsProviderLoader.getInstance().load(uriScheme, useInstructions);
    }
}
