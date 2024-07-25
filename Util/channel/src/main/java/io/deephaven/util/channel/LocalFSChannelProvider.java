//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.channel;

import io.deephaven.base.FileUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.stream.Stream;

public class LocalFSChannelProvider implements SeekableChannelsProvider {
    private static final int MAX_READ_BUFFER_SIZE = 1 << 16; // 64 KiB

    @Override
    public SeekableChannelContext makeContext() {
        // No additional context required for local FS
        return SeekableChannelContext.NULL;
    }

    @Override
    public boolean isCompatibleWith(@Nullable final SeekableChannelContext channelContext) {
        // Context is not used, hence always compatible
        return true;
    }

    @Override
    public boolean exists(@NotNull final URI uri) {
        return Files.exists(Path.of(uri));
    }

    @Override
    public SeekableByteChannel getReadChannel(@Nullable final SeekableChannelContext channelContext,
            @NotNull final URI uri) throws IOException {
        // context is unused here
        return FileChannel.open(Path.of(uri), StandardOpenOption.READ);
    }

    @Override
    public InputStream getInputStream(final SeekableByteChannel channel, final int sizeHint) {
        // FileChannel is not buffered, need to buffer
        final int bufferSize = Math.min(sizeHint, MAX_READ_BUFFER_SIZE);
        return new BufferedInputStream(Channels.newInputStreamNoClose(channel), bufferSize);
    }

    @Override
    public SeekableByteChannel getWriteChannel(@NotNull final URI uri, final boolean append) throws IOException {
        final FileChannel result = FileChannel.open(Path.of(uri),
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                append ? StandardOpenOption.APPEND : StandardOpenOption.TRUNCATE_EXISTING);
        if (append) {
            result.position(result.size());
        } else {
            result.position(0);
        }
        return result;
    }

    @Override
    public final Stream<URI> list(@NotNull final URI directory) throws IOException {
        // Assuming that the URI is a file, not a directory. The caller should manage file vs. directory handling in
        // the processor.
        return Files.list(Path.of(directory)).map(path -> FileUtils.convertToURI(path, false));
    }

    @Override
    public final Stream<URI> walk(@NotNull final URI directory) throws IOException {
        // Assuming that the URI is a file, not a directory. The caller should manage file vs. directory handling in
        // the processor.
        return Files.walk(Path.of(directory)).map(path -> FileUtils.convertToURI(path, false));
    }

    @Override
    public void close() {}
}
