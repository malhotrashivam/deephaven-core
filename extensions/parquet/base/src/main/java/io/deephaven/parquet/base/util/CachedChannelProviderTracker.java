package io.deephaven.parquet.base.util;

import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.io.logger.Logger;
import io.deephaven.io.sched.Scheduler;
import io.deephaven.io.sched.TimedJob;
import io.deephaven.net.CommBase;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.Iterator;

/**
 * Singleton class for tracking weak references of {@link CachedChannelProvider}, with ability to lookup providers based
 * on file path. This is useful to invalidate channels and file handles in case the underlying file has been modified.
 */
public class CachedChannelProviderTracker { // TODO Think of a better name
    private static volatile CachedChannelProviderTracker instance;

    private static final long CLEANUP_INTERVAL_MILLIS = 60_000;

    private final Scheduler scheduler;

    public static CachedChannelProviderTracker getInstance() {
        if (instance == null) {
            synchronized (CachedChannelProviderTracker.class) {
                if (instance == null) {
                    instance = new CachedChannelProviderTracker();
                }
            }
        }
        return instance;
    }

    /**
     * Mapping from canonical file path to weak references of cached channel providers
     */
    private static class FileToProviderMapEntry {
        public final String fileCanonicalPath;
        public final List<WeakReference<CachedChannelProvider>> providerList;

        public FileToProviderMapEntry(final String path, List<WeakReference<CachedChannelProvider>> providerList) {
            this.fileCanonicalPath = path;
            this.providerList = providerList;
        }
    }

    private final Map<String, FileToProviderMapEntry> fileToProviderMap;

    private CachedChannelProviderTracker() {
        fileToProviderMap = new KeyedObjectHashMap<>(new KeyedObjectKey.Basic<>() {
            @Override
            public String getKey(FileToProviderMapEntry entry) {
                return entry.fileCanonicalPath;
            }
        });

        // Schedule a cleanup job
        scheduler =
                CommBase.singleThreadedScheduler("CachedChannelProviderTracker.CleanupScheduler", Logger.NULL).start();
        scheduler.installJob(new TimedJob() {
            @Override
            public void timedOut() {
                tryCleanup();
                scheduler.installJob(this, scheduler.currentTimeMillis() + CLEANUP_INTERVAL_MILLIS);
            }
        }, scheduler.currentTimeMillis() + CLEANUP_INTERVAL_MILLIS);
    }

    /**
     * Register a {@link CachedChannelProvider} as associated with a particular {@code file}
     *
     * @param ccp {@link CachedChannelProvider} used to create channels for {@code file}
     * @param file File path
     */
    public final synchronized void registerCachedChannelProvider(@NotNull final CachedChannelProvider ccp,
            @NotNull final File file) throws IOException {
        final String filePath = file.getCanonicalPath();
        FileToProviderMapEntry entry = fileToProviderMap.computeIfAbsent(filePath,
                k -> new FileToProviderMapEntry(filePath, new CopyOnWriteArrayList<>()));
        entry.providerList.add(new WeakReference<>(ccp));
    }

    /**
     * Invalidate all channels and providers associated with the {@code file} to prevent reading overwritten files.
     *
     * @param file File path
     */
    public final synchronized void invalidateChannels(@NotNull final File file) throws IOException {
        final String filePath = file.getCanonicalPath();
        FileToProviderMapEntry entry = fileToProviderMap.remove(filePath);
        if (entry == null) {
            return;
        }
        for (WeakReference<CachedChannelProvider> providerWeakRef : entry.providerList) {
            final CachedChannelProvider ccp = providerWeakRef.get();
            if (ccp != null) {
                ccp.invalidate();
            }
        }
    }

    /**
     * Clear any null weak-references to providers
     */
    private void tryCleanup() {
        final Iterator<Map.Entry<String, FileToProviderMapEntry>> mapIter = fileToProviderMap.entrySet().iterator();
        while (mapIter.hasNext()) {
            final List<WeakReference<CachedChannelProvider>> providerList = mapIter.next().getValue().providerList;
            providerList.removeIf(providerWeakRef -> providerWeakRef.get() == null);
            if (providerList.isEmpty()) {
                mapIter.remove();
            }
        }
    }
}
