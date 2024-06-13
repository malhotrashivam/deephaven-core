//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.updategraph;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * Provides guidance for initialization operations on how they can parallelize.
 */
public interface OperationInitializer {

    String DEFAULT_NAME = "OPERATION_INITIALIZER";
    String EGRESS_NAME = "EGRESS_OPERATION_INITIALIZER";

    OperationInitializer NON_PARALLELIZABLE = new OperationInitializer() {
        @Override
        public boolean canParallelize() {
            return false;
        }

        @Override
        public Future<?> submit(Runnable runnable) {
            runnable.run();
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public int parallelismFactor() {
            return 1;
        }

        @Override
        public void shutdownNow() {
            // no-op
        }
    };

    /**
     * Whether the current thread can parallelize operations using this OperationInitialization.
     */
    boolean canParallelize();

    /**
     * Submits a task to run in this thread pool.
     */
    Future<?> submit(Runnable runnable);

    /**
     * Number of threads that are potentially available.
     */
    int parallelismFactor();

    /**
     * Shutdown the thread pool.
     */
    void shutdownNow();
}
