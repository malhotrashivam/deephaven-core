//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.base.pool.Pool;
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

class S3WriteRequest {

    private static final int INVALID_PART_NUMBER = -1;

    private Pool<ByteBuffer> pool;

    /**
     * The buffer for this request
     */
    ByteBuffer buffer;

    /**
     * The part number for the part to be uploaded
     */
    int partNumber;

    /**
     * The future for the part upload, returned by the AWS SDK. This future should be used to cancel the upload, if
     * necessary.
     */
    CompletableFuture<UploadPartResponse> sdkUploadFuture;

    /**
     * A future derived from the SDK future that includes the post-processing/callback logic. This future should be used
     * to wait for the completion of the upload.
     */
    CompletableFuture<UploadPartResponse> completionFuture;

    S3WriteRequest(@NotNull final Pool<ByteBuffer> pool) {
        this.pool = pool;
        buffer = pool.take();
        partNumber = INVALID_PART_NUMBER;
    }

    void releaseBuffer() {
        pool.give(buffer);
        buffer = null;
    }
}
