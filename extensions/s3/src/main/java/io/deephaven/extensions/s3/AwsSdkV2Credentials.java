//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.util.annotations.InternalUseOnly;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

@InternalUseOnly
interface AwsSdkV2Credentials extends Credentials {

    /**
     * Create an AWS SDK v2 credentials provider using the provided instructions.
     */
    AwsCredentialsProvider awsV2CredentialsProvider(S3Instructions instructions);
}
