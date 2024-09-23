//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.annotations.CopyableStyle;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import org.immutables.value.Value;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

import static io.deephaven.extensions.s3.S3ClientFactory.getOrBuildStsClient;

@Value.Immutable
@CopyableStyle
public abstract class AssumeRoleCredentials implements LogOutputAppendable, AwsSdkV2Credentials {

    public static Builder builder() {
        return ImmutableAssumeRoleCredentials.builder();
    }

    public abstract String roleArn();

    @Value.Default
    public String roleSessionName() {
        return String.format("dh-iceberg-aws-%s", UUID.randomUUID());
    }

    public abstract Optional<Duration> roleSessionDuration();

    public abstract Optional<String> externalId();

    @Override
    public LogOutput append(final LogOutput logOutput) {
        return logOutput.append(toString());
    }

    @Override
    public final AwsCredentialsProvider awsV2CredentialsProvider(final S3Instructions instructions) {
        final AssumeRoleRequest.Builder assumeRoleRequestBuilder = AssumeRoleRequest.builder()
                .roleArn(roleArn())
                .roleSessionName(roleSessionName());
        roleSessionDuration().ifPresent(duration -> assumeRoleRequestBuilder.durationSeconds((int) duration.getSeconds()));
        externalId().ifPresent(assumeRoleRequestBuilder::externalId);
        final AssumeRoleRequest assumeRoleRequest = assumeRoleRequestBuilder.build();

        // TODO Make sure this provider is closed
        return StsAssumeRoleCredentialsProvider.builder()
                .stsClient(getOrBuildStsClient(instructions))
                .refreshRequest(assumeRoleRequest)
                .asyncCredentialUpdateEnabled(true)
                .build();
    }

    public interface Builder {
        Builder roleArn(String roleArn);

        Builder roleSessionDuration(Duration roleSessionDuration);

        Builder roleSessionName(String roleSessionName);

        Builder externalId(String externalId);

        AssumeRoleCredentials build();
    }
}
