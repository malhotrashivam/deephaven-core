/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.extensions.s3;


import io.deephaven.util.channel.CachedChannelProvider;
import io.deephaven.util.channel.SeekableChannelContext;
import io.deephaven.util.channel.SeekableChannelsProvider;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.localstack.LocalStackContainer.Service;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
public class S3SeekableChannelTest {

    @Container
    private final LocalStackContainer localstack =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.0.2")).withServices(Service.S3);

    // todo: move to unique key-name per test, can make localstack static then; will make testing much faster
    // @BucketKey
    // String key;

    @Test
    void emptyFile() throws IOException {
        try (final S3Client client = client()) {
            client.createBucket(CreateBucketRequest.builder().bucket("my-bucket-name").build());
            client.putObject(PutObjectRequest.builder()
                    .bucket("my-bucket-name")
                    .key("empty.txt")
                    .build(),
                    RequestBody.empty());
        }
        final URI uri = URI.create("s3://my-bucket-name/empty.txt");
        final ByteBuffer buffer = ByteBuffer.allocate(1);
        // todo: should we be creating S3SeekableByteChannel directly and test this structure separately?
        try (
                final SeekableChannelsProvider providerImpl = providerImpl(uri);
                final SeekableChannelsProvider provider = new CachedChannelProvider(providerImpl, 32);
                final SeekableChannelContext context = provider.makeContext();
                final SeekableByteChannel readChannel = provider.getReadChannel(context, uri)) {
            assertThat(readChannel.read(buffer)).isEqualTo(-1);
        }
    }

    @Test
    void read32MiB() throws IOException {
        final int numBytes = 33554432;
        try (final S3Client client = client()) {
            client.createBucket(CreateBucketRequest.builder().bucket("my-bucket-name").build());
            client.putObject(PutObjectRequest.builder()
                    .bucket("my-bucket-name")
                    .key("32MiB.bin")
                    .build(),
                    RequestBody.fromInputStream(new InputStream() {
                        @Override
                        public int read() throws IOException {
                            return 42;
                        }
                    }, numBytes));
        }
        final URI uri = URI.create("s3://my-bucket-name/32MiB.bin");
        final ByteBuffer buffer = ByteBuffer.allocate(1);
        // todo: should we be creating S3SeekableByteChannel directly and test this structure separately?
        try (
                final SeekableChannelsProvider providerImpl = providerImpl(uri);
                final SeekableChannelsProvider provider = new CachedChannelProvider(providerImpl, 32);
                final SeekableChannelContext context = provider.makeContext();
                final SeekableByteChannel readChannel = provider.getReadChannel(context, uri)) {
            for (long p = 0; p < numBytes; ++p) {
                assertThat(readChannel.read(buffer)).isEqualTo(1);
                assertThat(buffer.get(0)).isEqualTo((byte) 42);
                buffer.clear();
            }
            assertThat(readChannel.read(buffer)).isEqualTo(-1);
        }
    }

    private S3Client client() {
        return S3Client
                .builder()
                .endpointOverride(localstack.getEndpoint())
                .credentialsProvider(credentialsProvider())
                .region(Region.of(localstack.getRegion()))
                .build();
    }

    private SeekableChannelsProvider providerImpl(URI uri) {
        final S3SeekableChannelProviderPlugin plugin = new S3SeekableChannelProviderPlugin();
        final S3Instructions instructions = S3Instructions.builder()
                .awsRegionName(localstack.getRegion())
                .credentialsProvider(credentialsProvider())
                .endpoint(localstack.getEndpoint())
                .build();
        return plugin.createProvider(uri, instructions);
    }

    private StaticCredentialsProvider credentialsProvider() {
        return StaticCredentialsProvider.create(
                AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey()));
    }
}
