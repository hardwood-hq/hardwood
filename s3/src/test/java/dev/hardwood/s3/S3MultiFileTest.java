/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3;

import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import dev.hardwood.Hardwood;
import dev.hardwood.InputFile;
import dev.hardwood.reader.MultiFileParquetReader;
import dev.hardwood.reader.MultiFileRowReader;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class S3MultiFileTest {

    private static final Path TEST_RESOURCES = Path.of("").toAbsolutePath()
            .resolve("../core/src/test/resources").normalize();

    @Container
    static LocalStackContainer localstack = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:latest"))
            .withServices(LocalStackContainer.Service.S3);

    static S3Client s3;

    @BeforeAll
    static void setup() {
        s3 = S3Client.builder()
                .endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.S3))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())))
                .region(Region.of(localstack.getRegion()))
                .build();

        s3.createBucket(b -> b.bucket("test-bucket"));

        s3.putObject(
                b -> b.bucket("test-bucket").key("plain_uncompressed.parquet"),
                TEST_RESOURCES.resolve("plain_uncompressed.parquet"));
    }

    @Test
    void readMultipleFiles() throws Exception {
        List<InputFile> files = List.of(
                S3InputFile.of(s3, "test-bucket", "plain_uncompressed.parquet"),
                S3InputFile.of(s3, "test-bucket", "plain_uncompressed.parquet"));

        try (Hardwood hardwood = Hardwood.create();
                MultiFileParquetReader reader = hardwood.openAll(files)) {
            try (MultiFileRowReader rows = reader.createRowReader()) {
                int count = 0;
                while (rows.hasNext()) {
                    rows.next();
                    count++;
                }
                assertThat(count).isEqualTo(6); // 3 rows x 2 files
            }
        }
    }
}
