/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3;

import java.net.URI;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;

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
    static S3MockContainer s3Mock = new S3MockContainer("latest");

    static S3Client s3;

    @BeforeAll
    static void setup() {
        s3 = S3Client.builder()
                .endpointOverride(URI.create(s3Mock.getHttpEndpoint()))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("access", "secret")))
                .region(Region.US_EAST_1)
                .forcePathStyle(true)
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
