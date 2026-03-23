/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;

import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
class S3InputFileTest {

    private static final Path TEST_RESOURCES = Path.of("").toAbsolutePath()
            .resolve("../core/src/test/resources").normalize();

    @Container
    static S3MockContainer s3Mock = new S3MockContainer("latest");

    static S3Client s3;

    @BeforeAll
    static void setup() {
        s3 = createS3Client();

        s3.createBucket(b -> b.bucket("test-bucket"));

        uploadTestFile("plain_uncompressed.parquet");
        uploadTestFile("plain_uncompressed_with_nulls.parquet");
    }

    private static S3Client createS3Client() {
        return S3Client.builder()
                .endpointOverride(URI.create(s3Mock.getHttpEndpoint()))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("access", "secret")))
                .region(Region.US_EAST_1)
                .forcePathStyle(true)
                .build();
    }

    private static void uploadTestFile(String name) {
        s3.putObject(
                b -> b.bucket("test-bucket").key(name),
                TEST_RESOURCES.resolve(name));
    }

    @Test
    void readMetadata() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(
                S3InputFile.of(s3, "test-bucket", "plain_uncompressed.parquet"))) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(3);
        }
    }

    @Test
    void readRows() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(
                S3InputFile.of(s3, "test-bucket", "plain_uncompressed.parquet"))) {
            try (RowReader rows = reader.createRowReader()) {
                int count = 0;
                while (rows.hasNext()) {
                    rows.next();
                    count++;
                }
                assertThat(count).isEqualTo(3);
            }
        }
    }

    @Test
    void readRowValues() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(
                S3InputFile.of(s3, "test-bucket", "plain_uncompressed.parquet"))) {
            try (RowReader rows = reader.createRowReader()) {
                assertThat(rows.hasNext()).isTrue();
                rows.next();
                assertThat(rows.getLong("id")).isEqualTo(1L);
                assertThat(rows.getLong("value")).isEqualTo(100L);

                assertThat(rows.hasNext()).isTrue();
                rows.next();
                assertThat(rows.getLong("id")).isEqualTo(2L);
                assertThat(rows.getLong("value")).isEqualTo(200L);

                assertThat(rows.hasNext()).isTrue();
                rows.next();
                assertThat(rows.getLong("id")).isEqualTo(3L);
                assertThat(rows.getLong("value")).isEqualTo(300L);

                assertThat(rows.hasNext()).isFalse();
            }
        }
    }

    @Test
    void readWithNulls() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(
                S3InputFile.of(s3, "test-bucket", "plain_uncompressed_with_nulls.parquet"))) {
            try (RowReader rows = reader.createRowReader()) {
                int count = 0;
                while (rows.hasNext()) {
                    rows.next();
                    count++;
                }
                assertThat(count).isGreaterThan(0);
            }
        }
    }

    @Test
    void fileNotFound() {
        assertThatThrownBy(() ->
                ParquetFileReader.open(
                        S3InputFile.of(s3, "test-bucket", "nonexistent.parquet")))
                .isInstanceOf(IOException.class);
    }

    @Test
    void name() {
        S3InputFile file = S3InputFile.of(s3, "test-bucket", "data/file.parquet");
        assertThat(file.name()).isEqualTo("s3://test-bucket/data/file.parquet");
    }
}
