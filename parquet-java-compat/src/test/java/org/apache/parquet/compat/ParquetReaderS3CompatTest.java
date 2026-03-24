/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.parquet.compat;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.GroupReadSupport;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import static org.assertj.core.api.Assertions.assertThat;

/// Tests S3 support via the parquet-java compatible API.
@Testcontainers
class ParquetReaderS3CompatTest {

    private static final java.nio.file.Path TEST_RESOURCES = java.nio.file.Path.of("").toAbsolutePath()
            .resolve("../core/src/test/resources").normalize();

    @Container
    static S3MockContainer s3Mock = new S3MockContainer("latest");

    @BeforeAll
    static void setup() {
        S3Client s3 = S3Client.builder()
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
        s3.putObject(
                b -> b.bucket("test-bucket").key("subdir/nested.parquet"),
                TEST_RESOURCES.resolve("plain_uncompressed.parquet"));
        s3.close();
    }

    private Configuration s3Config() {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.access.key", "access");
        conf.set("fs.s3a.secret.key", "secret");
        conf.set("fs.s3a.endpoint", s3Mock.getHttpEndpoint());
        conf.set("fs.s3a.endpoint.region", "us-east-1");
        conf.setBoolean("fs.s3a.path.style.access", true);
        return conf;
    }

    @Test
    void readViaPathAndConfiguration() throws Exception {
        Path path = new Path("s3a://test-bucket/plain_uncompressed.parquet");
        Configuration conf = s3Config();

        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path)
                .withConf(conf)
                .build()) {
            List<Group> records = new ArrayList<>();
            Group record;
            while ((record = reader.read()) != null) {
                records.add(record);
            }

            assertThat(records).hasSize(3);
            assertThat(records.get(0).getLong("id", 0)).isEqualTo(1L);
            assertThat(records.get(0).getLong("value", 0)).isEqualTo(100L);
            assertThat(records.get(1).getLong("id", 0)).isEqualTo(2L);
            assertThat(records.get(2).getLong("id", 0)).isEqualTo(3L);
        }
    }

    @Test
    void readViaHadoopInputFile() throws Exception {
        Path path = new Path("s3a://test-bucket/plain_uncompressed.parquet");
        Configuration conf = s3Config();

        InputFile inputFile = HadoopInputFile.fromPath(path, conf);

        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), inputFile).build()) {
            List<Group> records = new ArrayList<>();
            Group record;
            while ((record = reader.read()) != null) {
                records.add(record);
            }

            assertThat(records).hasSize(3);
            assertThat(records.get(0).getLong("id", 0)).isEqualTo(1L);
        }
    }

    @Test
    void readFromSubdirectory() throws Exception {
        Path path = new Path("s3a://test-bucket/subdir/nested.parquet");
        Configuration conf = s3Config();

        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path)
                .withConf(conf)
                .build()) {
            Group record = reader.read();
            assertThat(record).isNotNull();
            assertThat(record.getLong("id", 0)).isEqualTo(1L);
        }
    }

    @Test
    void s3PathParsing() {
        Path path = new Path("s3a://my-bucket/some/key.parquet");
        assertThat(path.toUri().getScheme()).isEqualTo("s3a");
        assertThat(path.toUri().getHost()).isEqualTo("my-bucket");
        assertThat(path.toUri().getPath()).isEqualTo("/some/key.parquet");
        assertThat(path.getName()).isEqualTo("key.parquet");
    }

    @Test
    void localPathStillWorks() throws Exception {
        Path path = new Path("../core/src/test/resources/plain_uncompressed.parquet");

        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path).build()) {
            Group record = reader.read();
            assertThat(record).isNotNull();
            assertThat(record.getLong("id", 0)).isEqualTo(1L);
        }
    }
}
