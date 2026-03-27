/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.command;

import java.lang.reflect.Field;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

@Testcontainers
abstract class AbstractS3CommandTest {
    protected static final String S3_FILE = "s3://test-bucket/plain_uncompressed.parquet";
    protected static final String S3_DICT_FILE = "s3://test-bucket/dictionary_uncompressed.parquet";
    protected static final String S3_NONEXISTENT_FILE = "s3://test-bucket/nonexistent.parquet";

    private static final Path TEST_RESOURCES = Path.of("").toAbsolutePath()
            .resolve("../core/src/test/resources").normalize();

    @Container
    static S3MockContainer s3Mock = new S3MockContainer("latest");

    @BeforeAll
    static void setup() throws Exception {
        // Redirect AWS profile files to an empty temp file so the SDK does not parse
        // the developer's ~/.aws/config (which may contain non-standard profiles that
        // trigger parse warnings and interfere with the test credential provider chain).
        String emptyFile = Files.createTempFile("hardwood-test-aws", "").toString();
        System.setProperty("aws.configFile", emptyFile);
        System.setProperty("aws.sharedCredentialsFile", emptyFile);

        System.setProperty("aws.accessKeyId", "access");
        System.setProperty("aws.secretAccessKey", "secret");
        System.setProperty("aws.region", "us-east-1");
        System.setProperty("aws.endpointUrl", s3Mock.getHttpEndpoint());
        System.setProperty("AWS_ENDPOINT_URL", s3Mock.getHttpEndpoint());

        S3Client s3 = S3Client.builder()
                .endpointOverride(URI.create(s3Mock.getHttpEndpoint()))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("access", "secret")))
                .region(Region.US_EAST_1)
                .forcePathStyle(true)
                .build();

        s3.createBucket(b -> b.bucket("test-bucket"));
        s3.putObject(b -> b.bucket("test-bucket").key("plain_uncompressed.parquet"),
                TEST_RESOURCES.resolve("plain_uncompressed.parquet"));
        s3.putObject(b -> b.bucket("test-bucket").key("dictionary_uncompressed.parquet"),
                TEST_RESOURCES.resolve("dictionary_uncompressed.parquet"));
        s3.close();
    }
}
