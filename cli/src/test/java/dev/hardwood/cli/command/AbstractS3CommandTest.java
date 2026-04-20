/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package dev.hardwood.cli.command;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;

/// Singleton S3Mock container shared across all S3 command tests.
/// The container starts once when this class is loaded and is stopped
/// automatically by Testcontainers' shutdown hook when the JVM exits.
abstract class AbstractS3CommandTest {
    protected static final String S3_FILE = "s3://test-bucket/plain_uncompressed.parquet";
    protected static final String S3_DICT_FILE = "s3://test-bucket/dictionary_uncompressed.parquet";
    protected static final String S3_BYTE_ARRAY_FILE = "s3://test-bucket/delta_byte_array_test.parquet";
    protected static final String S3_DEEP_NESTED_FILE = "s3://test-bucket/deep_nested_struct_test.parquet";
    protected static final String S3_LIST_FILE = "s3://test-bucket/list_basic_test.parquet";
    protected static final String S3_NONEXISTENT_FILE = "s3://test-bucket/nonexistent.parquet";
    protected static final String S3_UNSIGNED_INT_FILE = "s3://test-bucket/unsigned_int_test.parquet";
    protected static final String S3_PAGE_INDEX_FILE = "s3://test-bucket/column_index_pushdown.parquet";

    static final S3MockContainer s3Mock = new S3MockContainer("latest");

    static {
        s3Mock.start();

        try {
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
            System.setProperty("aws.pathStyle", "true");

            String endpoint = s3Mock.getHttpEndpoint();
            try (HttpClient http = HttpClient.newHttpClient()) {
                putS3(http, endpoint, "/test-bucket", new byte[0]);
                putS3(http, endpoint, "/test-bucket/plain_uncompressed.parquet",
                        readClasspathResource("/plain_uncompressed.parquet"));
                putS3(http, endpoint, "/test-bucket/dictionary_uncompressed.parquet",
                        readClasspathResource("/dictionary_uncompressed.parquet"));
                putS3(http, endpoint, "/test-bucket/delta_byte_array_test.parquet",
                        readClasspathResource("/delta_byte_array_test.parquet"));
                putS3(http, endpoint, "/test-bucket/deep_nested_struct_test.parquet",
                        readClasspathResource("/deep_nested_struct_test.parquet"));
                putS3(http, endpoint, "/test-bucket/list_basic_test.parquet",
                        readClasspathResource("/list_basic_test.parquet"));
                putS3(http, endpoint, "/test-bucket/unsigned_int_test.parquet",
                        readClasspathResource("/unsigned_int_test.parquet"));
                putS3(http, endpoint, "/test-bucket/column_index_pushdown.parquet",
                        readClasspathResource("/column_index_pushdown.parquet"));
            }
        }
        catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static void putS3(HttpClient http, String endpoint, String path, byte[] body)
            throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(endpoint + path))
                .PUT(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();
        HttpResponse<Void> response = http.send(request, HttpResponse.BodyHandlers.discarding());
        if (response.statusCode() >= 300) {
            throw new IOException("S3Mock PUT " + path + " failed with status " + response.statusCode());
        }
    }

    private static byte[] readClasspathResource(String name) throws IOException {
        try (InputStream in = AbstractS3CommandTest.class.getResourceAsStream(name)) {
            return in.readAllBytes();
        }
    }
}
