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
import java.util.HashMap;
import java.util.Map;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

/// Starts a singleton S3Mock container for native integration tests and exposes
/// the AWS connection properties to the Quarkus test infrastructure, which passes
/// them as `-D` system-property flags to the launched native binary.
public class S3MockTestResource implements QuarkusTestResourceLifecycleManager {

    private S3MockContainer s3Mock;

    @Override
    public Map<String, String> start() {
        s3Mock = new S3MockContainer("latest");
        s3Mock.start();

        try {
            String emptyFile = Files.createTempFile("hardwood-test-aws", "").toString();
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
            }

            Map<String, String> config = new HashMap<>();
            config.put("aws.configFile", emptyFile);
            config.put("aws.sharedCredentialsFile", emptyFile);
            config.put("aws.accessKeyId", "access");
            config.put("aws.secretAccessKey", "secret");
            config.put("aws.region", "us-east-1");
            config.put("aws.endpointUrl", endpoint);
            config.put("aws.pathStyle", "true");
            config.put("quarkus.log.console.enable", "false");
            return config;
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to start S3Mock test resource", e);
        }
    }

    @Override
    public void stop() {
        if (s3Mock != null) {
            s3Mock.stop();
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
        try (InputStream in = S3MockTestResource.class.getResourceAsStream(name)) {
            return in.readAllBytes();
        }
    }
}
