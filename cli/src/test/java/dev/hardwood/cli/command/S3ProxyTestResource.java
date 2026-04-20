/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.MountableFile;

import dev.hardwood.s3.S3ProxyContainers;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

/// Starts a singleton s3proxy container for native integration tests and
/// exposes the AWS connection properties to the Quarkus test infrastructure,
/// which passes them as `-D` system-property flags to the launched native
/// binary.
///
/// `plain_uncompressed.parquet` from `core/src/test/resources/` is copied into
/// the container at startup as `s3://test-bucket/plain_uncompressed.parquet`,
/// so no upload step is needed.
public class S3ProxyTestResource implements QuarkusTestResourceLifecycleManager {

    private static final Path TEST_RESOURCES = Path.of("").toAbsolutePath()
            .resolve("../core/src/test/resources").normalize();

    private GenericContainer<?> s3;

    @Override
    public Map<String, String> start() {
        s3 = S3ProxyContainers.filesystemBacked()
                .withCopyFileToContainer(
                        MountableFile.forHostPath(TEST_RESOURCES.resolve("plain_uncompressed.parquet")),
                        S3ProxyContainers.objectPath("plain_uncompressed.parquet"));
        s3.start();

        try {
            String emptyFile = Files.createTempFile("hardwood-test-aws", "").toString();

            Map<String, String> config = new HashMap<>();
            config.put("aws.configFile", emptyFile);
            config.put("aws.sharedCredentialsFile", emptyFile);
            config.put("aws.accessKeyId", S3ProxyContainers.ACCESS_KEY);
            config.put("aws.secretAccessKey", S3ProxyContainers.SECRET_KEY);
            config.put("aws.region", "us-east-1");
            config.put("aws.endpointUrl", S3ProxyContainers.endpoint(s3));
            config.put("aws.pathStyle", "true");
            config.put("quarkus.log.console.enable", "false");
            return config;
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to start s3proxy test resource", e);
        }
    }

    @Override
    public void stop() {
        if (s3 != null) {
            s3.stop();
        }
    }
}
