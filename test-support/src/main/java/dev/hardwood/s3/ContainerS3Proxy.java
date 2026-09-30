/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;

/// An s3proxy Testcontainers container serving `target/s3proxy/` of the
/// module under test, bind-mounted read-only, so the Docker daemon must be
/// able to see that path. It runs until the JVM exits.
record ContainerS3Proxy(String endpoint, Path dataDir) implements S3Proxy {

    private static final int PORT = 80;
    private static final String CONTAINER_DATA_DIR = "/data";

    static S3Proxy start() {
        Path dataDir = Path.of("target", "s3proxy").toAbsolutePath();
        try {
            Files.createDirectories(dataDir);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to create " + dataDir, e);
        }
        GenericContainer<?> container = new GenericContainer<>(IMAGE)
                .withExposedPorts(PORT)
                .withEnv("S3PROXY_AUTHORIZATION", "aws-v2-or-v4")
                .withEnv("S3PROXY_IDENTITY", ACCESS_KEY)
                .withEnv("S3PROXY_CREDENTIAL", SECRET_KEY)
                .withEnv("S3PROXY_ENDPOINT", "http://0.0.0.0:" + PORT)
                .withEnv("JCLOUDS_PROVIDER", "filesystem")
                .withEnv("JCLOUDS_FILESYSTEM_BASEDIR", CONTAINER_DATA_DIR)
                .withFileSystemBind(dataDir.toString(), CONTAINER_DATA_DIR, BindMode.READ_ONLY);
        container.start();
        Runtime.getRuntime().addShutdownHook(new Thread(container::stop));
        return new ContainerS3Proxy("http://" + container.getHost() + ":" + container.getMappedPort(PORT), dataDir);
    }
}
