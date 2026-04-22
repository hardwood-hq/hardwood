/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3;

import org.testcontainers.containers.GenericContainer;

/// Test helper that builds [s3proxy](https://github.com/gaul/s3proxy)
/// Testcontainers instances with the `filesystem` jclouds backend rooted at
/// `/data`. Callers populate the bucket "test-bucket" before the container
/// starts via [GenericContainer#withCopyFileToContainer] (host file) or
/// [GenericContainer#withCopyToContainer] (in-memory bytes), placing each
/// object at the path returned by [#objectPath].
///
/// `withCopyFileToContainer` works under Docker-in-Docker (the testcontainers
/// client streams the file to the daemon over the docker socket), unlike
/// `withFileSystemBind`, whose paths must be visible to the daemon directly.
///
/// Published as part of the `hardwood-s3` test-jar so the `cli` and
/// `parquet-java-compat` modules can share a single image SHA and container
/// configuration.
public final class S3ProxyContainers {

    /// `andrewgaul/s3proxy` image pinned to the s3proxy 3.1.0 release commit,
    /// mirrored to `ghcr.io/hardwood-hq/s3proxy` so CI runs pull from GHCR
    /// instead of Docker Hub. When bumping this tag, run the
    /// `Mirror Container Images` workflow first to populate the new tag on
    /// GHCR — see `.github/workflows/mirror-container-images.yml`.
    /// `andrewgaul/s3proxy` publishes only commit-SHA tags and `master`.
    public static final String IMAGE = "ghcr.io/hardwood-hq/s3proxy:sha-6597ca59cd5c5fa8ee313e13d349d507cc6090c3";

    public static final String ACCESS_KEY = "access";
    public static final String SECRET_KEY = "secret";
    public static final String BUCKET = "test-bucket";
    public static final int PORT = 80;

    private S3ProxyContainers() {
    }

    public static GenericContainer<?> filesystemBacked() {
        return new GenericContainer<>(IMAGE)
                .withExposedPorts(PORT)
                .withEnv("S3PROXY_AUTHORIZATION", "aws-v2-or-v4")
                .withEnv("S3PROXY_IDENTITY", ACCESS_KEY)
                .withEnv("S3PROXY_CREDENTIAL", SECRET_KEY)
                .withEnv("S3PROXY_ENDPOINT", "http://0.0.0.0:" + PORT)
                .withEnv("JCLOUDS_PROVIDER", "filesystem")
                .withEnv("JCLOUDS_FILESYSTEM_BASEDIR", "/data");
    }

    /// Container path for an object with the given key in bucket "test-bucket".
    public static String objectPath(String key) {
        return "/data/" + BUCKET + "/" + key;
    }

    public static String endpoint(GenericContainer<?> container) {
        return "http://" + container.getHost() + ":" + container.getMappedPort(PORT);
    }
}
