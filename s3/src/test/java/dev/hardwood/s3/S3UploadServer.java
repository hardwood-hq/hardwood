/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3;

import org.testcontainers.containers.GenericContainer;

/// A real metadata-capable s3proxy independent of host filesystem xattr support.
final class S3UploadServer implements AutoCloseable {

    private final GenericContainer<?> container;
    private final String endpoint;

    S3UploadServer() {
        String shared = System.getenv("HARDWOOD_S3PROXY_UPLOAD_ENDPOINT");
        if (shared != null && !shared.isBlank()) {
            container = null;
            endpoint = shared;
        }
        else {
            container = new GenericContainer<>(S3Proxy.IMAGE)
                    .withExposedPorts(80)
                    .withEnv("S3PROXY_AUTHORIZATION", "aws-v2-or-v4")
                    .withEnv("S3PROXY_IDENTITY", S3Proxy.ACCESS_KEY)
                    .withEnv("S3PROXY_CREDENTIAL", S3Proxy.SECRET_KEY)
                    .withEnv("S3PROXY_ENDPOINT", "http://0.0.0.0:80")
                    .withEnv("JCLOUDS_PROVIDER", "transient");
            container.start();
            endpoint = "http://" + container.getHost() + ":" + container.getMappedPort(80);
        }
    }

    String endpoint() {
        return endpoint;
    }

    @Override
    public void close() {
        if (container != null) {
            container.stop();
        }
    }
}
