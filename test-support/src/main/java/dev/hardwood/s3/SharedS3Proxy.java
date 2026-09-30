/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

/// An s3proxy server that is already running, such as the dev container's
/// `s3proxy` compose service. `HARDWOOD_S3PROXY_ENDPOINT` names the server
/// and `HARDWOOD_S3PROXY_DATA_DIR` the directory it serves, as seen from the
/// test JVM.
record SharedS3Proxy(String endpoint, Path dataDir) implements S3Proxy {

    static final String ENDPOINT_ENV = "HARDWOOD_S3PROXY_ENDPOINT";
    static final String DATA_DIR_ENV = "HARDWOOD_S3PROXY_DATA_DIR";

    /// The server the environment names, or empty if it names none.
    static Optional<S3Proxy> fromEnvironment() {
        String endpoint = System.getenv(ENDPOINT_ENV);
        String dataDir = System.getenv(DATA_DIR_ENV);
        if (endpoint == null && dataDir == null) {
            return Optional.empty();
        }
        if (endpoint == null || dataDir == null) {
            throw new IllegalStateException("Set both " + ENDPOINT_ENV + " and " + DATA_DIR_ENV + ", or neither");
        }
        Path path = Path.of(dataDir);
        // Maven runs each module's tests in the module directory, so a
        // relative path would name a different directory per module.
        if (!path.isAbsolute()) {
            throw new IllegalStateException(DATA_DIR_ENV + " must be an absolute path: " + dataDir);
        }
        if (!Files.isDirectory(path)) {
            throw new IllegalStateException(DATA_DIR_ENV + " is not a directory: " + dataDir);
        }
        return Optional.of(new SharedS3Proxy(endpoint, path));
    }
}
