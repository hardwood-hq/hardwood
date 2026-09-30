/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3;

import java.nio.file.Path;

/// The [s3proxy](https://github.com/gaul/s3proxy) server integration tests
/// read S3 objects from. It runs s3proxy's `filesystem` provider: an object
/// is the file `<data dir>/<bucket>/<key>`, and s3proxy serves whatever is on
/// disk at request time. Tests work in a [TestBucket] of their own, from
/// [#bucketFor].
///
/// [#get()] returns one server per JVM, chosen by environment:
///
/// - [SharedS3Proxy] when `HARDWOOD_S3PROXY_ENDPOINT` and
///   `HARDWOOD_S3PROXY_DATA_DIR` are set: a running server, such as the dev
///   container's `s3proxy` compose service. The tests start no containers.
/// - [ContainerS3Proxy] otherwise: a Testcontainers container, started on
///   first use and stopped when the JVM exits.
///
/// Lives in the non-deployed `hardwood-test-support` module so the `s3`,
/// `cli`, `parquet-java-compat`, and `performance-testing/end-to-end` modules
/// share one image and configuration without exposing it on Maven Central.
public interface S3Proxy {

    /// `andrewgaul/s3proxy` image pinned to the s3proxy 3.1.0 release commit,
    /// mirrored to `ghcr.io/hardwood-hq/s3proxy` so CI runs pull from GHCR
    /// instead of Docker Hub. When bumping this tag, run the
    /// `Mirror Container Images` workflow first to populate the new tag on
    /// GHCR — see `.github/workflows/mirror-container-images.yml` — and bump
    /// the `s3proxy` service in `docker-compose.yaml` to match.
    /// `andrewgaul/s3proxy` publishes only commit-SHA tags and `master`.
    String IMAGE = "ghcr.io/hardwood-hq/s3proxy:sha-6597ca59cd5c5fa8ee313e13d349d507cc6090c3";

    String ACCESS_KEY = "access";
    String SECRET_KEY = "secret";

    /// The server for this JVM, started on the first call.
    static S3Proxy get() {
        return S3ProxyHolder.INSTANCE;
    }

    /// A new bucket for the given test class, not yet created.
    default TestBucket bucketFor(Class<?> testClass) {
        return new TestBucket(this, testClass);
    }

    String endpoint();

    /// The directory the server serves, as seen from the test JVM; each
    /// subdirectory is a bucket.
    Path dataDir();
}
