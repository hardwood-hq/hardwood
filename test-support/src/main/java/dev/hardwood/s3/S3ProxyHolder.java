/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3;

import java.time.Duration;

/// Holds the JVM's [S3Proxy], created on first access. Creating it deletes
/// buckets older than [#STALE_AFTER] from the data dir: a shared server
/// outlives test runs, and a run that did not finish never deleted its
/// buckets.
final class S3ProxyHolder {

    static final Duration STALE_AFTER = Duration.ofDays(1);

    static final S3Proxy INSTANCE = create();

    private S3ProxyHolder() {
    }

    private static S3Proxy create() {
        S3Proxy proxy = SharedS3Proxy.fromEnvironment().orElseGet(ContainerS3Proxy::start);
        TestBucket.deleteStaleBuckets(proxy.dataDir(), STALE_AFTER);
        return proxy;
    }
}
