/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression.libdeflate;

/**
 * Stub implementation for Java 21 (no FFM API).
 * The real implementation using FFM is in META-INF/versions/22/ for Java 22+.
 */
public final class LibdeflatePool {

    /**
     * Create a new pool with default max size.
     */
    public LibdeflatePool() {
    }

    /**
     * Create a new pool with the specified max size.
     */
    public LibdeflatePool(int maxPoolSize) {
    }

    /**
     * No-op on Java 21 since libdeflate is not available.
     * On Java 22+, this clears the decompressor pool.
     */
    public void clear() {
        // No-op: libdeflate not available on Java 21
    }
}
