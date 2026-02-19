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
public final class LibdeflateLoader {

    private LibdeflateLoader() {
    }

    /**
     * Returns false on Java 21 since FFM API is not available.
     * On Java 22+, the real implementation in the multi-release JAR overlay is used.
     */
    public static boolean isAvailable() {
        return false;
    }

    /**
     * Not available on Java 21.
     */
    static Object getSymbolLookup() {
        throw new UnsupportedOperationException("libdeflate requires Java 22+");
    }
}
