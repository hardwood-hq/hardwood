/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding.simd;

/**
 * Runtime detection for SIMD/Vector API support.
 *
 * <p>This is the base implementation for Java 21 that always returns the scalar
 * fallback. On Java 22+, the multi-release JAR overlay provides the real
 * implementation with Vector API support.</p>
 *
 * <p>Use {@code -Dhardwood.simd.disabled=true} to force scalar operations
 * even on Java 22+ for debugging or comparison.</p>
 */
public final class VectorSupport {

    private static final System.Logger LOG = System.getLogger(VectorSupport.class.getName());
    private static final SimdOperations INSTANCE;

    static {
        INSTANCE = new ScalarOperations();
        LOG.log(System.Logger.Level.INFO, "SIMD support: disabled (requires Java 22+)");
    }

    private VectorSupport() {
    }

    /**
     * Returns true if SIMD/Vector API operations are available.
     * Always returns false on Java 21.
     */
    public static boolean isAvailable() {
        return false;
    }

    /**
     * Returns the SIMD operations implementation.
     *
     * <p>On Java 21, this always returns the scalar fallback.
     * On Java 22+, this returns the Vector API implementation if available
     * and not disabled via system property.</p>
     */
    public static SimdOperations operations() {
        return INSTANCE;
    }

    /**
     * Returns the name of the active implementation for diagnostics.
     */
    public static String implementationName() {
        return "scalar";
    }
}
