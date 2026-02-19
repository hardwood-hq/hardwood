/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding.simd;

import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorSpecies;

/**
 * Runtime detection for SIMD/Vector API support (Java 22+ version).
 *
 * <p>This is the multi-release JAR overlay that provides actual Vector API
 * detection and SIMD operations when running on Java 22+.</p>
 *
 * <p>Use {@code -Dhardwood.simd.disabled=true} to force scalar operations
 * for debugging or comparison.</p>
 */
public final class VectorSupport {

    private static final System.Logger LOG = System.getLogger(VectorSupport.class.getName());

    private static final boolean AVAILABLE;
    private static final SimdOperations INSTANCE;
    private static final String IMPL_NAME;

    static {
        boolean available = false;
        SimdOperations ops = new ScalarOperations();
        String implName = "scalar";

        // Check if SIMD is disabled via system property
        boolean disabled = Boolean.getBoolean("hardwood.simd.disabled");

        if (disabled) {
            LOG.log(System.Logger.Level.INFO, "SIMD support: disabled via system property");
        }
        else {
            try {
                // Verify Vector API is working
                VectorSpecies<Integer> species = IntVector.SPECIES_PREFERRED;
                int vectorLength = species.length();

                if (vectorLength >= 4) {
                    // Vector API is available and has reasonable vector width
                    ops = new VectorOperations();
                    available = true;
                    implName = "simd-" + species.vectorBitSize() + "bit";
                    LOG.log(System.Logger.Level.INFO, "SIMD support: enabled ({0}-bit vectors)", species.vectorBitSize());
                }
                else {
                    LOG.log(System.Logger.Level.INFO, "SIMD support: disabled (vector length {0} too small)", vectorLength);
                }
            }
            catch (Throwable t) {
                LOG.log(System.Logger.Level.INFO, "SIMD support: disabled (Vector API not available: {0})", t.getMessage());
            }
        }

        AVAILABLE = available;
        INSTANCE = ops;
        IMPL_NAME = implName;
    }

    private VectorSupport() {
    }

    /**
     * Returns true if SIMD/Vector API operations are available and enabled.
     */
    public static boolean isAvailable() {
        return AVAILABLE;
    }

    /**
     * Returns the SIMD operations implementation.
     *
     * <p>Returns the Vector API implementation if available and enabled,
     * otherwise returns the scalar fallback.</p>
     */
    public static SimdOperations operations() {
        return INSTANCE;
    }

    /**
     * Returns the name of the active implementation for diagnostics.
     * Example: "scalar", "simd-256bit", "simd-512bit"
     */
    public static String implementationName() {
        return IMPL_NAME;
    }
}
