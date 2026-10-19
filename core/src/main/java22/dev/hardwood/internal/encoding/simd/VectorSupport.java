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

/// Runtime detection for SIMD/Vector API support (Java 22+ version).
///
/// This is the multi-release JAR overlay that provides actual Vector API
/// detection and SIMD operations when running on Java 22+.
///
/// SIMD engages only when the incubating Vector API module is resolved
/// (`--add-modules jdk.incubator.vector`). Detection checks for the module before it
/// touches the Vector API, and selects the scalar operations when the module is absent
/// or the API fails to link. That launch flag is therefore the opt-out: omit it to run
/// scalar.
public final class VectorSupport {

    private static final boolean AVAILABLE;
    private static final SimdOperations INSTANCE;
    private static final String IMPL_NAME;

    static {
        boolean available = false;
        SimdOperations ops = new ScalarOperations();
        String implName = "scalar";

        // Without the module, resolving IntVector makes the class loader search every jar on
        // the class path before it fails, which on a large class path costs tens of milliseconds.
        if (vectorModulePresent()) {
            try {
                // Verify Vector API is working
                VectorSpecies<Integer> species = IntVector.SPECIES_PREFERRED;
                int vectorLength = species.length();

                if (vectorLength >= 4) {
                    // Vector API is available and has reasonable vector width
                    ops = new VectorOperations();
                    available = true;
                    implName = "simd-" + species.vectorBitSize() + "bit";
                }
            }
            catch (Throwable t) {
                // The Vector API fails to link: scalar operations stay in place.
            }
        }

        AVAILABLE = available;
        INSTANCE = ops;
        IMPL_NAME = implName;
    }

    private VectorSupport() {
    }

    private static boolean vectorModulePresent() {
        ModuleLayer layer = VectorSupport.class.getModule().getLayer();
        return (layer != null ? layer : ModuleLayer.boot()).findModule("jdk.incubator.vector").isPresent();
    }

    /// Returns true if SIMD/Vector API operations are available and enabled.
    public static boolean isAvailable() {
        return AVAILABLE;
    }

    /// Returns the SIMD operations implementation.
    ///
    /// Returns the Vector API implementation if available and enabled,
    /// otherwise returns the scalar fallback.
    public static SimdOperations operations() {
        return INSTANCE;
    }

    /// Returns the name of the active implementation for diagnostics.
    /// Example: "scalar", "simd-256bit", "simd-512bit"
    public static String implementationName() {
        return IMPL_NAME;
    }
}
