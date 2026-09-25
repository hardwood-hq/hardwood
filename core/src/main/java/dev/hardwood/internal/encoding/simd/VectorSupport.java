/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.encoding.simd;

/// Runtime detection for SIMD/Vector API support.
///
/// This is the base implementation, which always selects the scalar kernels. It is
/// in effect on Java 21 and whenever the classes are loaded from a directory rather
/// than the multi-release JAR, for instance when tests run from `target/classes`. On
/// Java 22+ loading from the JAR, the overlay in `META-INF/versions/22` replaces it
/// and selects the Vector API kernels when the incubating Vector API module is added
/// (`--add-modules jdk.incubator.vector`).
public final class VectorSupport {

    private static final System.Logger LOG = System.getLogger(VectorSupport.class.getName());
    private static final SimdOperations INSTANCE;

    static {
        INSTANCE = new ScalarOperations();
        LOG.log(System.Logger.Level.INFO, "SIMD support: disabled (requires Java 22+ and the multi-release JAR)");
    }

    private VectorSupport() {
    }

    /// Returns true if SIMD/Vector API operations are available.
    /// Always returns false in this base implementation.
    public static boolean isAvailable() {
        return false;
    }

    /// Returns the SIMD operations implementation.
    ///
    /// This base implementation always returns the scalar kernels.
    public static SimdOperations operations() {
        return INSTANCE;
    }

    /// Returns the name of the active implementation for diagnostics.
    public static String implementationName() {
        return "scalar";
    }
}
