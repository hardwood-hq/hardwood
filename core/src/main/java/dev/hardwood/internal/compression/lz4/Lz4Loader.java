/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression.lz4;

/**
 * Checks availability of the LZ4 library on the classpath.
 */
public final class Lz4Loader {

    private static final boolean AVAILABLE;

    static {
        boolean available;
        try {
            Class.forName("net.jpountz.lz4.LZ4Factory");
            available = true;
        }
        catch (ClassNotFoundException e) {
            available = false;
        }
        AVAILABLE = available;
    }

    private Lz4Loader() {
    }

    /**
     * Returns true if the lz4-java library is available on the classpath.
     */
    public static boolean isAvailable() {
        return AVAILABLE;
    }

    /**
     * Throws {@link UnsupportedOperationException} if lz4-java is not available.
     *
     * @param codecName the codec name for the error message (e.g. "LZ4" or "LZ4_RAW")
     */
    public static void checkAvailable(String codecName) {
        if (!AVAILABLE) {
            throw new UnsupportedOperationException(
                    "Cannot read " + codecName + "-compressed Parquet file: required library not found. " +
                            "Add the following dependency to your project: at.yawk.lz4:lz4-java");
        }
    }
}
