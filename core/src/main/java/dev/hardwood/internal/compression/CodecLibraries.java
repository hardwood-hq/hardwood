/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression;

/// Probes for the optional per-codec libraries (zstd-jni, snappy-java, lz4-java, …) on the
/// classpath. These are optional dependencies, so both the read and write paths must confirm
/// a codec's library is present before handing work to it, and fail with an actionable message
/// naming the missing dependency when it is not.
public final class CodecLibraries {

    private CodecLibraries() {
    }

    /// Whether `className` is loadable from this class's loader. The class is probed for
    /// presence only, not initialized, so probing a native-backed codec never triggers its
    /// native load.
    ///
    /// @param className the fully qualified class to probe for
    /// @return `true` when the class is on the classpath
    public static boolean isPresent(String className) {
        try {
            Class.forName(className, false, CodecLibraries.class.getClassLoader());
            return true;
        }
        catch (ClassNotFoundException e) {
            return false;
        }
    }

    /// Ensures the library class backing a codec is on the classpath, throwing an
    /// [UnsupportedOperationException] that names the dependency to add otherwise.
    ///
    /// @param className the library class that must be loadable
    /// @param codecName the codec's name, for the error message
    /// @param dependency the Maven coordinates to add when the library is missing
    /// @param action `read` or `write`, naming the path that needs the library
    /// @throws UnsupportedOperationException if the library is not on the classpath
    public static void require(String className, String codecName, String dependency, String action) {
        if (!isPresent(className)) {
            throw new UnsupportedOperationException(
                    "Cannot " + action + " " + codecName + "-compressed Parquet file: required library not found. " +
                            "Add the following dependency to your project: " + dependency);
        }
    }
}
