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
///
/// Each library is probed once, when [Library] is initialized: whether a class is on the
/// classpath cannot change during a run, and the read path asks once per decompressed page.
public final class CodecLibraries {

    /// An optional codec library, with the class that marks its presence and the Maven
    /// coordinates to add when it is missing.
    public enum Library {
        SNAPPY("org.xerial.snappy.Snappy", "org.xerial.snappy:snappy-java"),
        ZSTD("com.github.luben.zstd.Zstd", "com.github.luben:zstd-jni"),
        LZ4("net.jpountz.lz4.LZ4Factory", "at.yawk.lz4:lz4-java"),
        BROTLI("com.aayushatharva.brotli4j.Brotli4jLoader", "com.aayushatharva.brotli4j:brotli4j");

        private final String dependency;
        private final boolean present;

        Library(String className, String dependency) {
            this.dependency = dependency;
            this.present = isLoadable(className);
        }

        /// Whether the library is on the classpath.
        public boolean isPresent() {
            return present;
        }
    }

    private CodecLibraries() {
    }

    /// Ensures a codec's library is on the classpath, throwing an
    /// [UnsupportedOperationException] that names the dependency to add otherwise.
    ///
    /// @param library the library that must be present
    /// @param codecName the codec's name, for the error message
    /// @param action `read` or `write`, naming the path that needs the library
    /// @throws UnsupportedOperationException if the library is not on the classpath
    public static void require(Library library, String codecName, String action) {
        if (!library.present) {
            throw new UnsupportedOperationException(
                    "Cannot " + action + " " + codecName + "-compressed Parquet file: required library not found. " +
                            "Add the following dependency to your project: " + library.dependency);
        }
    }

    /// Whether `className` is loadable from this class's loader. The class is probed for
    /// presence only, not initialized, so probing a native-backed codec never triggers its
    /// native load.
    private static boolean isLoadable(String className) {
        try {
            Class.forName(className, false, CodecLibraries.class.getClassLoader());
            return true;
        }
        catch (ClassNotFoundException e) {
            return false;
        }
    }
}
