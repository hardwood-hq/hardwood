/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.compression;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.compression.CodecLibraries.Library;

import static org.assertj.core.api.Assertions.assertThat;

/// The optional codec libraries are all on the test classpath, so the missing-library refusal is
/// exercised by loading [CodecLibraries] a second time through a class loader that sees `core`'s
/// own classes and none of the libraries.
class CodecLibrariesTest {

    @Test
    void librariesOnTheClasspathArePresent() {
        for (Library library : Library.values()) {
            assertThat(library.isPresent()).as(library.name()).isTrue();
        }
    }

    @Test
    void missingLibraryIsRefusedWithItsDependency() throws Exception {
        URL coreClasses = CodecLibraries.class.getProtectionDomain().getCodeSource().getLocation();
        try (URLClassLoader isolated = new URLClassLoader(new URL[]{ coreClasses }, ClassLoader.getPlatformClassLoader())) {
            Class<?> codecLibraries = isolated.loadClass(CodecLibraries.class.getName());
            Class<?> library = isolated.loadClass(Library.class.getName());
            Method require = codecLibraries.getMethod("require", library, String.class, String.class);
            Method isPresent = library.getMethod("isPresent");

            for (Object constant : library.getEnumConstants()) {
                assertThat((boolean) isPresent.invoke(constant)).as(constant.toString()).isFalse();
            }

            assertThat(refusal(require, library, "SNAPPY", "SNAPPY", "read")).hasMessage(
                    "Cannot read SNAPPY-compressed Parquet file: required library not found. "
                            + "Add the following dependency to your project: org.xerial.snappy:snappy-java");
            assertThat(refusal(require, library, "ZSTD", "ZSTD", "write")).hasMessage(
                    "Cannot write ZSTD-compressed Parquet file: required library not found. "
                            + "Add the following dependency to your project: com.github.luben:zstd-jni");
            assertThat(refusal(require, library, "LZ4", "LZ4_RAW", "read")).hasMessage(
                    "Cannot read LZ4_RAW-compressed Parquet file: required library not found. "
                            + "Add the following dependency to your project: at.yawk.lz4:lz4-java");
            assertThat(refusal(require, library, "BROTLI", "BROTLI", "write")).hasMessage(
                    "Cannot write BROTLI-compressed Parquet file: required library not found. "
                            + "Add the following dependency to your project: com.aayushatharva.brotli4j:brotli4j");
        }
    }

    private static Throwable refusal(Method require, Class<?> library, String constant, String codecName, String action)
            throws IllegalAccessException {
        Object libraryConstant = valueOf(library, constant);
        try {
            require.invoke(null, libraryConstant, codecName, action);
        }
        catch (InvocationTargetException e) {
            assertThat(e.getCause()).isInstanceOf(UnsupportedOperationException.class);
            return e.getCause();
        }
        throw new AssertionError("require(" + constant + ") did not refuse a missing library");
    }

    private static Object valueOf(Class<?> library, String constant) {
        for (Object candidate : library.getEnumConstants()) {
            if (((Enum<?>) candidate).name().equals(constant)) {
                return candidate;
            }
        }
        throw new IllegalArgumentException("No library constant " + constant);
    }
}
