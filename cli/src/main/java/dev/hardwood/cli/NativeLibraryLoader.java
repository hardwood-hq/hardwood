/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli;

import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.CodeSource;
import java.util.Locale;
import java.util.stream.Stream;

/// Loads compression native libraries (zstd-jni, snappy-java, lz4-java, brotli4j) when running as a
/// GraalVM native image. Libraries must be placed in a directory next to the executable (e.g. lib/...)
/// or pointed to by `HARDWOOD_LIB_PATH`.
public final class NativeLibraryLoader {

    private static final String ZSTD_JNI_VERSION = "1.5.7-6";
    private static final String OS_NAME_PROP = "os.name";

    private NativeLibraryLoader() {
    }

    public static boolean inImageCode() {
        try {
            Class<?> c = Class.forName("org.graalvm.nativeimage.ImageInfo");
            Object result = c.getMethod("inImageCode").invoke(null);
            return result instanceof Boolean b && b;
        }
        catch (ReflectiveOperationException e) {
            return false;
        }
    }

    /// Loads zstd-jni native library. No-op on JVM (zstd-jni loads from the JAR).
    public static void loadZstd() {
        loadCodec("zstd", "libzstd-jni-" + ZSTD_JNI_VERSION, "libzstd-jni-",
                NativeLibraryLoader::assumeZstdLoaded);
    }

    /// Loads lz4-java native library. No-op on JVM (lz4-java loads from the JAR).
    public static void loadLz4() {
        loadCodec("lz4", "liblz4-java", "liblz4-java", null);
    }

    /// Loads snappy-java native library. No-op on JVM (snappy-java loads from the JAR).
    public static void loadSnappy() {
        loadCodec("snappy", "libsnappyjava", "libsnappyjava",
                NativeLibraryLoader::assumeSnappyLoaded);
    }

    private static void loadCodec(String name, String exactBaseName, String scanPrefix, Runnable postLoad) {
        if (!inImageCode()) {
            return;
        }
        Path libDir = resolveLibDir();
        if (libDir == null) {
            return;
        }
        loadNative(name, resolveLibFile(libDir, exactBaseName, scanPrefix), postLoad);
    }

    private static void loadNative(String name, Path libFile, Runnable postLoad) {
        if (libFile == null || !Files.isRegularFile(libFile)) {
            return;
        }
        try {
            System.load(libFile.toAbsolutePath().toString());
            if (postLoad != null) {
                postLoad.run();
            }
        }
        catch (UnsatisfiedLinkError e) {
            System.err.println("WARNING: Could not load " + name + " native library from " + libFile + ": " + e.getMessage());
        }
    }

    private static Path resolveLibDir() {
        String env = System.getenv("HARDWOOD_LIB_PATH");
        if (env != null && !env.isBlank()) {
            Path p = Path.of(env.trim());
            if (Files.isDirectory(p)) {
                return p;
            }
        }
        Path exeDir = getExecutableParent();
        if (exeDir != null) {
            Path libDir = exeDir.getParent().resolve("lib");
            if (Files.isDirectory(libDir)) {
                return libDir;
            }
        }
        return null;
    }

    private static Path getExecutableParent() {
        try {
            CodeSource src = NativeLibraryLoader.class.getProtectionDomain().getCodeSource();
            if (src == null || src.getLocation() == null) {
                return null;
            }
            Path exe = Path.of(src.getLocation().toURI());
            return exe.getParent();
        }
        catch (URISyntaxException | NullPointerException e) {
            return null;
        }
    }

    /// Resolves a native library file within `libDir`.
    ///
    /// @param exactBaseName file base name to try first (without extension)
    /// @param scanPrefix    prefix used as a fallback when scanning the directory
    private static Path resolveLibFile(Path libDir, String exactBaseName, String scanPrefix) {
        String ext = nativeLibExtension();
        Path exact = libDir.resolve(exactBaseName + ext);
        if (Files.isRegularFile(exact)) {
            return exact;
        }
        try (Stream<Path> list = Files.list(libDir)) {
            return list
                    .filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().startsWith(scanPrefix) && p.getFileName().toString().endsWith(ext))
                    .findFirst()
                    .orElse(null);
        }
        catch (Exception e) {
            return null;
        }
    }

    private static String nativeLibExtension() {
        String os = System.getProperty(OS_NAME_PROP, "").toLowerCase(Locale.ROOT);
        if (os.contains("windows")) {
            return ".dll";
        }
        if (os.contains("mac") || os.contains("darwin")) {
            return ".dylib";
        }
        return ".so";
    }

    /// Guides snappy-java's SnappyLoader to the native library we already loaded via
    /// `System.load`. snappy-java has no public "assumeLoaded" API, so we set
    /// the `org.xerial.snappy.lib.path` / `org.xerial.snappy.lib.name`
    /// system properties that its `findNativeLibrary()` checks, causing its own
    /// loader to call `System.load` on the same file (a no-op) rather than
    /// attempting JAR extraction (which fails in native images).
    private static void assumeSnappyLoaded() {
        Path libDir = resolveLibDir();
        if (libDir == null || !Files.isDirectory(libDir)) {
            return;
        }
        System.setProperty("org.xerial.snappy.lib.path", libDir.toString());
        System.setProperty("org.xerial.snappy.lib.name", "snappyjava");
    }

    private static void assumeZstdLoaded() {
        try {
            Class<?> nativeClass = Class.forName("com.github.luben.zstd.util.Native");
            nativeClass.getMethod("assumeLoaded").invoke(null);
        }
        catch (ReflectiveOperationException e) {
            throw new LinkageError("Failed to tell zstd-jni the native library is loaded", e);
        }
    }
}
