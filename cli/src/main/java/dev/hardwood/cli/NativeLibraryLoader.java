/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.CodeSource;
import java.util.Locale;
import java.util.stream.Stream;

/**
 * Loads compression native libraries (zstd-jni, snappy-java, lz4-java, brotli4j) when running as a
 * GraalVM native image. Libraries must be placed in a directory next to the executable (e.g. lib/...)
 * or pointed to by {@code HARDWOOD_LIB_PATH}.
 */
public final class NativeLibraryLoader {

    private static final String ZSTD_JNI_VERSION = "1.5.7-6";

    private NativeLibraryLoader() {
    }

    public static boolean inImageCode() {
        try {
            Class<?> c = Class.forName("org.graalvm.nativeimage.ImageInfo");
            return Boolean.TRUE.equals(c.getMethod("inImageCode").invoke(null));
        }
        catch (ReflectiveOperationException e) {
            return false;
        }
    }

    /**
     * Loads zstd-jni native library. No-op on JVM (zstd-jni loads from the JAR).
     */
    public static void loadZstd() {
        if (!inImageCode()) {
            return;
        }
        Path libDir = resolveLibDir();
        if (libDir == null) {
            return;
        }
        Path libFile = resolveZstdLibFile(libDir);
        if (libFile == null || !Files.isRegularFile(libFile)) {
            return;
        }
        try {
            System.load(libFile.toAbsolutePath().toString());
            assumeZstdLoaded();
        }
        catch (UnsatisfiedLinkError e) {
            System.err.println("WARNING: Could not load zstd native library from " + libFile + ": " + e.getMessage());
        }
    }

    /**
     * Loads lz4-java native library. No-op on JVM (lz4-java loads from the JAR).
     */
    public static void loadLz4() {
        if (!inImageCode()) {
            return;
        }
        Path libDir = resolveLibDir();
        if (libDir == null) {
            return;
        }
        Path libFile = resolveLz4LibFile(libDir);
        if (libFile == null || !Files.isRegularFile(libFile)) {
            return;
        }
        try {
            System.load(libFile.toAbsolutePath().toString());
        }
        catch (UnsatisfiedLinkError e) {
            System.err.println("WARNING: Could not load lz4 native library from " + libFile + ": " + e.getMessage());
        }
    }

    /**
     * Loads snappy-java native library. No-op on JVM (snappy-java loads from the JAR).
     */
    public static void loadSnappy() {
        if (!inImageCode()) {
            return;
        }
        Path libDir = resolveLibDir();
        if (libDir == null) {
            return;
        }
        Path libFile = resolveSnappyLibFile(libDir);
        if (libFile == null || !Files.isRegularFile(libFile)) {
            return;
        }
        try {
            System.load(libFile.toAbsolutePath().toString());
        }
        catch (UnsatisfiedLinkError e) {
            System.err.println("WARNING: Could not load snappy native library from " + libFile + ": " + e.getMessage());
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
        catch (java.net.URISyntaxException | NullPointerException e) {
            return null;
        }
    }

    /** zstd-jni JAR uses darwin/aarch64, darwin/x86_64, linux/amd64, linux/aarch64, win/amd64. */
    private static String zstdOsArchFragment() {
        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        String arch = System.getProperty("os.arch", "").toLowerCase(Locale.ROOT);
        String osPart = os.contains("mac") || os.contains("darwin") ? "darwin"
                : os.contains("linux") ? "linux"
                : os.contains("windows") ? "win"
                : null;
        if (osPart == null) {
            return null;
        }
        String archPart = arch.equals("aarch64") || arch.equals("arm64") ? "aarch64"
                : arch.equals("x86_64") ? (osPart.equals("linux") ? "amd64" : "x86_64")
                : arch.equals("amd64") ? "amd64"
                : arch;
        return osPart + File.separator + archPart;
    }

    /** lz4-java JAR uses darwin/aarch64, darwin/x86_64, linux/amd64, linux/aarch64, win32/amd64. */
    private static String lz4OsArchFragment() {
        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        String arch = System.getProperty("os.arch", "").toLowerCase(Locale.ROOT);
        String osPart = os.contains("mac") || os.contains("darwin") ? "darwin"
                : os.contains("linux") ? "linux"
                : os.contains("windows") ? "win32"
                : null;
        if (osPart == null) {
            return null;
        }
        String archPart = arch.equals("aarch64") || arch.equals("arm64") ? "aarch64"
                : arch.equals("x86_64") || arch.equals("amd64") ? (osPart.equals("linux") ? "amd64" : "x86_64")
                : arch;
        return osPart + File.separator + archPart;
    }

    /** snappy-java JAR uses Mac/aarch64, Linux/x86_64, Windows/x86_64. */
    private static String snappyOsArchFragment() {
        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        String arch = System.getProperty("os.arch", "").toLowerCase(Locale.ROOT);
        String osPart = os.contains("mac") || os.contains("darwin") ? "Mac"
                : os.contains("linux") ? "Linux"
                : os.contains("windows") ? "Windows"
                : null;
        if (osPart == null) {
            return null;
        }
        String archPart = arch.equals("aarch64") || arch.equals("arm64") ? "aarch64"
                : arch.equals("x86_64") || arch.equals("amd64") ? "x86_64"
                : arch.equals("x86") || arch.equals("i386") ? "x86"
                : arch;
        return osPart + File.separator + archPart;
    }

private static String nativeLibExtension() {
        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        if (os.contains("windows")) {
            return ".dll";
        }
        if (os.contains("mac") || os.contains("darwin")) {
            return ".dylib";
        }
        return ".so";
    }

    private static Path resolveZstdLibFile(Path libDir) {
        String fragment = zstdOsArchFragment();
        if (fragment == null) {
            return null;
        }
        Path platformDir = libDir.resolve(fragment);
        if (!Files.isDirectory(platformDir)) {
            return null;
        }
        String ext = nativeLibExtension();
        Path exact = platformDir.resolve("libzstd-jni-" + ZSTD_JNI_VERSION + ext);
        if (Files.isRegularFile(exact)) {
            return exact;
        }
        try (Stream<Path> list = Files.list(platformDir)) {
            return list
                    .filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().startsWith("libzstd-jni-") && p.getFileName().toString().endsWith(ext))
                    .findFirst()
                    .orElse(null);
        }
        catch (Exception e) {
            return null;
        }
    }

    private static Path resolveLz4LibFile(Path libDir) {
        String fragment = lz4OsArchFragment();
        if (fragment == null) {
            return null;
        }
        Path platformDir = libDir.resolve(fragment);
        if (!Files.isDirectory(platformDir)) {
            return null;
        }
        String ext = nativeLibExtension();
        Path exact = platformDir.resolve("liblz4-java" + ext);
        if (Files.isRegularFile(exact)) {
            return exact;
        }
        try (Stream<Path> list = Files.list(platformDir)) {
            return list
                    .filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().startsWith("liblz4-java") && p.getFileName().toString().endsWith(ext))
                    .findFirst()
                    .orElse(null);
        }
        catch (Exception e) {
            return null;
        }
    }

    private static Path resolveSnappyLibFile(Path libDir) {
        String fragment = snappyOsArchFragment();
        if (fragment == null) {
            return null;
        }
        Path platformDir = libDir.resolve(fragment);
        if (!Files.isDirectory(platformDir)) {
            return null;
        }
        String ext = nativeLibExtension();
        Path exact = platformDir.resolve("libsnappyjava" + ext);
        if (Files.isRegularFile(exact)) {
            return exact;
        }
        try (Stream<Path> list = Files.list(platformDir)) {
            return list
                    .filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().startsWith("libsnappyjava") && p.getFileName().toString().endsWith(ext))
                    .findFirst()
                    .orElse(null);
        }
        catch (Exception e) {
            return null;
        }
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
