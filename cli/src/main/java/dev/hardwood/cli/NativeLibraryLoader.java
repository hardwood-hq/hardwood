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
    private static final String OS_NAME_PROP = "os.name";
    private static final String OS_ARCH_PROP = "os.arch";
    private static final String ARCH_AARCH64 = "aarch64";
    private static final String ARCH_ARM64 = "arm64";
    private static final String ARCH_X86_64 = "x86_64";
    private static final String ARCH_AMD64 = "amd64";
    private static final String ARCH_X86 = "x86";
    private static final String ARCH_I386 = "i386";

    private enum OsFamily { DARWIN, LINUX, WINDOWS }

    enum Codec { ZSTD, LZ4, SNAPPY }

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

    /**
     * Loads zstd-jni native library. No-op on JVM (zstd-jni loads from the JAR).
     */
    public static void loadZstd() {
        loadCodec("zstd", Codec.ZSTD, "libzstd-jni-" + ZSTD_JNI_VERSION, "libzstd-jni-",
                NativeLibraryLoader::assumeZstdLoaded);
    }

    /**
     * Loads lz4-java native library. No-op on JVM (lz4-java loads from the JAR).
     */
    public static void loadLz4() {
        loadCodec("lz4", Codec.LZ4, "liblz4-java", "liblz4-java", null);
    }

    /**
     * Loads snappy-java native library. No-op on JVM (snappy-java loads from the JAR).
     */
    public static void loadSnappy() {
        loadCodec("snappy", Codec.SNAPPY, "libsnappyjava", "libsnappyjava", NativeLibraryLoader::assumeSnappyLoaded);
    }

    /** Returns {@code libDir} on successful load, {@code null} otherwise. */
    private static Path loadCodec(String name, Codec codec, String exactBaseName, String scanPrefix, Runnable postLoad) {
        if (!inImageCode()) {
            return null;
        }
        Path libDir = resolveLibDir();
        if (libDir == null) {
            return null;
        }
        return loadNative(name, resolveLibFile(libDir, osArchFragment(codec), exactBaseName, scanPrefix))
                ? libDir
                : null;
    }

    private static boolean loadNative(String name, Path libFile) {
        if (libFile == null || !Files.isRegularFile(libFile)) {
            return false;
        }
        try {
            System.load(libFile.toAbsolutePath().toString());
            return true;
        }
        catch (UnsatisfiedLinkError e) {
            System.err.println("WARNING: Could not load " + name + " native library from " + libFile + ": " + e.getMessage());
            return false;
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

    /**
     * Resolves a platform-specific native library file within {@code libDir}.
     *
     * @param fragment      OS/arch path fragment (e.g. {@code darwin/aarch64})
     * @param exactBaseName file base name to try first (without extension)
     * @param scanPrefix    prefix used as a fallback when scanning the directory
     */
    private static Path resolveLibFile(Path libDir, String fragment, String exactBaseName, String scanPrefix) {
        if (fragment == null) {
            return null;
        }
        Path platformDir = libDir.resolve(fragment);
        if (!Files.isDirectory(platformDir)) {
            return null;
        }
        String ext = nativeLibExtension();
        Path exact = platformDir.resolve(exactBaseName + ext);
        if (Files.isRegularFile(exact)) {
            return exact;
        }
        try (Stream<Path> list = Files.list(platformDir)) {
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

    private static OsFamily detectOsFamily() {
        String os = System.getProperty(OS_NAME_PROP, "").toLowerCase(Locale.ROOT);
        if (os.contains("darwin") || os.contains("mac")) {
            return OsFamily.DARWIN;
        }
        if (os.contains("linux")) {
            return OsFamily.LINUX;
        }
        if (os.contains("windows")) {
            return OsFamily.WINDOWS;
        }
        return null;
    }

    private static boolean isAarch64(String arch) {
        return arch.equals(ARCH_AARCH64) || arch.equals(ARCH_ARM64);
    }

    private static boolean isX86_64(String arch) {
        return arch.equals(ARCH_X86_64) || arch.equals(ARCH_AMD64);
    }

    /**
     * Returns the OS/arch path fragment used to locate the given codec's JNI native library
     * under {@code lib/} (e.g. {@code darwin/aarch64}, {@code linux/amd64}).
     */
    static String osArchFragment(Codec codec) {
        OsFamily osFamily = detectOsFamily();
        if (osFamily == null) {
            return null;
        }
        String osPart = switch (codec) {
            case ZSTD -> switch (osFamily) {
                case DARWIN  -> "darwin";
                case LINUX   -> "linux";
                case WINDOWS -> "win";
            };
            case LZ4 -> switch (osFamily) {
                case DARWIN  -> "darwin";
                case LINUX   -> "linux";
                case WINDOWS -> "win32";
            };
            case SNAPPY -> switch (osFamily) {
                case DARWIN  -> "Mac";
                case LINUX   -> "Linux";
                case WINDOWS -> "Windows";
            };
        };
        String arch = System.getProperty(OS_ARCH_PROP, "").toLowerCase(Locale.ROOT);
        String archPart;
        if (isAarch64(arch)) {
            archPart = ARCH_AARCH64;
        }
        else if (isX86_64(arch)) {
            archPart = (codec != Codec.SNAPPY && osFamily != OsFamily.DARWIN) ? ARCH_AMD64 : ARCH_X86_64;
        }
        else if (codec == Codec.SNAPPY && (arch.equals(ARCH_X86) || arch.equals(ARCH_I386))) {
            archPart = ARCH_X86;
        }
        else {
            archPart = arch;
        }
        return osPart + File.separator + archPart;
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

    /**
     * Guides snappy-java's SnappyLoader to the native library we already loaded via
     * {@link System#load}. snappy-java has no public "assumeLoaded" API, so we set
     * the {@code org.xerial.snappy.lib.path} / {@code org.xerial.snappy.lib.name}
     * system properties that its {@code findNativeLibrary()} checks, causing its own
     * loader to call {@code System.load} on the same file (a no-op) rather than
     * attempting JAR extraction (which fails in native images).
     */
    private static void assumeSnappyLoaded() {
        Path libDir = resolveLibDir();
        if (libDir == null) {
            return;
        }
        String fragment = osArchFragment(Codec.SNAPPY);
        if (fragment == null) {
            return;
        }
        Path snappyDir = libDir.resolve(fragment);
        if (!Files.isDirectory(snappyDir)) {
            return;
        }
        System.setProperty("org.xerial.snappy.lib.path", snappyDir.toString());
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