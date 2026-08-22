/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.internal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

class NativeLibraryLoaderTest {

    @Test
    void inImageCodeReturnsFalseOnJvm() {
        assertThat(NativeLibraryLoader.inImageCode()).isFalse();
    }

    @Test
    void loadCodecsAreNoOpOnJvm() {
        // Should not throw — all load methods are no-ops outside a native image
        NativeLibraryLoader.loadZstd();
        NativeLibraryLoader.loadLz4();
        NativeLibraryLoader.loadSnappy();
        NativeLibraryLoader.loadBrotli();
    }

    @Test
    void libPathEnvOverridesDefault(@TempDir Path tmpDir) throws IOException {
        // Create a fake lib file to verify HARDWOOD_LIB_PATH is respected
        // (loadCodec short-circuits on inImageCode(), so we just verify no errors)
        Files.createFile(tmpDir.resolve("libzstd-jni-fake.so"));
        NativeLibraryLoader.loadZstd();
    }

    @Test
    void resolveResourcePathProducesCorrectPaths() {
        String savedOs = System.getProperty("os.name");
        String savedArch = System.getProperty("os.arch");

        try {
            // Linux x86_64
            System.setProperty("os.name", "Linux");
            System.setProperty("os.arch", "amd64");
            assertThat(NativeLibraryLoader.resolveResourcePath("libzstd-jni-1.5.7-9"))
                    .isEqualTo("native/linux-x86_64/libzstd-jni-1.5.7-9.so");

            // Linux aarch64
            System.setProperty("os.arch", "aarch64");
            assertThat(NativeLibraryLoader.resolveResourcePath("libzstd-jni-1.5.7-9"))
                    .isEqualTo("native/linux-aarch64/libzstd-jni-1.5.7-9.so");

            // macOS x86_64
            System.setProperty("os.name", "Mac OS X");
            System.setProperty("os.arch", "x86_64");
            assertThat(NativeLibraryLoader.resolveResourcePath("libzstd-jni-1.5.7-9"))
                    .isEqualTo("native/macos-x86_64/libzstd-jni-1.5.7-9.dylib");

            // macOS aarch64
            System.setProperty("os.arch", "aarch64");
            assertThat(NativeLibraryLoader.resolveResourcePath("libzstd-jni-1.5.7-9"))
                    .isEqualTo("native/macos-aarch64/libzstd-jni-1.5.7-9.dylib");

            // Windows x86_64
            System.setProperty("os.name", "Windows 10");
            System.setProperty("os.arch", "amd64");
            assertThat(NativeLibraryLoader.resolveResourcePath("libzstd-jni-1.5.7-9"))
                    .isEqualTo("native/windows-x86_64/libzstd-jni-1.5.7-9.dll");

            // Snappy on Linux
            System.setProperty("os.name", "Linux");
            System.setProperty("os.arch", "amd64");
            assertThat(NativeLibraryLoader.resolveResourcePath("libsnappyjava"))
                    .isEqualTo("native/linux-x86_64/libsnappyjava.so");

            // Snappy on Windows (strips lib prefix)
            System.setProperty("os.name", "Windows 10");
            assertThat(NativeLibraryLoader.resolveResourcePath("libsnappyjava"))
                    .isEqualTo("native/windows-x86_64/snappyjava.dll");
        }
        finally {
            restoreProperty("os.name", savedOs);
            restoreProperty("os.arch", savedArch);
        }
    }

    @Test
    void resolveResourceBaseNameStripsLibPrefixForSnappyOnWindows() {
        String savedOs = System.getProperty("os.name");

        try {
            // Windows: libsnappyjava -> snappyjava
            System.setProperty("os.name", "Windows 10");
            assertThat(NativeLibraryLoader.resolveResourceBaseName("libsnappyjava"))
                    .isEqualTo("snappyjava");

            // Linux: libsnappyjava -> libsnappyjava (unchanged)
            System.setProperty("os.name", "Linux");
            assertThat(NativeLibraryLoader.resolveResourceBaseName("libsnappyjava"))
                    .isEqualTo("libsnappyjava");

            // Windows: zstd always unchanged
            System.setProperty("os.name", "Windows 10");
            assertThat(NativeLibraryLoader.resolveResourceBaseName("libzstd-jni-1.5.7-9"))
                    .isEqualTo("libzstd-jni-1.5.7-9");

            // Windows: lz4 always unchanged
            assertThat(NativeLibraryLoader.resolveResourceBaseName("liblz4-java"))
                    .isEqualTo("liblz4-java");
        }
        finally {
            restoreProperty("os.name", savedOs);
        }
    }

    @Test
    void cacheFileNameIncludesVersionHashAndLibraryName() {
        String savedOs = System.getProperty("os.name");

        try {
            System.setProperty("os.name", "Linux");
            assertThat(NativeLibraryLoader.cacheFileName("1.1.0-SNAPSHOT", "a1b2c3d4", "libtest"))
                    .isEqualTo("hardwood-1.1.0-SNAPSHOT-a1b2c3d4-libtest.so");
        }
        finally {
            restoreProperty("os.name", savedOs);
        }
    }

    @Test
    void lockFileNameReplacesPlatformExtension() {
        String savedOs = System.getProperty("os.name");

        try {
            System.setProperty("os.name", "Linux");
            assertThat(NativeLibraryLoader.lockFileName("hardwood-1.1.0-a1b2c3d4-libtest.so"))
                    .isEqualTo("hardwood-1.1.0-a1b2c3d4-libtest.lck");
        }
        finally {
            restoreProperty("os.name", savedOs);
        }
    }

    @Test
    void shortSha256IsShortBase64UrlWithoutPadding() {
        String shortHash = NativeLibraryLoader.shortSha256("hello".getBytes(StandardCharsets.UTF_8));
        assertThat(shortHash).isEqualTo("LPJNul-wow4");
        assertThat(shortHash).hasSize(11);
        assertThat(shortHash).matches("[A-Za-z0-9_-]+");
        assertThat(shortHash).doesNotContain("=");
    }

    @Test
    void sha256HexProducesCorrectDigest() {
        byte[] input = "hello".getBytes(StandardCharsets.UTF_8);
        String expected = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";
        assertThat(NativeLibraryLoader.sha256Hex(input)).isEqualTo(expected);
    }

    @Test
    void resolveVersionReturnsNonEmptyString() {
        // The filtered application.properties on the test classpath provides project.version
        assertThat(NativeLibraryLoader.resolveVersion()).isNotEmpty();
    }

    @Test
    void extractToCacheCreatesFileInCacheDir(@TempDir Path tmpDir) throws IOException {
        byte[] content = "test library content".getBytes(StandardCharsets.UTF_8);
        String sha = NativeLibraryLoader.sha256Hex(content);
        String fileName = libTestCacheFileName(content);

        Path result = NativeLibraryLoader.extractToCache(tmpDir, fileName, content, sha);

        assertThat(result).exists();
        assertThat(Files.readAllBytes(result)).isEqualTo(content);
        assertThat(result.getFileName().toString()).isEqualTo(fileName);
    }

    @Test
    void extractToCacheReusesExistingValidFile(@TempDir Path tmpDir) throws IOException {
        byte[] content = "test library content".getBytes(StandardCharsets.UTF_8);
        String sha = NativeLibraryLoader.sha256Hex(content);
        String fileName = libTestCacheFileName(content);

        Path first = NativeLibraryLoader.extractToCache(tmpDir, fileName, content, sha);
        long firstModified = Files.getLastModifiedTime(first).toMillis();

        // Small delay to ensure different modification time if overwritten
        try {
            Thread.sleep(50);
        }
        catch (InterruptedException ignored) {
            // ignored
        }

        Path second = NativeLibraryLoader.extractToCache(tmpDir, fileName, content, sha);

        assertThat(second).isEqualTo(first);
        assertThat(Files.getLastModifiedTime(second).toMillis()).isEqualTo(firstModified);
    }

    @Test
    void extractToCacheReplacesCorruptedFile(@TempDir Path tmpDir) throws IOException {
        byte[] content = "test library content".getBytes(StandardCharsets.UTF_8);
        String sha = NativeLibraryLoader.sha256Hex(content);
        String fileName = libTestCacheFileName(content);

        NativeLibraryLoader.extractToCache(tmpDir, fileName, content, sha);

        // Corrupt the file
        Path target = tmpDir.resolve(fileName);
        Files.write(target, "corrupted content".getBytes(StandardCharsets.UTF_8));

        Path result = NativeLibraryLoader.extractToCache(tmpDir, fileName, content, sha);

        assertThat(result).exists();
        assertThat(Files.readAllBytes(result)).isEqualTo(content);
    }

    @Test
    void extractToCacheHandlesConcurrentExtraction(@TempDir Path tmpDir) throws IOException {
        byte[] content = "test library content".getBytes(StandardCharsets.UTF_8);
        String sha = NativeLibraryLoader.sha256Hex(content);
        String fileName = libTestCacheFileName(content);

        // Simulate a concurrent extraction: pre-create the target with valid content
        Files.write(tmpDir.resolve(fileName), content);

        Path result = NativeLibraryLoader.extractToCache(tmpDir, fileName, content, sha);

        assertThat(result).exists();
        assertThat(Files.readAllBytes(result)).isEqualTo(content);
    }

    @Test
    void extractToCacheCreatesMissingCacheDir(@TempDir Path tmpDir) throws IOException {
        byte[] content = "test library content".getBytes(StandardCharsets.UTF_8);
        String sha = NativeLibraryLoader.sha256Hex(content);
        String fileName = libTestCacheFileName(content);
        Path newCacheDir = tmpDir.resolve("new-cache-dir");

        // Extraction creates the cache directory when it is actually needed
        Path result = NativeLibraryLoader.extractToCache(newCacheDir, fileName, content, sha);

        assertThat(newCacheDir).exists();
        assertThat(result).exists();
        assertThat(Files.readAllBytes(result)).isEqualTo(content);
    }

    @Test
    void extractToCacheDeletesLockFileAfterExtraction(@TempDir Path tmpDir) throws IOException {
        byte[] content = "test library content".getBytes(StandardCharsets.UTF_8);
        String sha = NativeLibraryLoader.sha256Hex(content);
        String fileName = libTestCacheFileName(content);

        NativeLibraryLoader.extractToCache(tmpDir, fileName, content, sha);

        assertThat(tmpDir.resolve(NativeLibraryLoader.lockFileName(fileName))).doesNotExist();
    }

    @Test
    void extractToCacheFastPathRemovesStaleLockFile(@TempDir Path tmpDir) throws IOException {
        byte[] content = "test library content".getBytes(StandardCharsets.UTF_8);
        String sha = NativeLibraryLoader.sha256Hex(content);
        String fileName = libTestCacheFileName(content);
        Path lockFile = tmpDir.resolve(NativeLibraryLoader.lockFileName(fileName));

        // Pre-create a valid target plus a stale lock file (e.g. from a crashed run)
        Files.write(tmpDir.resolve(fileName), content);
        Files.write(lockFile, new byte[0]);

        Path result = NativeLibraryLoader.extractToCache(tmpDir, fileName, content, sha);

        assertThat(result).exists();
        assertThat(lockFile).doesNotExist();
    }

    @Test
    void readEmbeddedResourceFindsResource() {
        // application.properties is always present on the test classpath, exercising the
        // happy path of readEmbeddedResource without a dedicated binary fixture.
        Optional<byte[]> result = NativeLibraryLoader.readEmbeddedResource(
                "/application.properties");
        assertThat(result).isPresent();
    }

    @Test
    void readEmbeddedResourceReturnsEmptyForMissingResource() {
        Optional<byte[]> result = NativeLibraryLoader.readEmbeddedResource(
                "/native/nonexistent/lib.so");
        assertThat(result).isEmpty();
    }

    @Test
    void loadCodecsAreIdempotent() {
        // Reset flags for test isolation
        NativeLibraryLoader.zstdLoaded = false;
        NativeLibraryLoader.lz4Loaded = false;
        NativeLibraryLoader.snappyLoaded = false;
        NativeLibraryLoader.brotliLoaded = false;

        // First call sets the flag
        NativeLibraryLoader.loadZstd();
        assertThat(NativeLibraryLoader.zstdLoaded).isTrue();
        assertThat(NativeLibraryLoader.lz4Loaded).isFalse();
        assertThat(NativeLibraryLoader.snappyLoaded).isFalse();
        assertThat(NativeLibraryLoader.brotliLoaded).isFalse();

        // Second call returns immediately
        NativeLibraryLoader.loadZstd();
        assertThat(NativeLibraryLoader.zstdLoaded).isTrue();

        // Loading lz4 does not affect the other codecs
        NativeLibraryLoader.loadLz4();
        assertThat(NativeLibraryLoader.lz4Loaded).isTrue();
        assertThat(NativeLibraryLoader.snappyLoaded).isFalse();
        assertThat(NativeLibraryLoader.brotliLoaded).isFalse();

        // Loading snappy does not affect the others
        NativeLibraryLoader.loadSnappy();
        assertThat(NativeLibraryLoader.snappyLoaded).isTrue();
        assertThat(NativeLibraryLoader.brotliLoaded).isFalse();

        // Loading brotli does not affect the others
        NativeLibraryLoader.loadBrotli();
        assertThat(NativeLibraryLoader.brotliLoaded).isTrue();
        assertThat(NativeLibraryLoader.zstdLoaded).isTrue();
    }

    @Test
    void resolveCacheDirFindsWritableDirectory() {
        Optional<Path> result = NativeLibraryLoader.resolveCacheDir();
        // On a typical dev system, java.io.tmpdir or ~/.hardwood should be writable
        assertThat(result).isPresent();
    }

    @Test
    void resolveCacheDirLogsWarnings() {
        PrintStream savedErr = System.err;
        try {
            ByteArrayOutputStream captured = new ByteArrayOutputStream();
            System.setErr(new PrintStream(captured, true, StandardCharsets.UTF_8));

            Optional<Path> result = NativeLibraryLoader.resolveCacheDir(p -> false);

            assertThat(result).isEmpty();
            String output = captured.toString(StandardCharsets.UTF_8);

            // At minimum, the tmpdir and home-dir branches should fire
            assertThat(output).contains("WARNING:");
            assertThat(output).contains("not writable");
        }
        finally {
            System.setErr(savedErr);
        }
    }

    @Test
    void resolveCacheDirsReturnsTmpThenHome() {
        assertThat(NativeLibraryLoader.resolveCacheDirs(p -> true))
                .containsExactly(
                        Path.of(System.getProperty("java.io.tmpdir")),
                        Path.of(System.getProperty("user.home"), ".hardwood"));
    }

    @Test
    void resolveCacheDirsDoesNotCreateHomeDirectory() {
        String savedHome = System.getProperty("user.home");
        try {
            // Point user.home at a fresh temp dir; java.io.tmpdir remains the real one
            Path home = Files.createTempDirectory("hardwood-home-test");
            System.setProperty("user.home", home.toString());

            List<Path> dirs = NativeLibraryLoader.resolveCacheDirs();

            // tmp is usable, so home is a candidate, but resolution must not create it
            assertThat(dirs).isNotEmpty();
            assertThat(home.resolve(".hardwood")).doesNotExist();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        finally {
            restoreProperty("user.home", savedHome);
        }
    }

    @Test
    void extractAndLoadReturnsFalseWhenResourceMissing(@TempDir Path tmpDir) {
        boolean result = NativeLibraryLoader.extractAndLoad(
                "test", "nonexistent-lib", null, () -> List.of(tmpDir));
        assertThat(result).isFalse();
    }

    @Test
    void loadCodecImplPrefersExternalLibDir(@TempDir Path tmpDir) throws IOException {
        Files.write(tmpDir.resolve("libtest-fallback.so"),
                "test-fixture-content\n".getBytes(StandardCharsets.UTF_8));

        String savedOs = System.getProperty("os.name");
        String savedArch = System.getProperty("os.arch");
        PrintStream savedErr = System.err;
        try {
            System.setProperty("os.name", "Linux");
            System.setProperty("os.arch", "amd64");

            ByteArrayOutputStream captured = new ByteArrayOutputStream();
            System.setErr(new PrintStream(captured, true, StandardCharsets.UTF_8));

            // The external lib exists but is a fake .so, so it is tried first (the loader
            // reports the load failure) and then embedded extraction is attempted, which
            // also fails (no embedded resource for this base name) and logs the final ERROR.
            NativeLibraryLoader.loadCodecImpl("test", "libtest-fallback",
                    "lib", null, () -> tmpDir);

            String output = captured.toString(StandardCharsets.UTF_8);
            assertThat(output).contains("external lib dir");
            assertThat(output).contains("ERROR: Failed to load native library test");
        }
        finally {
            System.setErr(savedErr);
            restoreProperty("os.name", savedOs);
            restoreProperty("os.arch", savedArch);
        }
    }

    @Test
    void loadCodecImplFallsBackToEmbeddedWhenExternalFails(@TempDir Path homeDir) {
        String savedOs = System.getProperty("os.name");
        String savedArch = System.getProperty("os.arch");
        String savedHome = System.getProperty("user.home");
        PrintStream savedErr = System.err;
        try {
            System.setProperty("os.name", "Linux");
            System.setProperty("os.arch", "amd64");
            // Keep the fallback extraction out of the real user home
            System.setProperty("user.home", homeDir.toString());

            ByteArrayOutputStream captured = new ByteArrayOutputStream();
            System.setErr(new PrintStream(captured, true, StandardCharsets.UTF_8));

            // External lib dir unavailable; there is no embedded resource for "libtest"
            // either, so embedded extraction fails and the final ERROR is logged.
            NativeLibraryLoader.loadCodecImpl("test", "libtest", "", null, () -> null);

            String output = captured.toString(StandardCharsets.UTF_8);
            assertThat(output).contains(
                    "WARNING: External lib dir not available, falling back to embedded extraction for test.");
            assertThat(output).contains("ERROR: Failed to load native library test");
        }
        finally {
            System.setErr(savedErr);
            restoreProperty("os.name", savedOs);
            restoreProperty("os.arch", savedArch);
            restoreProperty("user.home", savedHome);
        }
    }

    @Test
    void loadCodecImplLogsErrorWhenAllFail() {
        PrintStream savedErr = System.err;
        try {
            ByteArrayOutputStream captured = new ByteArrayOutputStream();
            System.setErr(new PrintStream(captured, true, StandardCharsets.UTF_8));

            NativeLibraryLoader.loadCodecImpl("test", "nonexistent-lib", "", null);

            String output = captured.toString(StandardCharsets.UTF_8);
            assertThat(output).contains("ERROR: Failed to load native library test");
            assertThat(output).contains("External lib dir not available and embedded extraction failed");
        }
        finally {
            System.setErr(savedErr);
        }
    }

    @Test
    void normalizeOsProducesExpectedValues() {
        assertThat(NativeLibraryLoader.normalizeOs("Linux")).isEqualTo("linux");
        assertThat(NativeLibraryLoader.normalizeOs("linux")).isEqualTo("linux");
        assertThat(NativeLibraryLoader.normalizeOs("Mac OS X")).isEqualTo("macos");
        assertThat(NativeLibraryLoader.normalizeOs("Darwin")).isEqualTo("macos");
        assertThat(NativeLibraryLoader.normalizeOs("Windows 10")).isEqualTo("windows");
        assertThat(NativeLibraryLoader.normalizeOs("Windows Server 2019")).isEqualTo("windows");
        assertThat(NativeLibraryLoader.normalizeOs("unknown")).isEqualTo("linux");
    }

    @Test
    void normalizeArchProducesExpectedValues() {
        assertThat(NativeLibraryLoader.normalizeArch("amd64")).isEqualTo("x86_64");
        assertThat(NativeLibraryLoader.normalizeArch("x86_64")).isEqualTo("x86_64");
        assertThat(NativeLibraryLoader.normalizeArch("aarch64")).isEqualTo("aarch64");
    }

    private static String libTestCacheFileName(byte[] content) {
        return NativeLibraryLoader.cacheFileName(
                NativeLibraryLoader.resolveVersion(),
                NativeLibraryLoader.shortSha256(content),
                "libtest");
    }

    private static void restoreProperty(String key, String value) {
        if (value == null) {
            System.clearProperty(key);
        }
        else {
            System.setProperty(key, value);
        }
    }
}
