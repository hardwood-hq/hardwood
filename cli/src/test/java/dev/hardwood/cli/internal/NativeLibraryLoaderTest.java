/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.internal;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

class NativeLibraryLoaderTest {

    private static final byte[] LIB_BYTES = "test library content".getBytes(StandardCharsets.UTF_8);

    private String savedOsName;
    private String savedOsArch;

    @BeforeEach
    void pinPlatform() {
        savedOsName = System.getProperty("os.name");
        savedOsArch = System.getProperty("os.arch");
        System.setProperty("os.name", "Linux");
        System.setProperty("os.arch", "amd64");
    }

    @AfterEach
    void restorePlatform() {
        restoreProperty("os.name", savedOsName);
        restoreProperty("os.arch", savedOsArch);
    }

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
    void resourcePathUsesPlatformIndependentBaseName() {
        assertThat(NativeLibraryLoader.resourcePath("libzstd-jni")).isEqualTo("native/linux-x86_64/libzstd-jni.so");

        System.setProperty("os.arch", "aarch64");
        assertThat(NativeLibraryLoader.resourcePath("libzstd-jni")).isEqualTo("native/linux-aarch64/libzstd-jni.so");

        System.setProperty("os.name", "Mac OS X");
        assertThat(NativeLibraryLoader.resourcePath("libsnappyjava")).isEqualTo("native/macos-aarch64/libsnappyjava.dylib");

        System.setProperty("os.name", "Windows 11");
        System.setProperty("os.arch", "amd64");
        assertThat(NativeLibraryLoader.resourcePath("libbrotli")).isEqualTo("native/windows-x86_64/libbrotli.dll");
    }

    @Test
    void cacheFileNameIncludesHashAndLibraryName() {
        assertThat(NativeLibraryLoader.cacheFileName("a1b2c3d4", "libtest"))
                .isEqualTo("hardwood-a1b2c3d4-libtest.so");
    }

    @Test
    void shortHashIsFirstEightDigestBytesAsUnpaddedBase64Url() {
        byte[] digest = NativeLibraryLoader.sha256("hello".getBytes(StandardCharsets.UTF_8));

        assertThat(HexFormat.of().formatHex(digest))
                .isEqualTo("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824");
        assertThat(NativeLibraryLoader.shortHash(digest)).isEqualTo("LPJNul-wow4");
    }

    @Test
    void readEmbeddedResourceReturnsEmptyForMissingResource() throws IOException {
        assertThat(NativeLibraryLoader.readEmbeddedResource("native/nonexistent/lib.so")).isEmpty();
    }

    @Test
    void resolveLibFileMatchesVersionedAndUnprefixedNames(@TempDir Path libDir) throws IOException {
        Files.createFile(libDir.resolve("libzstd-jni-1.5.7-9.so"));
        Files.createFile(libDir.resolve("snappyjava.so"));
        Files.createFile(libDir.resolve("liblz4-java.so"));

        assertThat(NativeLibraryLoader.resolveLibFile(libDir, "libzstd-jni")).isEqualTo(libDir.resolve("libzstd-jni-1.5.7-9.so"));
        assertThat(NativeLibraryLoader.resolveLibFile(libDir, "libsnappyjava")).isEqualTo(libDir.resolve("snappyjava.so"));
        assertThat(NativeLibraryLoader.resolveLibFile(libDir, "liblz4-java")).isEqualTo(libDir.resolve("liblz4-java.so"));
        assertThat(NativeLibraryLoader.resolveLibFile(libDir, "libbrotli")).isNull();
    }

    @Test
    void resolveCacheDirsReturnsPerUserTmpDirThenHome(@TempDir Path root) {
        String savedTmp = System.getProperty("java.io.tmpdir");
        String savedHome = System.getProperty("user.home");
        String savedUser = System.getProperty("user.name");
        try {
            System.setProperty("java.io.tmpdir", root.resolve("tmp").toString());
            System.setProperty("user.home", root.resolve("home").toString());
            System.setProperty("user.name", "DOMAIN\\alice smith");

            assertThat(NativeLibraryLoader.resolveCacheDirs()).containsExactly(
                    root.resolve("tmp").resolve("hardwood-DOMAIN_alice_smith"),
                    root.resolve("home").resolve(".hardwood"));
            assertThat(root.resolve("tmp")).doesNotExist();
            assertThat(root.resolve("home")).doesNotExist();
        }
        finally {
            restoreProperty("java.io.tmpdir", savedTmp);
            restoreProperty("user.home", savedHome);
            restoreProperty("user.name", savedUser);
        }
    }

    @Test
    void ensurePrivateDirectoryCreatesOwnerOnlyDirectory(@TempDir Path root) throws IOException {
        Path dir = root.resolve("hardwood-test");

        assertThat(NativeLibraryLoader.ensurePrivateDirectory(dir)).isTrue();
        assertThat(PosixFilePermissions.toString(Files.getPosixFilePermissions(dir))).isEqualTo("rwx------");
    }

    @Test
    void ensurePrivateDirectoryRejectsDirectoryWritableByOthers(@TempDir Path root) throws IOException {
        Path dir = Files.createDirectory(root.resolve("shared"));
        Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("rwxrwxrwx"));

        assertThat(NativeLibraryLoader.ensurePrivateDirectory(dir)).isFalse();
    }

    @Test
    void ensurePrivateDirectoryRejectsSymbolicLink(@TempDir Path root) throws IOException {
        Path target = Files.createDirectory(root.resolve("target"),
                PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
        Path link = Files.createSymbolicLink(root.resolve("link"), target);

        assertThat(NativeLibraryLoader.ensurePrivateDirectory(link)).isFalse();
    }

    @Test
    void extractToCacheWritesFileAndReusesIt(@TempDir Path cacheDir) throws IOException {
        String sha = sha256Hex(LIB_BYTES);

        Path first = NativeLibraryLoader.extractToCache(cacheDir, "lib.so", LIB_BYTES, sha);
        long firstModified = Files.getLastModifiedTime(first).toMillis();
        Path second = NativeLibraryLoader.extractToCache(cacheDir, "lib.so", LIB_BYTES, sha);

        assertThat(Files.readAllBytes(first)).isEqualTo(LIB_BYTES);
        assertThat(second).isEqualTo(first);
        assertThat(Files.getLastModifiedTime(second).toMillis()).isEqualTo(firstModified);
        try (Stream<Path> files = Files.list(cacheDir)) {
            assertThat(files).containsExactly(first);
        }
    }

    @Test
    void extractToCacheReplacesCorruptedFile(@TempDir Path cacheDir) throws IOException {
        Files.write(cacheDir.resolve("lib.so"), "corrupted".getBytes(StandardCharsets.UTF_8));

        Path result = NativeLibraryLoader.extractToCache(cacheDir, "lib.so", LIB_BYTES, sha256Hex(LIB_BYTES));

        assertThat(Files.readAllBytes(result)).isEqualTo(LIB_BYTES);
    }

    @Test
    void loadEmbeddedExtractsToFirstDirectoryOnly(@TempDir Path root) {
        Path tmp = root.resolve("tmp");
        Path home = root.resolve("home");
        List<Path> loaded = new ArrayList<>();
        List<String> problems = new ArrayList<>();

        boolean result = NativeLibraryLoader.loadEmbedded("test", "libtest", LIB_BYTES, List.of(tmp, home),
                recordingLoader(loaded, true), problems);

        assertThat(result).isTrue();
        assertThat(loaded).containsExactly(tmp.resolve(cacheFileName()));
        assertThat(problems).isEmpty();
        assertThat(home).doesNotExist();
    }

    @Test
    void loadEmbeddedFallsBackAndCleansUpWhenLoadFails(@TempDir Path root) {
        Path tmp = root.resolve("tmp");
        Path home = root.resolve("home");
        List<Path> loaded = new ArrayList<>();
        List<String> problems = new ArrayList<>();
        NativeLibraryLoader.LibraryLoader noexecTmp = libFile -> {
            loaded.add(libFile);
            if (libFile.startsWith(tmp)) {
                throw new UnsatisfiedLinkError("failed to map segment from shared object");
            }
        };

        boolean result = NativeLibraryLoader.loadEmbedded("test", "libtest", LIB_BYTES, List.of(tmp, home), noexecTmp,
                problems);

        assertThat(result).isTrue();
        assertThat(loaded).containsExactly(tmp.resolve(cacheFileName()), home.resolve(cacheFileName()));
        assertThat(problems).containsExactly("Could not load test native library from " + tmp.resolve(cacheFileName())
                + ": failed to map segment from shared object");
        assertThat(tmp.resolve(cacheFileName())).doesNotExist();
        assertThat(home.resolve(cacheFileName())).exists();
    }

    @Test
    void loadEmbeddedPrefersDirectoryHoldingAnEarlierCopy(@TempDir Path root) throws IOException {
        Path tmp = root.resolve("tmp");
        Path home = Files.createDirectory(root.resolve("home"),
                PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
        Files.write(home.resolve(cacheFileName()), LIB_BYTES);
        List<Path> loaded = new ArrayList<>();
        List<String> problems = new ArrayList<>();

        boolean result = NativeLibraryLoader.loadEmbedded("test", "libtest", LIB_BYTES, List.of(tmp, home),
                recordingLoader(loaded, true), problems);

        assertThat(result).isTrue();
        assertThat(loaded).containsExactly(home.resolve(cacheFileName()));
        assertThat(problems).isEmpty();
        assertThat(tmp).doesNotExist();
    }

    @Test
    void loadEmbeddedSkipsDirectoryWritableByOthers(@TempDir Path root) throws IOException {
        Path shared = Files.createDirectory(root.resolve("shared"));
        Files.setPosixFilePermissions(shared, PosixFilePermissions.fromString("rwxrwxrwx"));
        Files.write(shared.resolve(cacheFileName()), LIB_BYTES);
        Path home = root.resolve("home");
        List<Path> loaded = new ArrayList<>();
        List<String> problems = new ArrayList<>();

        boolean result = NativeLibraryLoader.loadEmbedded("test", "libtest", LIB_BYTES, List.of(shared, home),
                recordingLoader(loaded, true), problems);

        assertThat(result).isTrue();
        assertThat(loaded).containsExactly(home.resolve(cacheFileName()));
        assertThat(problems).containsExactly("Not using " + shared
                + " for native libraries: it is a symbolic link, not writable, or writable by other users.");
    }

    @Test
    void loadEmbeddedReturnsFalseWhenNoDirectoryWorks(@TempDir Path root) {
        List<Path> loaded = new ArrayList<>();
        List<String> problems = new ArrayList<>();

        boolean result = NativeLibraryLoader.loadEmbedded("test", "libtest", LIB_BYTES,
                List.of(root.resolve("a"), root.resolve("b")), recordingLoader(loaded, false), problems);

        assertThat(result).isFalse();
        assertThat(loaded).hasSize(2);
        assertThat(problems).containsExactly(
                "Could not load test native library from " + root.resolve("a").resolve(cacheFileName()) + ": rejected",
                "Could not load test native library from " + root.resolve("b").resolve(cacheFileName()) + ": rejected");
        assertThat(root.resolve("a").resolve(cacheFileName())).doesNotExist();
        assertThat(root.resolve("b").resolve(cacheFileName())).doesNotExist();
    }

    @Test
    void loadPrefersExternalLibDir(@TempDir Path root) throws IOException {
        Path libDir = Files.createDirectory(root.resolve("lib"));
        Path external = Files.createFile(libDir.resolve("libzstd-jni-1.5.7-9.so"));
        Path cacheDir = root.resolve("cache");
        List<Path> loaded = new ArrayList<>();
        List<String> problems = new ArrayList<>();

        boolean result = NativeLibraryLoader.load("zstd", "libzstd-jni", libDir, List.of(cacheDir),
                recordingLoader(loaded, true), problems);

        assertThat(result).isTrue();
        assertThat(loaded).containsExactly(external);
        assertThat(problems).isEmpty();
        assertThat(cacheDir).doesNotExist();
    }

    @Test
    void loadReportsMissingEmbeddedResource(@TempDir Path root) {
        List<Path> loaded = new ArrayList<>();
        List<String> problems = new ArrayList<>();

        boolean result = NativeLibraryLoader.load("test", "libnonexistent", null, List.of(root),
                recordingLoader(loaded, true), problems);

        assertThat(result).isFalse();
        assertThat(loaded).isEmpty();
        assertThat(problems).containsExactly("Embedded native library not found: native/linux-x86_64/libnonexistent.so");
    }

    private static NativeLibraryLoader.LibraryLoader recordingLoader(List<Path> loaded, boolean succeed) {
        return libFile -> {
            loaded.add(libFile);
            if (!succeed) {
                throw new UnsatisfiedLinkError("rejected");
            }
        };
    }

    private static String cacheFileName() {
        return NativeLibraryLoader.cacheFileName(
                NativeLibraryLoader.shortHash(NativeLibraryLoader.sha256(LIB_BYTES)), "libtest");
    }

    private static String sha256Hex(byte[] bytes) {
        return HexFormat.of().formatHex(NativeLibraryLoader.sha256(bytes));
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
