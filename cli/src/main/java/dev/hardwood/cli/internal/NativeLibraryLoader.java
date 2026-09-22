/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.internal;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

/// Loads the compression native libraries (zstd-jni, snappy-java, lz4-java, brotli4j) when
/// running as a GraalVM native image.
///
/// The libraries are embedded in the image as resources under `native/<os>-<arch>/`. Each one
/// is extracted into a private cache directory and loaded via `System.load()`. The cache
/// directories, in order, are `<java.io.tmpdir>/hardwood-<user>/` and `~/.hardwood/`; the
/// second is used when a library in the first cannot be loaded (e.g. `/tmp` mounted
/// `noexec`). A cache directory is used only when it is not a symbolic link and neither group
/// nor others can write to it, so no other local user can replace a library between its
/// verification and its loading. Cache files are named
/// `hardwood-<shortHash>-<library><ext>`, so they are shared by every Hardwood version embedding
/// the same library, and their SHA-256 is verified before each use.
///
/// `HARDWOOD_LIB_PATH` names a directory whose libraries are loaded instead of the embedded
/// ones.
public final class NativeLibraryLoader {

    private static final String OS_NAME_PROP = "os.name";
    private static final Set<PosixFilePermission> OWNER_ONLY = PosixFilePermissions.fromString("rwx------");

    static volatile boolean zstdLoaded;
    static volatile boolean lz4Loaded;
    static volatile boolean snappyLoaded;
    static volatile boolean brotliLoaded;

    private NativeLibraryLoader() {
    }

    /// Loads a native library file, throwing [UnsatisfiedLinkError] if that fails. Tests
    /// substitute it to simulate `System.load()` outcomes without a real native library.
    @FunctionalInterface
    interface LibraryLoader {
        void load(Path libFile);
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
        if (zstdLoaded) {
            return;
        }
        zstdLoaded = true;
        loadCodec("zstd", "libzstd-jni", path -> assumeZstdLoaded());
    }

    /// Loads lz4-java native library. No-op on JVM (lz4-java loads from the JAR).
    public static void loadLz4() {
        if (lz4Loaded) {
            return;
        }
        lz4Loaded = true;
        loadCodec("lz4", "liblz4-java", null);
    }

    /// Loads snappy-java native library. No-op on JVM (snappy-java loads from the JAR).
    public static void loadSnappy() {
        if (snappyLoaded) {
            return;
        }
        snappyLoaded = true;
        loadCodec("snappy", "libsnappyjava", NativeLibraryLoader::assumeSnappyLoaded);
    }

    /// Loads brotli4j native library. No-op on JVM (brotli4j loads from the JAR).
    public static void loadBrotli() {
        if (brotliLoaded) {
            return;
        }
        brotliLoaded = true;
        loadCodec("brotli", "libbrotli", NativeLibraryLoader::assumeBrotliLoaded);
    }

    /// @param name     human-readable library name for messages
    /// @param baseName platform-independent file base name, e.g. `libzstd-jni`
    /// @param postLoad callback invoked with the loaded file after a successful `System.load()`, or `null`
    private static void loadCodec(String name, String baseName, Consumer<Path> postLoad) {
        if (!inImageCode()) {
            return;
        }
        LibraryLoader loader = libFile -> loadNative(libFile, postLoad);
        List<String> problems = new ArrayList<>();
        if (!load(name, baseName, externalLibDir(), resolveCacheDirs(), loader, problems)) {
            for (String problem : problems) {
                System.err.println("WARNING: " + problem);
            }
            System.err.println("WARNING: Could not load the " + name + " native library; files compressed with "
                    + name + " cannot be read.");
        }
    }

    /// Loads a library from `externalLibDir` when given and it holds the library, otherwise
    /// from the embedded resource.
    ///
    /// @param problems receives a description of each failed attempt; reported only when no
    ///                 attempt succeeds, so a working fallback stays silent
    /// @return `true` if the library was loaded
    static boolean load(String name, String baseName, Path externalLibDir, List<Path> cacheDirs, LibraryLoader loader,
            List<String> problems) {
        if (externalLibDir != null) {
            Path libFile = resolveLibFile(externalLibDir, baseName);
            if (libFile != null) {
                return tryLoad(name, libFile, loader, problems);
            }
            System.err.println("WARNING: No " + name + " native library in HARDWOOD_LIB_PATH directory "
                    + externalLibDir + "; using the embedded one.");
        }

        String resource = resourcePath(baseName);
        Optional<byte[]> bytes;
        try {
            bytes = readEmbeddedResource(resource);
        }
        catch (IOException e) {
            problems.add("Could not read embedded native library " + resource + ": " + e.getMessage());
            return false;
        }
        if (bytes.isEmpty()) {
            problems.add("Embedded native library not found: " + resource);
            return false;
        }
        return loadEmbedded(name, baseName, bytes.get(), cacheDirs, loader, problems);
    }

    /// Extracts `libBytes` into the first cache directory where the result can be loaded, and
    /// loads it. A directory that already holds the library from an earlier run is tried first:
    /// that run could not load it from the directories before it.
    ///
    /// @return `true` if the library was loaded
    static boolean loadEmbedded(String name, String baseName, byte[] libBytes, List<Path> cacheDirs,
            LibraryLoader loader, List<String> problems) {
        byte[] digest = sha256(libBytes);
        String sha256Hex = HexFormat.of().formatHex(digest);
        String fileName = cacheFileName(shortHash(digest), baseName);

        for (Path cacheDir : existingCopyFirst(cacheDirs, fileName)) {
            if (!ensurePrivateDirectory(cacheDir)) {
                problems.add("Not using " + cacheDir
                        + " for native libraries: it is a symbolic link, not writable, or writable by other users.");
                continue;
            }
            Path target;
            try {
                target = extractToCache(cacheDir, fileName, libBytes, sha256Hex);
            }
            catch (IOException e) {
                problems.add("Could not extract the " + name + " native library to " + cacheDir + ": "
                        + e.getMessage());
                continue;
            }
            if (tryLoad(name, target, loader, problems)) {
                return true;
            }
            deleteIfExistsBestEffort(target);
        }
        return false;
    }

    private static List<Path> existingCopyFirst(List<Path> cacheDirs, String fileName) {
        List<Path> ordered = new ArrayList<>(cacheDirs.size());
        for (Path dir : cacheDirs) {
            if (Files.isRegularFile(dir.resolve(fileName), LinkOption.NOFOLLOW_LINKS)) {
                ordered.add(dir);
            }
        }
        for (Path dir : cacheDirs) {
            if (!ordered.contains(dir)) {
                ordered.add(dir);
            }
        }
        return ordered;
    }

    private static boolean tryLoad(String name, Path libFile, LibraryLoader loader, List<String> problems) {
        try {
            loader.load(libFile);
            return true;
        }
        catch (UnsatisfiedLinkError e) {
            problems.add("Could not load " + name + " native library from " + libFile + ": " + e.getMessage());
            return false;
        }
    }

    private static void loadNative(Path libFile, Consumer<Path> postLoad) {
        System.load(libFile.toAbsolutePath().toString());
        if (postLoad != null) {
            postLoad.accept(libFile);
        }
    }

    private static void deleteIfExistsBestEffort(Path path) {
        try {
            Files.deleteIfExists(path);
        }
        catch (IOException ignored) {
            // best effort; the next run verifies the hash before using the file
        }
    }

    private static Path externalLibDir() {
        String env = System.getenv("HARDWOOD_LIB_PATH");
        if (env == null || env.isBlank()) {
            return null;
        }
        Path dir = Path.of(env.trim());
        if (!Files.isDirectory(dir)) {
            System.err.println("WARNING: HARDWOOD_LIB_PATH is set but not a directory: " + dir);
            return null;
        }
        return dir;
    }

    /// Resolves a native library file within `libDir`: `<baseName><ext>` if present, otherwise
    /// the first file whose name starts with `baseName`, or with `baseName` minus its `lib`
    /// prefix (the Windows naming of snappy-java and brotli4j), and ends with the platform
    /// extension. Covers version-suffixed names such as `libzstd-jni-1.5.7-9.so`.
    static Path resolveLibFile(Path libDir, String baseName) {
        String ext = nativeLibExtension();
        Path exact = libDir.resolve(baseName + ext);
        if (Files.isRegularFile(exact)) {
            return exact;
        }
        String unprefixed = baseName.startsWith("lib") ? baseName.substring(3) : baseName;
        try (Stream<Path> list = Files.list(libDir)) {
            return list
                    .filter(Files::isRegularFile)
                    .filter(p -> {
                        String fileName = p.getFileName().toString();
                        return (fileName.startsWith(baseName) || fileName.startsWith(unprefixed)) && fileName.endsWith(ext);
                    })
                    .sorted()
                    .findFirst()
                    .orElse(null);
        }
        catch (IOException e) {
            return null;
        }
    }

    static String nativeLibExtension() {
        String os = System.getProperty(OS_NAME_PROP, "").toLowerCase(Locale.ROOT);
        if (os.contains("windows")) {
            return ".dll";
        }
        if (os.contains("mac") || os.contains("darwin")) {
            return ".dylib";
        }
        return ".so";
    }

    /// Builds the cache file name for an embedded library:
    /// `hardwood-<shortHash>-<baseName><nativeLibExtension()>`.
    static String cacheFileName(String shortHash, String baseName) {
        return "hardwood-" + shortHash + "-" + baseName + nativeLibExtension();
    }

    /// Resolves the resource path of an embedded native library. The build stages each
    /// library under its platform-independent base name.
    static String resourcePath(String baseName) {
        String os = normalizeOs(System.getProperty(OS_NAME_PROP, ""));
        String arch = normalizeArch(System.getProperty("os.arch", ""));
        return "native/" + os + "-" + arch + "/" + baseName + nativeLibExtension();
    }

    /// Maps the `os.name` system property to a canonical OS identifier.
    static String normalizeOs(String osName) {
        String lower = osName.toLowerCase(Locale.ROOT);
        if (lower.contains("windows")) {
            return "windows";
        }
        if (lower.contains("mac") || lower.contains("darwin")) {
            return "macos";
        }
        return "linux";
    }

    /// Maps the `os.arch` system property to a canonical architecture identifier.
    static String normalizeArch(String osArch) {
        if ("amd64".equals(osArch) || "x86_64".equals(osArch)) {
            return "x86_64";
        }
        return "aarch64";
    }

    /// Reads an embedded classpath resource, given as a path relative to the classpath root.
    ///
    /// @return the resource bytes, or empty if there is no such resource
    static Optional<byte[]> readEmbeddedResource(String resourcePath) throws IOException {
        try (InputStream is = NativeLibraryLoader.class.getResourceAsStream("/" + resourcePath)) {
            return is == null ? Optional.empty() : Optional.of(is.readAllBytes());
        }
    }

    static byte[] sha256(byte[] data) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(data);
        }
        catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    /// Returns the first 8 bytes of a digest, Base64 URL-encoded without padding.
    static String shortHash(byte[] digest) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(Arrays.copyOf(digest, 8));
    }

    /// Returns the candidate cache directories in order: `<java.io.tmpdir>/hardwood-<user>/`,
    /// then `~/.hardwood/`. Neither is created here; see [#ensurePrivateDirectory(Path)].
    static List<Path> resolveCacheDirs() {
        List<Path> dirs = new ArrayList<>(2);
        String tmpDir = System.getProperty("java.io.tmpdir");
        if (tmpDir != null && !tmpDir.isBlank()) {
            String user = System.getProperty("user.name", "").replaceAll("[^A-Za-z0-9._-]", "_");
            dirs.add(Path.of(tmpDir, "hardwood-" + user));
        }
        String home = System.getProperty("user.home");
        if (home != null && !home.isBlank()) {
            dirs.add(Path.of(home, ".hardwood"));
        }
        return dirs;
    }

    /// Creates `dir` with owner-only permissions if it is missing, and reports whether it is
    /// safe to load libraries from: a real directory (not a symbolic link) that the current
    /// user can write to and, on POSIX file systems, neither group nor others can write to.
    /// Together these mean only the current user (or root) can have placed files in it.
    static boolean ensurePrivateDirectory(Path dir) {
        try {
            if (!Files.exists(dir, LinkOption.NOFOLLOW_LINKS)) {
                Files.createDirectories(dir.getParent());
                if (supportsPosix(dir.getParent())) {
                    Files.createDirectory(dir, PosixFilePermissions.asFileAttribute(OWNER_ONLY));
                }
                else {
                    Files.createDirectory(dir);
                }
            }
        }
        catch (IOException e) {
            // Includes losing a creation race to another process; the checks below decide.
        }
        if (!Files.isDirectory(dir, LinkOption.NOFOLLOW_LINKS) || !Files.isWritable(dir)) {
            return false;
        }
        if (!supportsPosix(dir)) {
            return true;
        }
        try {
            Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(dir, LinkOption.NOFOLLOW_LINKS);
            return !permissions.contains(PosixFilePermission.GROUP_WRITE)
                    && !permissions.contains(PosixFilePermission.OTHERS_WRITE);
        }
        catch (IOException e) {
            return false;
        }
    }

    private static boolean supportsPosix(Path path) {
        return path.getFileSystem().supportedFileAttributeViews().contains("posix")
                && Files.getFileAttributeView(path, PosixFileAttributeView.class, LinkOption.NOFOLLOW_LINKS) != null;
    }

    /// Writes the library into `cacheDir` as `fileName`, unless a file with the expected
    /// SHA-256 is already there. The bytes go to a uniquely named temporary file that is then
    /// atomically renamed onto `fileName`, so concurrent processes never see a partial file.
    static Path extractToCache(Path cacheDir, String fileName, byte[] libBytes, String expectedSha256)
            throws IOException {
        Path target = cacheDir.resolve(fileName);
        if (hasContent(target, expectedSha256)) {
            return target;
        }
        Path temp = Files.createTempFile(cacheDir, fileName, ".tmp");
        try {
            Files.write(temp, libBytes);
            Files.move(temp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        }
        catch (IOException e) {
            Files.deleteIfExists(temp);
            throw e;
        }
        return target;
    }

    private static boolean hasContent(Path file, String expectedSha256) {
        if (!Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS)) {
            return false;
        }
        try {
            return HexFormat.of().formatHex(sha256(Files.readAllBytes(file))).equals(expectedSha256);
        }
        catch (IOException e) {
            return false;
        }
    }

    /// Guides brotli4j's Brotli4jLoader to the native library we already loaded via
    /// `System.load`. When the `brotli4j.library.path` property is set, brotli4j's loader
    /// calls `System.load` on that path (a no-op for an already-loaded file) and skips its
    /// own temp-file extraction.
    private static void assumeBrotliLoaded(Path libPath) {
        System.setProperty("brotli4j.library.path", libPath.toAbsolutePath().toString());
    }

    /// Guides snappy-java's SnappyLoader to the native library we already loaded via
    /// `System.load`. snappy-java has no public "assumeLoaded" API, so we set
    /// the `org.xerial.snappy.lib.path` / `org.xerial.snappy.lib.name`
    /// system properties that its `findNativeLibrary()` checks, causing its own
    /// loader to call `System.load` on the same file (a no-op) rather than
    /// attempting JAR extraction (which fails in native images). The lib name must
    /// be the full file name (e.g. `hardwood-<shortHash>-libsnappyjava.so`)
    /// so that `findNativeLibrary()` resolves `new File(libPath, libName)`.
    private static void assumeSnappyLoaded(Path libPath) {
        Path absolute = libPath.toAbsolutePath();
        System.setProperty("org.xerial.snappy.lib.path", absolute.getParent().toString());
        System.setProperty("org.xerial.snappy.lib.name", absolute.getFileName().toString());
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
