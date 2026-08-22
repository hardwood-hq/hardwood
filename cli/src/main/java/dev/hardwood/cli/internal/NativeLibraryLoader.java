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
import java.net.URISyntaxException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.CodeSource;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

/// Loads compression native libraries (zstd-jni, snappy-java, lz4-java) when running as a
/// GraalVM native image. The external `lib/` directory (via `HARDWOOD_LIB_PATH` or next to
/// the executable) is tried first. If unavailable, embedded resources are extracted to a
/// flat, content-addressed cache directly in `java.io.tmpdir` (fallback `~/.hardwood/`) and
/// loaded via `System.load()`. Cache files are named
/// `hardwood-<version>-<shortHash>-<library><ext>`.
public final class NativeLibraryLoader {

    private static final String ZSTD_JNI_VERSION = "1.5.7-9";
    private static final String OS_NAME_PROP = "os.name";

    static volatile boolean zstdLoaded;
    static volatile boolean lz4Loaded;
    static volatile boolean snappyLoaded;
    static volatile boolean brotliLoaded;

    private NativeLibraryLoader() {
    }

    /// Test seam for simulating `System.load()` success/failure without a real native
    /// library. Package-private; the production path passes `null` and uses [loadNative].
    @FunctionalInterface
    interface LibraryLoader {
        boolean load(Path libFile, Consumer<Path> postLoad);
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
        loadCodec("zstd", "libzstd-jni-" + ZSTD_JNI_VERSION, "libzstd-jni-",
                path -> assumeZstdLoaded());
    }

    /// Loads lz4-java native library. No-op on JVM (lz4-java loads from the JAR).
    public static void loadLz4() {
        if (lz4Loaded) {
            return;
        }
        lz4Loaded = true;
        loadCodec("lz4", "liblz4-java", "liblz4-java", null);
    }

    /// Loads snappy-java native library. No-op on JVM (snappy-java loads from the JAR).
    public static void loadSnappy() {
        if (snappyLoaded) {
            return;
        }
        snappyLoaded = true;
        loadCodec("snappy", "libsnappyjava", "libsnappyjava",
                NativeLibraryLoader::assumeSnappyLoaded);
    }

    /// Loads brotli4j native library. No-op on JVM (brotli4j loads from the JAR).
    public static void loadBrotli() {
        if (brotliLoaded) {
            return;
        }
        brotliLoaded = true;
        loadCodec("brotli", "libbrotli", "libbrotli",
                NativeLibraryLoader::assumeBrotliLoaded);
    }

    /// Loads a native codec library using the external-first, embedded-fallback chain.
    ///
    /// Delegates to [loadCodecImpl] after the `inImageCode()` gate check. On JVM (where
    /// `inImageCode()` returns `false`), this method is a no-op — each codec loads from
    /// its own JAR.
    ///
    /// @param name          human-readable library name for logging
    /// @param exactBaseName file base name to try first (without extension)
    /// @param scanPrefix    fallback prefix when scanning the lib dir
    /// @param postLoad      callback invoked after a successful `System.load()`, or `null`
    static void loadCodec(String name, String exactBaseName, String scanPrefix,
            Consumer<Path> postLoad) {
        if (!inImageCode()) {
            return;
        }
        loadCodecImpl(name, exactBaseName, scanPrefix, postLoad);
    }

    /// Executes the native library loading logic without the `inImageCode()` gate.
    ///
    /// Delegates to the Supplier-accepting overload with the real lib-dir resolver.
    ///
    /// @param name          human-readable library name for logging
    /// @param exactBaseName file base name to try first (without extension)
    /// @param scanPrefix    fallback prefix when scanning the lib dir
    /// @param postLoad      callback invoked after a successful `System.load()`, or `null`
    static void loadCodecImpl(String name, String exactBaseName, String scanPrefix,
            Consumer<Path> postLoad) {
        loadCodecImpl(name, exactBaseName, scanPrefix, postLoad,
                NativeLibraryLoader::resolveLibDir);
    }

    /// Executes the native library loading logic with a pluggable lib-dir supplier for testability.
    ///
    /// 1. Tries the external `lib/` directory obtained from `libDirSupplier` as the primary path.
    /// 2. Falls back to embedded resources (cache-dir extraction).
    /// 3. Logs a `WARNING:` when external is unavailable and falling back to embedded.
    /// 4. Logs a final ERROR to [System.err] if all approaches fail.
    ///
    /// @param name            human-readable library name for logging
    /// @param exactBaseName   file base name to try first (without extension)
    /// @param scanPrefix      fallback prefix when scanning the lib dir
    /// @param postLoad        callback invoked after a successful `System.load()`, or `null`
    /// @param libDirSupplier  supplies the external lib directory, or `null` if unavailable
    static void loadCodecImpl(String name, String exactBaseName, String scanPrefix,
            Consumer<Path> postLoad, Supplier<Path> libDirSupplier) {
        // 1. Try external lib dir (HARDWOOD_LIB_PATH or next to executable)
        Path libDir = libDirSupplier.get();
        Path libFile = libDir == null ? null : resolveLibFile(libDir, exactBaseName, scanPrefix);
        if (libFile != null && Files.isRegularFile(libFile)) {
            if (loadNative(name, libFile, postLoad)) {
                return;
            }
            // The external library exists but cannot be loaded — fall through to embedded
            // extraction rather than leaving the codec unloaded.
            System.err.println("WARNING: Could not load " + name + " native library from external lib dir "
                    + libFile + "; falling back to embedded extraction.");
        }
        else {
            // 2. External lib dir not available — fall back to embedded extraction
            System.err.println("WARNING: External lib dir not available, falling back to embedded extraction for " + name + ".");
        }

        // 3. Try embedded resources (cache dir: java.io.tmpdir → ~/.hardwood)
        if (extractAndLoad(name, exactBaseName, postLoad)) {
            return;
        }

        // 4. All approaches failed
        System.err.println("ERROR: Failed to load native library " + name
                + ". External lib dir not available and embedded extraction failed.");
    }

    private static boolean loadNative(String name, Path libFile, Consumer<Path> postLoad) {
        if (libFile == null || !Files.isRegularFile(libFile)) {
            return false;
        }
        try {
            System.load(libFile.toAbsolutePath().toString());
            if (postLoad != null) {
                postLoad.accept(libFile);
            }
            return true;
        }
        catch (UnsatisfiedLinkError e) {
            System.err.println("WARNING: Could not load " + name + " native library from " + libFile + ": " + e.getMessage());
            return false;
        }
    }

    /// Extracts and loads a native library from embedded resources.
    ///
    /// Cache files are named `hardwood-<version>-<shortHash>-<library><ext>` and live
    /// directly in `java.io.tmpdir`, falling back to `~/.hardwood/`. Before touching
    /// java.io.tmpdir, an existing `~/.hardwood/` copy is preferred: its presence signals
    /// a previous execution could not use java.io.tmpdir (e.g. mounted `noexec`), so
    /// re-trying tmp would fail again. If a tmp-extracted library cannot be loaded, the
    /// extracted file and its `.lck` are cleaned up and `~/.hardwood/` is retried.
    ///
    /// @return `true` if the library was extracted and `System.load()` succeeded
    static boolean extractAndLoad(String name, String exactBaseName, Consumer<Path> postLoad) {
        return extractAndLoad(name, exactBaseName, postLoad,
                NativeLibraryLoader::resolveCacheDirs, null);
    }

    /// Extracts and loads a native library from embedded resources with a pluggable
    /// cache-dir supplier for testability.
    ///
    /// @return `true` if the library was extracted and `System.load()` succeeded
    static boolean extractAndLoad(String name, String exactBaseName,
            Consumer<Path> postLoad, Supplier<List<Path>> cacheDirsSupplier) {
        return extractAndLoad(name, exactBaseName, postLoad, cacheDirsSupplier, null);
    }

    /// Extracts and loads a native library from embedded resources with a pluggable
    /// cache-dir supplier and loader for testability.
    ///
    /// @return `true` if the library was extracted and the loader reported success
    static boolean extractAndLoad(String name, String exactBaseName,
            Consumer<Path> postLoad, Supplier<List<Path>> cacheDirsSupplier, LibraryLoader loader) {
        String resourceName = resolveResourcePath(exactBaseName);
        Optional<byte[]> resourceBytes = readEmbeddedResource(resourceName);
        if (resourceBytes.isEmpty()) {
            String os = normalizeOs(System.getProperty(OS_NAME_PROP, ""));
            String arch = normalizeArch(System.getProperty("os.arch", ""));
            System.err.println("WARNING: Embedded native library not found: " + resourceName
                    + " (platform: " + os + "-" + arch + ")");
            return false;
        }

        byte[] libBytes = resourceBytes.get();
        String sha256Hex = sha256Hex(libBytes);
        String version = resolveVersion();
        String shortHash = shortSha256(libBytes);
        String fileName = cacheFileName(version, shortHash, exactBaseName);

        // Home-first: an existing ~/.hardwood copy means a previous run could not use
        // java.io.tmpdir. Prefer it and avoid re-attempting tmp.
        Path homeDir = homeCacheDir();
        if (homeDir != null) {
            Path homeTarget = homeDir.resolve(fileName);
            if (isValidTarget(homeTarget, sha256Hex)) {
                if (tryLoad(name, homeTarget, postLoad, loader)) {
                    return true;
                }
                System.err.println("WARNING: Could not load " + name + " native library from " + homeTarget
                        + "; cleaning up and trying java.io.tmpdir.");
                cleanupFailedExtraction(homeDir, fileName);
            }
        }

        List<Path> cacheDirs = cacheDirsSupplier.get();
        if (cacheDirs.isEmpty()) {
            System.err.println("WARNING: Failed to extract native library " + name
                    + ": no writable cache directory.\n"
                    + "Tried: <java.io.tmpdir>/, ~/.hardwood/");
            return false;
        }

        for (Path cacheDir : cacheDirs) {
            if (extractAndLoadIn(name, fileName, libBytes, sha256Hex, cacheDir, postLoad, loader)) {
                return true;
            }
            // Attempt failed (extraction error or load failure); it already cleaned up.
        }
        return false;
    }

    private static boolean tryLoad(String name, Path target, Consumer<Path> postLoad, LibraryLoader loader) {
        return loader != null ? loader.load(target, postLoad) : loadNative(name, target, postLoad);
    }

    private static boolean extractAndLoadIn(String name, String fileName, byte[] libBytes,
            String sha256Hex, Path cacheDir, Consumer<Path> postLoad, LibraryLoader loader) {
        Path target;
        try {
            target = extractToCache(cacheDir, fileName, libBytes, sha256Hex);
        }
        catch (IOException e) {
            System.err.println("WARNING: Failed to extract native library " + fileName + ": " + e.getMessage());
            return false;
        }

        if (!tryLoad(name, target, postLoad, loader)) {
            // The file is present but cannot be loaded (e.g. /tmp mounted noexec or a
            // corrupt/unloadable file). Clean up so a failed attempt does not linger,
            // then let the caller retry in the next cache directory.
            System.err.println("WARNING: Could not load " + name + " native library from " + target
                    + "; cleaning up and trying the next cache directory.");
            cleanupFailedExtraction(cacheDir, fileName);
            return false;
        }
        return true;
    }

    private static void cleanupFailedExtraction(Path cacheDir, String fileName) {
        deleteIfExistsBestEffort(cacheDir.resolve(fileName));
        deleteIfExistsBestEffort(cacheDir.resolve(lockFileName(fileName)));
    }

    private static void deleteIfExistsBestEffort(Path path) {
        try {
            Files.deleteIfExists(path);
        }
        catch (IOException ignored) {
            // best effort
        }
    }

    private static Path homeCacheDir() {
        String home = System.getProperty("user.home");
        if (home == null || home.isBlank()) {
            return null;
        }
        return Path.of(home, ".hardwood");
    }

    private static Path resolveLibDir() {
        String env = System.getenv("HARDWOOD_LIB_PATH");
        if (env != null && !env.isBlank()) {
            Path p = Path.of(env.trim());
            if (Files.isDirectory(p)) {
                return p;
            }
            System.err.println("WARNING: HARDWOOD_LIB_PATH is set but not a valid directory: " + p);
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

    /// Builds the flat cache file name for an embedded library:
    /// `hardwood-<version>-<shortHash>-<exactBaseName><nativeLibExtension()>`.
    static String cacheFileName(String version, String shortHash, String exactBaseName) {
        return "hardwood-" + version + "-" + shortHash + "-" + exactBaseName + nativeLibExtension();
    }

    /// Builds the lock file name for a cache file: same basename with `.lck` replacing
    /// the platform extension.
    static String lockFileName(String cacheFileName) {
        String ext = nativeLibExtension();
        if (cacheFileName.endsWith(ext)) {
            return cacheFileName.substring(0, cacheFileName.length() - ext.length()) + ".lck";
        }
        return cacheFileName + ".lck";
    }

    /// Resolves the resource path for an embedded native library.
    static String resolveResourcePath(String exactBaseName) {
        String os = normalizeOs(System.getProperty(OS_NAME_PROP, ""));
        String arch = normalizeArch(System.getProperty("os.arch", ""));
        String ext = nativeLibExtension();
        String resourceBaseName = resolveResourceBaseName(exactBaseName);
        return "native/" + os + "-" + arch + "/" + resourceBaseName + ext;
    }

    /// Resolves the resource base name, accounting for platform-specific naming
    /// conventions (e.g. snappy-java omits the `lib` prefix on Windows).
    static String resolveResourceBaseName(String exactBaseName) {
        if ("libsnappyjava".equals(exactBaseName)) {
            String os = System.getProperty(OS_NAME_PROP, "").toLowerCase(Locale.ROOT);
            if (os.contains("windows")) {
                return "snappyjava";
            }
        }
        return exactBaseName;
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

    /// Reads an embedded classpath resource as a byte array.
    /// The `resourcePath` is resolved as an absolute classpath path (relative to the
    /// classpath root), so a leading `/` is prepended if not already present.
    static Optional<byte[]> readEmbeddedResource(String resourcePath) {
        String absolutePath = resourcePath.startsWith("/") ? resourcePath : "/" + resourcePath;
        try (InputStream is = NativeLibraryLoader.class.getResourceAsStream(absolutePath)) {
            if (is == null) {
                return Optional.empty();
            }
            return Optional.of(is.readAllBytes());
        }
        catch (IOException e) {
            return Optional.empty();
        }
    }

    /// Computes the SHA-256 hex digest of the given byte array.
    static String sha256Hex(byte[] data) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(data);
            return HexFormat.of().formatHex(digest);
        }
        catch (Exception e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    /// Computes a short content hash for cache naming: the first 8 bytes of the SHA-256
    /// digest, Base64 URL-encoded without padding.
    static String shortSha256(byte[] data) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(data);
            return Base64.getUrlEncoder().withoutPadding().encodeToString(Arrays.copyOf(digest, 8));
        }
        catch (Exception e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    /// Resolves the project version from the filtered `application.properties`
    /// resource, matching [Version]. Falls back to `unknown` when the resource
    /// or property is unavailable.
    static String resolveVersion() {
        Properties props = new Properties();
        try (InputStream in = NativeLibraryLoader.class.getResourceAsStream("/application.properties")) {
            if (in != null) {
                props.load(in);
            }
        }
        catch (IOException e) {
            // Ignore — fall through to default
        }
        return props.getProperty("project.version", "unknown");
    }

    /// Resolves the first writable cache directory from the fallback chain.
    /// Never creates `~/.hardwood/` — the home fallback is only created when extraction
    /// actually targets it.
    static Optional<Path> resolveCacheDir() {
        List<Path> dirs = resolveCacheDirs();
        return dirs.isEmpty() ? Optional.empty() : Optional.of(dirs.get(0));
    }

    /// Resolves the first writable cache directory with a pluggable directory-writability
    /// predicate for testability. Logs a `WARNING:` to [System.err] for each cache-directory
    /// option that is evaluated and found not writable.
    ///
    /// @param directoryCheck predicate that returns `true` if the directory is usable
    static Optional<Path> resolveCacheDir(Predicate<Path> directoryCheck) {
        List<Path> dirs = resolveCacheDirs(directoryCheck);
        return dirs.isEmpty() ? Optional.empty() : Optional.of(dirs.get(0));
    }

    /// Resolves all writable cache directories in fallback order.
    ///
    /// 1. `java.io.tmpdir` (directly — no subdirectory)
    /// 2. `~/.hardwood/` (only if usable; it is **not** created here — extraction creates
    ///    it only when java.io.tmpdir cannot be used)
    static List<Path> resolveCacheDirs() {
        List<Path> dirs = new ArrayList<>(2);

        // 1. java.io.tmpdir directly (no-op ensure; the directory already exists)
        String javaTmpDir = System.getProperty("java.io.tmpdir");
        if (javaTmpDir != null && !javaTmpDir.isBlank()) {
            Path tmpDir = Path.of(javaTmpDir);
            if (ensureDirectory(tmpDir)) {
                dirs.add(tmpDir);
            }
            else {
                System.err.println("WARNING: java.io.tmpdir not writable: " + tmpDir);
            }
        }

        // 2. ~/.hardwood/ — do not create it here; it is created by extraction only when
        //    it is actually needed (java.io.tmpdir failed).
        Path homeDir = homeCacheDir();
        if (homeDir != null) {
            if (isHomeCacheUsable(homeDir)) {
                dirs.add(homeDir);
            }
            else {
                System.err.println("WARNING: ~/.hardwood/ not usable: " + homeDir);
            }
        }

        return dirs;
    }

    /// Resolves all writable cache directories in fallback order with a pluggable
    /// directory-writability predicate for testability. Logs a `WARNING:` to [System.err]
    /// for each cache-directory option that is evaluated and found not writable.
    ///
    /// @param directoryCheck predicate that returns `true` if the directory is usable
    static List<Path> resolveCacheDirs(Predicate<Path> directoryCheck) {
        List<Path> dirs = new ArrayList<>(2);

        // 1. java.io.tmpdir directly
        String javaTmpDir = System.getProperty("java.io.tmpdir");
        if (javaTmpDir != null && !javaTmpDir.isBlank()) {
            Path tmpDir = Path.of(javaTmpDir);
            if (directoryCheck.test(tmpDir)) {
                dirs.add(tmpDir);
            }
            else {
                System.err.println("WARNING: java.io.tmpdir not writable: " + tmpDir);
            }
        }

        // 2. ~/.hardwood/
        String home = System.getProperty("user.home");
        if (home != null && !home.isBlank()) {
            Path homeDir = Path.of(home, ".hardwood");
            if (directoryCheck.test(homeDir)) {
                dirs.add(homeDir);
            }
            else {
                System.err.println("WARNING: ~/.hardwood/ not writable: " + homeDir);
            }
        }

        return dirs;
    }

    private static boolean ensureDirectory(Path dir) {
        try {
            Files.createDirectories(dir);
            return Files.isDirectory(dir) && Files.isWritable(dir);
        }
        catch (IOException e) {
            return false;
        }
    }

    /// Reports whether `~/.hardwood/` can be used as a cache directory **without creating
    /// it**: it is usable when it already exists and is writable, or when it does not exist
    /// but its parent (`~`) is writable so it can be created later when extraction needs it.
    private static boolean isHomeCacheUsable(Path homeDir) {
        if (Files.isDirectory(homeDir)) {
            return Files.isWritable(homeDir);
        }
        Path parent = homeDir.getParent();
        return parent != null && Files.isDirectory(parent) && Files.isWritable(parent);
    }

    /// Extracts library bytes into the flat cache directory using the name
    /// `hardwood-<version>-<shortHash>-<library><ext>`. Serializes concurrent extraction
    /// with a `.lck` file that is eagerly deleted once extraction completes; the lock file
    /// exists only to prevent two processes from extracting the same library at once.
    static Path extractToCache(Path cacheDir, String libFileName,
            byte[] libBytes, String expectedSha256) throws IOException {
        Path target = cacheDir.resolve(libFileName);
        Path lockFile = cacheDir.resolve(lockFileName(libFileName));

        // Step 1: Fast-path cache hit — existing file with matching hash
        if (isValidTarget(target, expectedSha256)) {
            // Opportunistically remove a stale lock file; the target is complete, so no
            // writer will need the lock again.
            deleteIfExistsBestEffort(lockFile);
            return target;
        }

        // Step 2: Create the cache directory (no-op when it already exists)
        Files.createDirectories(cacheDir);

        // Step 3: Attempt file-locked extraction
        boolean locked = false;
        try (FileChannel lockChannel = FileChannel.open(lockFile,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            FileLock lock = tryAcquireLock(lockChannel);
            if (lock != null) {
                locked = true;
                try {
                    // Re-check cache hit under lock
                    if (isValidTarget(target, expectedSha256)) {
                        return target;
                    }

                    Files.deleteIfExists(target);

                    Path temp = cacheDir.resolve(libFileName + "."
                            + ThreadLocalRandom.current().nextLong() + ".tmp");
                    try {
                        Files.write(temp, libBytes);
                        setExecutableBestEffort(temp);
                        Files.move(temp, target, StandardCopyOption.ATOMIC_MOVE);
                    }
                    catch (IOException e) {
                        Files.deleteIfExists(temp);
                        throw e;
                    }

                    return target;
                }
                finally {
                    lock.release();
                }
            }
        }
        finally {
            if (locked) {
                // Eagerly remove the lock file now that extraction is done. It only
                // exists to serialize extraction. Best effort, and only after the channel
                // is closed (Windows cannot delete an open file).
                deleteIfExistsBestEffort(lockFile);
            }
        }

        // Step 4: File locking unavailable — fall back to atomic-move race detection
        if (!locked) {
            System.err.println("WARNING: Could not acquire file lock for native library cache; "
                    + "proceeding without locking.");

            Path temp = cacheDir.resolve(libFileName + "."
                    + ThreadLocalRandom.current().nextLong() + ".tmp");
            try {
                Files.write(temp, libBytes);
                setExecutableBestEffort(temp);

                try {
                    Files.move(temp, target, StandardCopyOption.ATOMIC_MOVE);
                }
                catch (FileAlreadyExistsException e) {
                    Files.deleteIfExists(temp);
                    if (isValidTarget(target, expectedSha256)) {
                        return target;
                    }
                    // Corrupted — retry with a fresh temp file
                    Files.deleteIfExists(target);
                    Path retryTemp = cacheDir.resolve(libFileName + "."
                            + ThreadLocalRandom.current().nextLong() + ".tmp");
                    Files.write(retryTemp, libBytes);
                    setExecutableBestEffort(retryTemp);
                    Files.move(retryTemp, target, StandardCopyOption.ATOMIC_MOVE,
                            StandardCopyOption.REPLACE_EXISTING);
                }

                return target;
            }
            catch (IOException e) {
                Files.deleteIfExists(temp);
                throw e;
            }
        }

        throw new IOException("Failed to extract native library: " + libFileName);
    }

    private static FileLock tryAcquireLock(FileChannel lockChannel) throws IOException {
        try {
            return lockChannel.tryLock();
        }
        catch (OverlappingFileLockException e) {
            // Same-JVM overlapping lock; treat as "lock unavailable" and rely on the
            // atomic-move race-detection fallback below.
            return null;
        }
    }

    private static boolean isValidTarget(Path target, String expectedSha256) {
        if (!Files.isRegularFile(target)) {
            return false;
        }
        try {
            return sha256Hex(Files.readAllBytes(target)).equals(expectedSha256);
        }
        catch (IOException e) {
            return false;
        }
    }

    private static void setExecutableBestEffort(Path file) {
        try {
            file.toFile().setExecutable(true);
        }
        catch (Exception ignored) {
            // best effort; non-fatal on Windows
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
    /// be the full extracted file name (e.g. `hardwood-<version>-<shortHash>-libsnappyjava.so`)
    /// so that `findNativeLibrary()` resolves `new File(libPath, libName)`.
    private static void assumeSnappyLoaded(Path libPath) {
        System.setProperty("org.xerial.snappy.lib.path", libPath.getParent().toString());
        System.setProperty("org.xerial.snappy.lib.name", libPath.getFileName().toString());
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
