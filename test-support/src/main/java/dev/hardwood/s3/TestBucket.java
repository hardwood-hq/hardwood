/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/// A bucket on an [S3Proxy], owned by one test class: the directory
/// `<data dir>/<name>`. Tests register objects before [#create()] or write
/// them through [#objectPath] afterwards, and [#delete()] the bucket when
/// done.
///
/// The name is the test class name plus a random suffix, so concurrent runs
/// of the same test against a shared server never see each other's objects.
public final class TestBucket {

    /// Every bucket name has this shape; nothing else in a data dir does.
    private static final Pattern NAME = Pattern.compile("[a-z0-9]+-[0-9a-f]{8}");
    private static final Pattern PREFIX = Pattern.compile("[a-z0-9]{1,54}");

    private final S3Proxy proxy;
    private final String name;
    private final Map<String, Path> fileSources = new LinkedHashMap<>();
    private final Map<String, byte[]> contents = new LinkedHashMap<>();
    private boolean created;

    TestBucket(S3Proxy proxy, Class<?> testClass) {
        this.proxy = proxy;
        String prefix = testClass.getSimpleName().toLowerCase(Locale.ROOT);
        // S3 bucket names take at most 63 characters: the prefix, a hyphen and eight hex digits
        if (!PREFIX.matcher(prefix).matches()) {
            throw new IllegalArgumentException("Test class " + testClass.getName()
                    + " does not yield a valid bucket name; its simple name must be 1 to 54 letters and digits");
        }
        this.name = prefix + "-" + HexFormat.of().toHexDigits(ThreadLocalRandom.current().nextInt());
    }

    static boolean isBucketName(String candidate) {
        return NAME.matcher(candidate).matches();
    }

    /// Registers `source` to be placed at `key` when the bucket is created.
    /// The object may share storage with `source`, so [#objectPath] refuses it.
    public TestBucket withObject(String key, Path source) {
        checkRegistrable(key);
        fileSources.put(key, source);
        return this;
    }

    /// Registers `content` to be written to `key` when the bucket is created.
    public TestBucket withObject(String key, byte[] content) {
        checkRegistrable(key);
        contents.put(key, content);
        return this;
    }

    /// Creates the bucket with every registered object.
    public void create() {
        if (created) {
            throw new IllegalStateException("Bucket " + name + " is already created");
        }
        try {
            Files.createDirectories(directory());
            for (Map.Entry<String, Path> object : fileSources.entrySet()) {
                linkOrCopy(object.getValue(), createParents(object.getKey()));
            }
            for (Map.Entry<String, byte[]> object : contents.entrySet()) {
                Files.write(createParents(object.getKey()), object.getValue());
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to create bucket " + name, e);
        }
        created = true;
    }

    /// Deletes the bucket and every object in it.
    public void delete() {
        deleteRecursively(directory());
    }

    public String name() {
        return name;
    }

    public String endpoint() {
        return proxy.endpoint();
    }

    /// `s3://<name>/<key>`.
    public String uri(String key) {
        return "s3://" + name + "/" + key;
    }

    /// Local path of the file backing `key`. Writes to it are visible to
    /// s3proxy immediately.
    ///
    /// @throws IllegalArgumentException if `key` was registered from a file,
    ///         whose storage the object may share
    public Path objectPath(String key) {
        Path source = fileSources.get(key);
        if (source != null) {
            throw new IllegalArgumentException("Object " + key + " of bucket " + name
                    + " may share storage with " + source + "; register the key from bytes to write to it");
        }
        return directory().resolve(key);
    }

    private void checkRegistrable(String key) {
        if (created) {
            throw new IllegalStateException("Cannot register " + key + ": bucket " + name + " is already created");
        }
        if (fileSources.containsKey(key) || contents.containsKey(key)) {
            throw new IllegalArgumentException("Object " + key + " is already registered in bucket " + name);
        }
    }

    private Path createParents(String key) throws IOException {
        Path target = directory().resolve(key);
        Files.createDirectories(target.getParent());
        return target;
    }

    private Path directory() {
        return proxy.dataDir().resolve(name);
    }

    /// Hard-links where source and bucket share a file system, so large
    /// fixtures cost nothing; copies otherwise.
    private static void linkOrCopy(Path source, Path target) throws IOException {
        try {
            Files.createLink(target, source);
        }
        catch (IOException | UnsupportedOperationException e) {
            Files.copy(source, target);
        }
    }

    /// Deletes directories named like a bucket and last modified more than
    /// `staleAfter` ago. Other JVMs sweep the same data dir concurrently, so an
    /// entry that disappears meanwhile is skipped.
    static void deleteStaleBuckets(Path dataDir, Duration staleAfter) {
        FileTime cutoff = FileTime.from(Instant.now().minus(staleAfter));
        try (Stream<Path> entries = Files.list(dataDir)) {
            for (Path candidate : entries.toList()) {
                if (isStaleBucket(candidate, cutoff)) {
                    deleteRecursively(candidate);
                }
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to delete stale buckets in " + dataDir, e);
        }
    }

    private static boolean isStaleBucket(Path candidate, FileTime cutoff) throws IOException {
        if (!isBucketName(candidate.getFileName().toString()) || !Files.isDirectory(candidate)) {
            return false;
        }
        try {
            return Files.getLastModifiedTime(candidate).compareTo(cutoff) < 0;
        }
        catch (NoSuchFileException e) {
            return false;
        }
    }

    /// Deletes `root` and everything below it. Entries that disappear
    /// meanwhile are skipped: another JVM's stale-bucket sweep may be
    /// deleting the same directory.
    static void deleteRecursively(Path root) {
        try (Stream<Path> paths = Files.walk(root)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        }
        catch (NoSuchFileException e) {
            // root or a subdirectory is already gone
        }
        catch (UncheckedIOException e) {
            if (!(e.getCause() instanceof NoSuchFileException)) {
                throw e;
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to delete " + root, e);
        }
    }
}
