/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestBucketTest {

    @TempDir
    Path dataDir;

    private record LocalProxy(Path dataDir) implements S3Proxy {

        @Override
        public String endpoint() {
            return "http://localhost:0";
        }
    }

    private static class Bad_Name {
    }

    @Test
    void createWritesRegisteredObjectsAndDeleteRemovesThem() throws IOException {
        Path source = Files.writeString(dataDir.resolve("source.txt"), "from file");
        TestBucket bucket = new LocalProxy(dataDir.resolve("data")).bucketFor(TestBucketTest.class)
                .withObject("nested/a.txt", source)
                .withObject("b.txt", "from bytes".getBytes());

        bucket.create();
        Path bucketDir = dataDir.resolve("data").resolve(bucket.name());
        assertThat(bucketDir.resolve("nested/a.txt")).hasContent("from file");
        assertThat(bucketDir.resolve("b.txt")).hasContent("from bytes");

        bucket.delete();
        assertThat(bucketDir).doesNotExist();
        assertThat(source).hasContent("from file");
    }

    @Test
    void nameIsClassNamePlusRandomSuffix() {
        String name = new LocalProxy(dataDir).bucketFor(TestBucketTest.class).name();

        assertThat(name).matches("testbuckettest-[0-9a-f]{8}");
        assertThat(TestBucket.isBucketName(name)).isTrue();
    }

    @Test
    void rejectsClassNameThatIsNoValidBucketName() {
        assertThatThrownBy(() -> new LocalProxy(dataDir).bucketFor(Bad_Name.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Test class dev.hardwood.s3.TestBucketTest$Bad_Name does not yield a valid bucket name; "
                        + "its simple name must be 1 to 54 letters and digits");
    }

    @Test
    void objectPathRejectsObjectRegisteredFromFile() throws IOException {
        Path source = Files.writeString(dataDir.resolve("fixture.parquet"), "fixture");
        TestBucket bucket = new LocalProxy(dataDir).bucketFor(TestBucketTest.class)
                .withObject("fixture.parquet", source);
        bucket.create();

        assertThatThrownBy(() -> bucket.objectPath("fixture.parquet"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Object fixture.parquet of bucket " + bucket.name() + " may share storage with "
                        + source + "; register the key from bytes to write to it");
    }

    @Test
    void rejectsRegistrationAfterCreate() {
        TestBucket bucket = new LocalProxy(dataDir).bucketFor(TestBucketTest.class);
        bucket.create();

        assertThatThrownBy(() -> bucket.withObject("late.txt", new byte[0]))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Cannot register late.txt: bucket " + bucket.name() + " is already created");
    }

    @Test
    void rejectsDuplicateKey() {
        TestBucket bucket = new LocalProxy(dataDir).bucketFor(TestBucketTest.class)
                .withObject("a.txt", new byte[0]);

        assertThatThrownBy(() -> bucket.withObject("a.txt", dataDir))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Object a.txt is already registered in bucket " + bucket.name());
    }

    @Test
    void sweepDeletesOnlyStaleDirectoriesNamedLikeBuckets() throws IOException {
        FileTime twoDaysAgo = FileTime.from(Instant.now().minus(Duration.ofDays(2)));
        Path staleBucket = staleDirectory("someit-0123abcd", twoDaysAgo);
        Path staleNonBucket = staleDirectory("src", twoDaysAgo);
        Path staleFile = Files.writeString(dataDir.resolve("notes-0123abcd"), "not a directory");
        Files.setLastModifiedTime(staleFile, twoDaysAgo);
        Path freshBucket = Files.createDirectory(dataDir.resolve("otherit-89abcdef"));

        TestBucket.deleteStaleBuckets(dataDir, Duration.ofDays(1));

        assertThat(staleBucket).doesNotExist();
        assertThat(staleNonBucket).exists();
        assertThat(staleFile).exists();
        assertThat(freshBucket).exists();
    }

    /// Several JVMs sweep the same data dir at once, so deletions of the
    /// same tree race; none of them may fail.
    @Test
    void concurrentDeletesOfTheSameTreeAllSucceed() throws Exception {
        for (int round = 0; round < 20; round++) {
            Path root = dataDir.resolve("someit-0123abcd");
            for (int dir = 0; dir < 50; dir++) {
                Path sub = Files.createDirectories(root.resolve("dir" + dir));
                for (int file = 0; file < 10; file++) {
                    Files.writeString(sub.resolve("file" + file), "x");
                }
            }
            CountDownLatch start = new CountDownLatch(1);
            ExecutorService executor = Executors.newFixedThreadPool(4);
            try {
                List<Future<?>> deletes = new ArrayList<>();
                for (int thread = 0; thread < 4; thread++) {
                    deletes.add(executor.submit(() -> {
                        start.await();
                        TestBucket.deleteRecursively(root);
                        return null;
                    }));
                }
                start.countDown();
                for (Future<?> delete : deletes) {
                    delete.get();
                }
            }
            finally {
                executor.shutdownNow();
            }
            assertThat(root).doesNotExist();
        }
    }

    private Path staleDirectory(String name, FileTime lastModified) throws IOException {
        Path directory = Files.createDirectory(dataDir.resolve(name));
        Files.setLastModifiedTime(directory, lastModified);
        return directory;
    }
}
