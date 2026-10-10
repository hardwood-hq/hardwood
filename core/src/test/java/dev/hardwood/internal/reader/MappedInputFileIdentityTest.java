/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.hardwood.InputFile;
import dev.hardwood.metadata.ParsedFooter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/// A local file's identity is its size, modification time and file key (or absolute path where
/// the file system has no file keys), read when it is opened (or, when a reader without a
/// `MetadataSource` opened it, when first asked for) and kept for the file's lifetime.
class MappedInputFileIdentityTest {

    private static final Path FILE = Path.of("src/test/resources/plain_uncompressed.parquet");

    @TempDir
    Path tempDir;

    @Test
    void theIdentityIsTheFileAttributesAtOpen() throws Exception {
        Path path = Files.copy(FILE, tempDir.resolve("file.parquet"));
        BasicFileAttributes attributes = Files.readAttributes(path, BasicFileAttributes.class);
        String expected = attributes.size() + ":" + attributes.lastModifiedTime().toInstant()
                + ":" + (attributes.fileKey() != null ? attributes.fileKey() : path.toAbsolutePath().normalize());

        assertThat(identityOf(path)).contains(expected);
    }

    @Test
    void aSameSizeRewriteWithinTheSameMillisecondChangesTheIdentity() throws Exception {
        Path path = Files.copy(FILE, tempDir.resolve("file.parquet"));
        Instant written = Instant.parse("2026-01-01T00:00:00.000100Z");
        Files.setLastModifiedTime(path, FileTime.from(written));
        assumeTrue(Files.getLastModifiedTime(path).toInstant().equals(written),
                "the file system keeps sub-millisecond modification times");
        Optional<String> before = identityOf(path);

        Files.write(path, Files.readAllBytes(FILE));
        Files.setLastModifiedTime(path, FileTime.from(written.plusNanos(1_000)));

        assertThat(identityOf(path)).isNotEqualTo(before);
    }

    @Test
    void twoFilesOfTheSameSizeAndModificationTimeHaveDifferentIdentities() throws Exception {
        Path first = Files.createDirectory(tempDir.resolve("a")).resolve("part-0.parquet");
        Path second = Files.createDirectory(tempDir.resolve("b")).resolve("part-0.parquet");
        Files.copy(FILE, first);
        Files.copy(FILE, second);
        FileTime modified = FileTime.from(Instant.parse("2026-01-01T00:00:00Z"));
        Files.setLastModifiedTime(first, modified);
        Files.setLastModifiedTime(second, modified);

        assertThat(identityOf(first)).isNotEqualTo(identityOf(second));
    }

    @Test
    void aReplacementByRenameChangesTheIdentity() throws Exception {
        Path path = Files.copy(FILE, tempDir.resolve("file.parquet"));
        assumeTrue(Files.readAttributes(path, BasicFileAttributes.class).fileKey() != null,
                "the file system has file keys");
        FileTime modified = Files.getLastModifiedTime(path);
        Optional<String> before = identityOf(path);

        Path replacement = Files.copy(FILE, tempDir.resolve("replacement.parquet"));
        Files.setLastModifiedTime(replacement, modified);
        Files.move(replacement, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

        assertThat(identityOf(path)).isNotEqualTo(before);
    }

    @Test
    void theIdentityStaysThatOfTheContentOpened() throws Exception {
        Path path = Files.copy(FILE, tempDir.resolve("file.parquet"));
        try (InputFile file = InputFile.of(path)) {
            file.open();
            Optional<String> atOpen = file.identity();

            Path replacement = Files.copy(FILE, tempDir.resolve("replacement.parquet"));
            Files.move(replacement, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

            assertThat(file.identity()).isEqualTo(atOpen);
        }
    }

    @Test
    void theIdentityIsResolvedByOpen() throws Exception {
        try (InputFile file = InputFile.of(FILE)) {
            assertThatThrownBy(file::identity)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("File not opened: plain_uncompressed.parquet");
        }
    }

    @Test
    void aReaderWithoutAMetadataSourceDefersTheIdentityToTheFirstCall() throws Exception {
        Path path = Files.copy(FILE, tempDir.resolve("file.parquet"));
        assumeTrue(Files.readAttributes(path, BasicFileAttributes.class).fileKey() != null,
                "the file system has file keys");
        try (InputFile file = InputFile.of(path)) {
            ParquetMetadataReader.open(file, ParquetMetadataReader.FROM_FILE);
            replace(path);
            Optional<String> replacement = identityOf(path);

            assertThat(file.identity()).isEqualTo(replacement);
            replace(path);
            assertThat(file.identity()).isEqualTo(replacement);
        }
    }

    @Test
    void aReaderWithAMetadataSourceResolvesTheIdentityAtOpen() throws Exception {
        Path path = Files.copy(FILE, tempDir.resolve("file.parquet"));
        assumeTrue(Files.readAttributes(path, BasicFileAttributes.class).fileKey() != null,
                "the file system has file keys");
        Optional<String> beforeOpen = identityOf(path);
        try (InputFile file = InputFile.of(path)) {
            ParquetMetadataReader.open(file, ParsedFooter::readFrom);
            replace(path);

            assertThat(file.identity()).isEqualTo(beforeOpen);
        }
    }

    @Test
    void aDeferredIdentityIsNotResolvedBeforeOpen() throws Exception {
        try (MappedInputFile file = new MappedInputFile(FILE)) {
            assertThatThrownBy(file::identity)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("File not opened: plain_uncompressed.parquet");
        }
    }

    @Test
    void anInMemoryFileHasNoIdentity() throws Exception {
        try (InputFile file = InputFile.of(ByteBuffer.wrap(Files.readAllBytes(FILE)))) {
            file.open();
            assertThat(file.identity()).isEmpty();
        }
    }

    /// Replaces the file at `path` by an atomic rename, which gives it a new file key.
    private void replace(Path path) throws IOException {
        Path replacement = Files.copy(FILE, tempDir.resolve("replacement.parquet"));
        Files.move(replacement, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    private static Optional<String> identityOf(Path path) throws IOException {
        try (InputFile file = InputFile.of(path)) {
            file.open();
            return file.identity();
        }
    }
}
