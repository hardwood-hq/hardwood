/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import dev.hardwood.HardwoodContext;
import dev.hardwood.InputFile;
import dev.hardwood.schema.ColumnProjection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A [MetadataSource] installed on a context must supply the footer without the
/// reader re-reading it from disk, and must trigger a [StaleMetadataException]
/// when the supplied identity does not match the file.
class ReuseFileMetadataTest {

    private static final String FILE = "src/test/resources/page_index_test.parquet";

    /// Installing a MetadataSource that calls ParsedFooter.readFrom() on first
    /// access and caches the result must skip the footer on subsequent opens while
    /// still reading the same rows.
    @Test
    void metadataSourceSkipsFooterOnSubsequentOpens() throws Exception {
        // Simple one-entry cache: reads the footer once, returns it on every call.
        ParsedFooter[] cache = new ParsedFooter[1];
        MetadataSource source = file -> {
            if (cache[0] == null) {
                cache[0] = ParsedFooter.readFrom(file);
            }
            return cache[0];
        };

        long expected;
        // First open — populates the cache.
        try (HardwoodContext ctx = HardwoodContext.builder().metadataSource(source).build()) {
            try (ParquetFileReader reader = ParquetFileReader.open(
                    InputFile.of(Paths.get(FILE)), ctx)) {
                expected = count(reader);
            }
        }

        // Second open against a WatchedInputFile: footer reads must be zero because
        // the source returns the cached ParsedFooter, not re-reading from disk.
        WatchedInputFile watched = new WatchedInputFile(
                InputFile.of(Paths.get(FILE)), footerStart(Paths.get(FILE)));
        try (HardwoodContext ctx = HardwoodContext.builder().metadataSource(source).build()) {
            try (ParquetFileReader reader = ParquetFileReader.open(watched, ctx)) {
                assertThat(count(reader)).isEqualTo(expected);
            }
        }
        // Exactly one trailer read (8 bytes) is expected: validateTrailer checks
        // the end-magic and footer length to detect file replacement. The full
        // footer parse (O(columns)) must not be repeated.
        assertThat(watched.tailReads.get())
                .as("a reader with a MetadataSource must read only the trailer, not the full footer")
                .isEqualTo(1);
    }

    /// A MetadataSource is consulted on every open, including multi-file openAll.
    @Test
    void metadataSourceIsConsultedForOpenAll() throws Exception {
        ParsedFooter[] cache = new ParsedFooter[1];
        MetadataSource source = file -> {
            if (cache[0] == null) {
                cache[0] = ParsedFooter.readFrom(file);
            }
            return cache[0];
        };

        long expected;
        try (HardwoodContext ctx = HardwoodContext.builder().metadataSource(source).build()) {
            try (ParquetFileReader r = ParquetFileReader.open(InputFile.of(Paths.get(FILE)), ctx)) {
                expected = count(r);
            }
        }

        WatchedInputFile watched = new WatchedInputFile(
                InputFile.of(Paths.get(FILE)), footerStart(Paths.get(FILE)));
        try (HardwoodContext ctx = HardwoodContext.builder().metadataSource(source).build()) {
            try (ParquetFileReader r = ParquetFileReader.openAll(
                    java.util.List.of(watched), ctx)) {
                assertThat(count(r)).isEqualTo(expected);
            }
        }
        assertThat(watched.tailReads.get())
                .as("openAll with a MetadataSource must read only the trailer, not the full footer")
                .isEqualTo(1);
    }

    /// When the ParsedFooter's sourceIdentity does not match the file's current
    /// identity, openInternal must throw StaleMetadataException before reading
    /// any data.
    @Test
    void staleIdentityThrows() throws Exception {
        ParsedFooter stale;
        // Read the real footer from an opened file, then wrap it with a wrong identity.
        InputFile tmpFile = InputFile.of(Paths.get(FILE));
        tmpFile.open();
        ParsedFooter real = ParsedFooter.readFrom(tmpFile);
        tmpFile.close();
        stale = ParsedFooter.of(
                real.metaData(),
                real.schema(),
                real.footerLength(),
                real.fileLength(),
                Optional.of("stale-identity-that-does-not-match"));

        MetadataSource source = file -> stale;
        try (HardwoodContext ctx = HardwoodContext.builder().metadataSource(source).build()) {
            assertThatThrownBy(() -> ParquetFileReader.open(InputFile.of(Paths.get(FILE)), ctx))
                    .isInstanceOf(StaleMetadataException.class)
                    .satisfies(e -> assertThat(((StaleMetadataException) e).sourceIdentity())
                            .contains("stale-identity-that-does-not-match"));
        }
    }

    /// A ParsedFooter without a sourceIdentity must never trigger a
    /// StaleMetadataException, even when the file has an identity.
    @Test
    void missingSourceIdentityNeverTriggersStaleException() throws Exception {
        ParsedFooter noId;
        InputFile tmpFile2 = InputFile.of(Paths.get(FILE));
        tmpFile2.open();
        ParsedFooter real2 = ParsedFooter.readFrom(tmpFile2);
        tmpFile2.close();
        noId = ParsedFooter.of(
                real2.metaData(), real2.schema(),
                real2.footerLength(), real2.fileLength(),
                Optional.empty());

        MetadataSource source = file -> noId;
        try (HardwoodContext ctx = HardwoodContext.builder().metadataSource(source).build()) {
            long rows;
            try (ParquetFileReader r = ParquetFileReader.open(InputFile.of(Paths.get(FILE)), ctx)) {
                rows = count(r);
            }
            assertThat(rows).isPositive();
        }
    }

    /// A context without a MetadataSource behaves exactly as before.
    @Test
    void contextWithoutSourceReadsNormally() throws Exception {
        long expected;
        try (HardwoodContext ctx = HardwoodContext.create()) {
            try (ParquetFileReader r = ParquetFileReader.open(InputFile.of(Paths.get(FILE)), ctx)) {
                expected = count(r);
            }
        }
        try (HardwoodContext ctx = HardwoodContext.builder().build()) {
            try (ParquetFileReader r = ParquetFileReader.open(InputFile.of(Paths.get(FILE)), ctx)) {
                assertThat(count(r)).isEqualTo(expected);
            }
        }
    }

    /// First byte of the Thrift footer, from the 8-byte trailer (4-byte footer
    /// length LE + 4-byte magic).
    private static long footerStart(Path file) throws IOException {
        byte[] trailer = new byte[8];
        long size = java.nio.file.Files.size(file);
        try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(file.toFile(), "r")) {
            raf.seek(size - 8);
            raf.readFully(trailer);
        }
        long footerLength = (trailer[0] & 0xFFL) | ((trailer[1] & 0xFFL) << 8)
                | ((trailer[2] & 0xFFL) << 16) | ((trailer[3] & 0xFFL) << 24);
        return size - 8 - footerLength;
    }

    private static long count(ParquetFileReader reader) throws IOException {
        String first = reader.getFileSchema().getColumn(0).fieldPath().toString();
        long rows = 0;
        try (ColumnReaders readers = reader.columnReaders(ColumnProjection.columns(first))) {
            while (readers.nextBatch()) {
                rows += readers.getRecordCount();
            }
        }
        return rows;
    }

    /// Records the lifecycle calls a reader makes on the file it is given, and
    /// the reads at or beyond `tailFrom` — the footer start for the staleness
    /// test, [Long#MAX_VALUE] to disable counting.
    private static final class WatchedInputFile implements InputFile {

        private final InputFile delegate;
        private final long tailFrom;
        // AtomicInteger: readRange may be called from hardwood worker threads;
        // volatile int with ++ is non-atomic and can lose concurrent increments.
        private final AtomicInteger tailReads = new AtomicInteger();
        private int opens;
        private int closes;

        WatchedInputFile(InputFile delegate, long tailFrom) {
            this.delegate = delegate;
            this.tailFrom = tailFrom;
        }

        @Override
        public void open() throws IOException {
            opens++;
            delegate.open();
        }

        @Override
        public ByteBuffer readRange(long position, int length) throws IOException {
            if (position >= tailFrom) {
                tailReads.incrementAndGet();
            }
            return delegate.readRange(position, length);
        }

        @Override
        public long length() throws IOException {
            return delegate.length();
        }

        @Override
        public String name() {
            return delegate.name();
        }

        @Override
        public void close() throws IOException {
            closes++;
            delegate.close();
        }
    }
}
