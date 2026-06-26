/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetFileReader.RowReaderBuilder;
import dev.hardwood.reader.RowGroupPredicate;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Coverage matrix for [RowReaderBuilder] option combinations.
///
/// Every public option — `projection`, `filter(FilterPredicate)` (row-level),
/// `filter(RowGroupPredicate)` (group-level / byte-range), `head`, `tail`, `skip` —
/// is exercised against the others. Each combination has one of two dispositions:
///
/// - **BUILDS** — the combination is legal: the reader is built and fully iterated,
///   asserting only that it constructs and yields a plausible row count. This pins the
///   *shape* of the matrix (which combinations are legal) and that none throw or hang.
/// - **REJECTED** — the combination is rejected at `build()`; the message is asserted.
///
/// Per-combination *correctness* — that a legal combination returns the **right rows**, not
/// merely a clean build — is deferred to the differential oracle (session task #1) and to
/// the dedicated tests in [ParquetReaderTest] / [RowGroupFilterTest]. Cells whose correctness
/// is not yet pinned carry an `oracleNote`; a non-`null` note flags a combination that needs
/// follow-up (a known bug in flight, or a legal-but-unverified interaction).
///
/// The matrix is the live contract: adding a builder option means adding its rows here, and a
/// behavior change makes the corresponding row fail until it is updated.
class BuilderCombinationTest {

    private static final Path FIXTURE = Paths.get("src/test/resources/filter_pushdown_int.parquet");
    private static final List<InputFile> MULTI_FIXTURE = InputFile.ofPaths(
            Paths.get("src/test/resources/multi_file_part0.parquet"),
            Paths.get("src/test/resources/multi_file_part1.parquet")
    );

    /// `filter_pushdown_int.parquet`: 3 row groups of 100 rows — `id` 1..100, 101..200, 201..300.
    private static final long TOTAL_ROWS = 300;

    /// Midpoint of row group 1; `byteRange(rg1Mid, fileLen)` keeps row groups 1 and 2 (rows 101..300).
    private static long rg1Mid;
    private static long fileLen;

    @BeforeAll
    static void readLayout() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE))) {
            List<RowGroup> rgs = reader.getFileMetaData().rowGroups();
            assertThat(rgs).hasSize(3);
            rg1Mid = midpoint(rgs.get(1));
            fileLen = FIXTURE.toFile().length();
        }
    }

    private static long midpoint(RowGroup rg) {
        long start = rg.columns().get(0).chunkStartOffset();
        long compressed = 0;
        for (ColumnChunk c : rg.columns()) {
            compressed += c.metaData().totalCompressedSize();
        }
        return start + compressed / 2;
    }

    enum Disposition { BUILDS, REJECTED }

    record Combo(String name, Consumer<RowReaderBuilder> apply, Disposition expect,
                 String rejectMessage, String oracleNote) {

        static Combo builds(String name, Consumer<RowReaderBuilder> apply) {
            return new Combo(name, apply, Disposition.BUILDS, null, null);
        }

        static Combo buildsPending(String name, Consumer<RowReaderBuilder> apply, String oracleNote) {
            return new Combo(name, apply, Disposition.BUILDS, null, oracleNote);
        }

        static Combo rejected(String name, Consumer<RowReaderBuilder> apply, String rejectMessage) {
            return new Combo(name, apply, Disposition.REJECTED, rejectMessage, null);
        }

        @Override
        public String toString() {
            return name;
        }
    }

    static Stream<Combo> matrix() {
        return Stream.of(
                // ---- singletons ----
                Combo.builds("projection", b -> b.projection(ColumnProjection.columns("id"))),
                Combo.builds("filterFP", b -> b.filter(FilterPredicate.gt("id", 150L))),
                Combo.builds("filterRGP", b -> b.filter(RowGroupPredicate.byteRange(rg1Mid, fileLen))),
                Combo.builds("head", b -> b.head(50)),
                Combo.builds("tail", b -> b.tail(50)),
                Combo.builds("skip", b -> b.skip(50)),

                // ---- projection × {…} (the orthogonal, previously untested row) ----
                Combo.builds("projection + filterFP",
                        b -> b.projection(ColumnProjection.columns("id")).filter(FilterPredicate.gt("id", 150L))),
                Combo.builds("projection + filterRGP",
                        b -> b.projection(ColumnProjection.columns("id")).filter(RowGroupPredicate.byteRange(rg1Mid, fileLen))),
                Combo.builds("projection + head",
                        b -> b.projection(ColumnProjection.columns("id")).head(50)),
                Combo.builds("projection + tail",
                        b -> b.projection(ColumnProjection.columns("id")).tail(50)),
                Combo.builds("projection + skip",
                        b -> b.projection(ColumnProjection.columns("id")).skip(50)),

                // ---- filter(FilterPredicate) × {…} ----
                Combo.builds("filterFP + filterRGP",
                        b -> b.filter(FilterPredicate.gt("id", 150L)).filter(RowGroupPredicate.byteRange(rg1Mid, fileLen))),
                Combo.builds("filterFP + head",
                        b -> b.filter(FilterPredicate.gt("id", 150L)).head(50)),
                Combo.rejected("filterFP + tail",
                        b -> b.filter(FilterPredicate.gt("id", 150L)).tail(50),
                        "tail cannot be combined with a filter"),
                Combo.builds("filterFP + skip",
                        b -> b.filter(FilterPredicate.gt("id", 150L)).skip(50)),

                // ---- filter(RowGroupPredicate) × {…} ----
                Combo.buildsPending("filterRGP + head",
                        b -> b.filter(RowGroupPredicate.byteRange(rg1Mid, fileLen)).head(50),
                        "head must cap the byte-range-kept set, not the whole file — oracle-pending"),
                Combo.rejected("filterRGP + tail",
                        b -> b.filter(RowGroupPredicate.byteRange(rg1Mid, fileLen)).tail(50),
                        "row-group filter"),
                Combo.builds("filterRGP + skip",
                        b -> b.filter(RowGroupPredicate.byteRange(rg1Mid, fileLen)).skip(50)),

                // ---- head / tail / skip mutual exclusions ----
                Combo.rejected("head + tail", b -> b.head(50).tail(50),
                        "head and tail are mutually exclusive"),
                Combo.builds("head + skip", b -> b.skip(50).head(50)),
                Combo.rejected("tail + skip", b -> b.skip(50).tail(50),
                        "tail and skip are mutually exclusive"),

                // ---- key triples ----
                Combo.builds("filterFP + skip + head",
                        b -> b.filter(FilterPredicate.gt("id", 150L)).skip(20).head(50)),
                Combo.builds("filterRGP + skip + head",
                        b -> b.filter(RowGroupPredicate.byteRange(rg1Mid, fileLen)).skip(20).head(50)),
                Combo.builds("projection + filterRGP + skip + head",
                        b -> b.projection(ColumnProjection.columns("id"))
                                .filter(RowGroupPredicate.byteRange(rg1Mid, fileLen)).skip(20).head(50)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("matrix")
    void combination(Combo c) throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE))) {
            RowReaderBuilder builder = reader.buildRowReader();
            c.apply().accept(builder);

            if (c.expect() == Disposition.REJECTED) {
                assertThatThrownBy(builder::build)
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining(c.rejectMessage());
                return;
            }

            long count = 0;
            try (RowReader rows = builder.build()) {
                while (rows.hasNext()) {
                    rows.next();
                    count++;
                }
            }
            String label = c.oracleNote() == null ? c.name() : c.name() + " [" + c.oracleNote() + "]";
            assertThat(count).as(label).isBetween(0L, TOTAL_ROWS);
        }
    }

    /// Multi-file boundary cells (#577, #672) — physical `skip` (no filter) is a
    /// global offset across all files, logical `skip` (with a filter) counts matches
    /// across *all* files in order, and `tail` is single-file-only (throws).
    /// `id` is the global row position: file 0 holds 0..149, file 1 holds 150..249.
    @Test
    void multiFileBoundaries() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.openAll(MULTI_FIXTURE)) {
            assertThat(ids(reader.buildRowReader().skip(125)))
                    .containsExactlyElementsOf(longRange(125, 249));
            assertThat(ids(reader.buildRowReader().filter(FilterPredicate.gt("id", 99L)).skip(60)))
                    .containsExactlyElementsOf(longRange(160, 249));
            assertThat(ids(reader.buildRowReader().skip(160)))
                    .containsExactlyElementsOf(longRange(160, 249));
            assertThat(ids(reader.buildRowReader().skip(250)))
                    .isEmpty();
            assertThat(ids(reader.buildRowReader().skip(300)))
                    .isEmpty();
            assertThatThrownBy(reader.buildRowReader().tail(50)::build)
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }

    @Test
    void physicalMultiFileSkipDoesNotReadSkippedFileData() throws Exception {
        CountingInputFile file0 = new CountingInputFile(InputFile.of(
                Paths.get("src/test/resources/multi_file_part0.parquet")));
        CountingInputFile file1 = new CountingInputFile(InputFile.of(
                Paths.get("src/test/resources/multi_file_part1.parquet")));

        try (ParquetFileReader reader = ParquetFileReader.openAll(List.of(file0, file1))) {
            int file0ReadsAfterOpen = file0.readCount();

            assertThat(ids(reader.buildRowReader().skip(160).head(1)))
                    .containsExactly(160L);
            assertThat(file0.readCount())
                    .as("file 0 should only have the footer reads from openAll")
                    .isEqualTo(file0ReadsAfterOpen);
        }
    }

    @Test
    void physicalMultiFileSkipDoesNotReadMiddleSkippedFileData() throws Exception {
        CountingInputFile file0 = new CountingInputFile(InputFile.of(
                Paths.get("src/test/resources/multi_file_part0.parquet")));
        CountingInputFile skippedMiddle = new CountingInputFile(InputFile.of(
                Paths.get("src/test/resources/multi_file_part1.parquet")));
        CountingInputFile target = new CountingInputFile(InputFile.of(
                Paths.get("src/test/resources/multi_file_part1.parquet")));

        try (ParquetFileReader reader = ParquetFileReader.openAll(List.of(file0, skippedMiddle, target))) {
            assertThat(ids(reader.buildRowReader().skip(250).head(1)))
                    .containsExactly(150L);
            assertThat(skippedMiddle.readCount())
                    .as("middle file should only be opened for metadata")
                    .isEqualTo(3);
            assertThat(skippedMiddle.bytesRead())
                    .as("middle file should not have data pages fetched")
                    .isLessThan(skippedMiddle.length());
            assertThat(target.readCount())
                    .as("target file should be read beyond metadata to produce a row")
                    .isGreaterThan(skippedMiddle.readCount());
        }
    }

    private static List<Long> ids(RowReaderBuilder builder) {
        List<Long> rows = new ArrayList<>();
        try (RowReader r = builder.build()) {
            while (r.hasNext()) {
                r.next();
                rows.add(r.getLong("id"));
            }
        }
        return rows;
    }

    private static List<Long> longRange(long fromInclusive, long toInclusive) {
        return LongStream.rangeClosed(fromInclusive, toInclusive).boxed().toList();
    }

    private static final class CountingInputFile implements InputFile {
        private final InputFile delegate;
        private final AtomicInteger readCount = new AtomicInteger();
        private final AtomicLong bytesRead = new AtomicLong();

        private CountingInputFile(InputFile delegate) {
            this.delegate = delegate;
        }

        int readCount() {
            return readCount.get();
        }

        long bytesRead() {
            return bytesRead.get();
        }

        @Override
        public void open() throws IOException {
            delegate.open();
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            readCount.incrementAndGet();
            bytesRead.addAndGet(length);
            return delegate.readRange(offset, length);
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
            delegate.close();
        }
    }
}
