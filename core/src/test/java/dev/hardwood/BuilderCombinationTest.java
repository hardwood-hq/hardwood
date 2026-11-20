/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.internal.io.CountingInputFile;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetFileReader.ColumnReaderBuilder;
import dev.hardwood.reader.ParquetFileReader.ColumnReadersBuilder;
import dev.hardwood.reader.ParquetFileReader.RowReaderBuilder;
import dev.hardwood.reader.RowGroupPredicate;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Coverage matrix for [RowReaderBuilder] option combinations, and for those of the column
/// builders ([ColumnReadersBuilder], [ColumnReaderBuilder]).
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
/// merely a clean build — is covered by [DifferentialReadTest] and by the dedicated tests in
/// [ParquetReaderTest] / [RowGroupFilterTest].
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
                 String rejectMessage) {

        static Combo builds(String name, Consumer<RowReaderBuilder> apply) {
            return new Combo(name, apply, Disposition.BUILDS, null);
        }

        static Combo rejected(String name, Consumer<RowReaderBuilder> apply, String rejectMessage) {
            return new Combo(name, apply, Disposition.REJECTED, rejectMessage);
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
                        "tail cannot be combined with a filter: the set of matching rows is"
                                + " not known from row-group statistics alone"),
                Combo.builds("filterFP + skip",
                        b -> b.filter(FilterPredicate.gt("id", 150L)).skip(50)),

                // ---- filter(RowGroupPredicate) × {…} ----
                Combo.builds("filterRGP + head",
                        b -> b.filter(RowGroupPredicate.byteRange(rg1Mid, fileLen)).head(50)),
                Combo.rejected("filterRGP + tail",
                        b -> b.filter(RowGroupPredicate.byteRange(rg1Mid, fileLen)).tail(50),
                        "tail cannot be combined with a row-group filter: tail mode requires"
                                + " a known total row count, which row-group filtering invalidates"),
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
                        .hasMessage(c.rejectMessage());
                return;
            }

            long count = 0;
            try (RowReader rows = builder.build()) {
                while (rows.hasNext()) {
                    rows.next();
                    count++;
                }
            }
            assertThat(count).as(c.name()).isBetween(0L, TOTAL_ROWS);
        }
    }

    record ColumnCombo(String name, Consumer<ColumnReadersBuilder> apply, String rejectMessage) {

        static ColumnCombo builds(String name, Consumer<ColumnReadersBuilder> apply) {
            return new ColumnCombo(name, apply, null);
        }

        static ColumnCombo rejected(String name, Consumer<ColumnReadersBuilder> apply, String rejectMessage) {
            return new ColumnCombo(name, apply, rejectMessage);
        }

        @Override
        public String toString() {
            return name;
        }
    }

    /// The column builders take `head`, `skip` and `rowRange` beside the two filters; a row range
    /// excludes `head` and `skip`, and the column builders have no `tail`.
    static Stream<ColumnCombo> columnMatrix() {
        return Stream.of(
                ColumnCombo.builds("head", b -> b.head(50)),
                ColumnCombo.builds("skip", b -> b.skip(50)),
                ColumnCombo.builds("rowRange", b -> b.rowRange(50, 150)),
                ColumnCombo.builds("skip + head", b -> b.skip(50).head(50)),
                ColumnCombo.builds("filterFP + head", b -> b.filter(FilterPredicate.gt("id", 150L)).head(50)),
                ColumnCombo.builds("filterFP + skip", b -> b.filter(FilterPredicate.gt("id", 150L)).skip(50)),
                ColumnCombo.builds("filterFP + skip + head",
                        b -> b.filter(FilterPredicate.gt("id", 150L)).skip(20).head(50)),
                ColumnCombo.builds("filterFP + rowRange",
                        b -> b.filter(FilterPredicate.gt("id", 150L)).rowRange(50, 250)),
                ColumnCombo.builds("filterRGP + skip + head",
                        b -> b.filter(RowGroupPredicate.byteRange(rg1Mid, fileLen)).skip(20).head(50)),
                ColumnCombo.builds("filterRGP + rowRange",
                        b -> b.filter(RowGroupPredicate.byteRange(rg1Mid, fileLen)).rowRange(50, 150)),
                ColumnCombo.builds("filterFP + filterRGP + rowRange",
                        b -> b.filter(FilterPredicate.gt("id", 150L))
                                .filter(RowGroupPredicate.byteRange(rg1Mid, fileLen)).rowRange(50, 150)),
                ColumnCombo.rejected("rowRange + skip", b -> b.rowRange(50, 150).skip(10),
                        "rowRange and skip are mutually exclusive: the row range fixes the first row itself"),
                ColumnCombo.rejected("rowRange + head", b -> b.head(10).rowRange(50, 150),
                        "rowRange and head are mutually exclusive: the row range fixes the row count itself"),
                ColumnCombo.rejected("filterFP + rowRange + skip",
                        b -> b.filter(FilterPredicate.gt("id", 150L)).rowRange(50, 150).skip(10),
                        "rowRange and skip are mutually exclusive: the row range fixes the first row itself"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("columnMatrix")
    void columnCombination(ColumnCombo c) throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE))) {
            ColumnReadersBuilder builder = reader.buildColumnReaders(ColumnProjection.columns("id"));
            c.apply().accept(builder);

            if (c.rejectMessage() != null) {
                assertThatThrownBy(builder::build)
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessage(c.rejectMessage());
                return;
            }

            long count = 0;
            try (ColumnReaders columns = builder.build()) {
                while (columns.nextBatch()) {
                    count += columns.getRecordCount();
                }
            }
            assertThat(count).as(c.name()).isBetween(0L, TOTAL_ROWS);
        }
    }

    /// The setters of every builder reject a count or position that names no rows.
    @Test
    void settersRejectInvalidRowCounts() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE))) {
            ColumnReadersBuilder columns = reader.buildColumnReaders(ColumnProjection.columns("id"));
            ColumnReaderBuilder column = reader.buildColumnReader("id");
            assertThatThrownBy(() -> columns.head(0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("head row count must be positive: 0");
            assertThatThrownBy(() -> column.head(-1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("head row count must be positive: -1");
            assertThatThrownBy(() -> columns.skip(-1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("skip must be non-negative: -1");
            assertThatThrownBy(() -> column.skip(-5))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("skip must be non-negative: -5");
            assertThatThrownBy(() -> columns.rowRange(-1, 10))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("rowRange start must be non-negative: -1");
            assertThatThrownBy(() -> column.rowRange(10, 9))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("rowRange end must not be below its start: [10, 9)");
            assertThatThrownBy(() -> reader.buildRowReader().head(0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("head row count must be positive: 0");
        }
    }

    /// Beside a filter and a `skip`, the largest `head` a setter takes caps nothing: every builder
    /// returns each match after the skipped ones.
    @Test
    void largestHeadBesideFilterAndSkip() throws Exception {
        FilterPredicate filter = FilterPredicate.gt("id", 150L);
        List<Long> expected = longRange(161, 300);
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE))) {
            assertThat(ids(reader.buildRowReader().filter(filter).skip(10).head(Long.MAX_VALUE)))
                    .isEqualTo(expected);
            assertThat(ids(reader.buildColumnReaders(ColumnProjection.columns("id"))
                    .filter(filter).skip(10).head(Long.MAX_VALUE)))
                    .isEqualTo(expected);
        }
    }

    /// Beside a physical `skip` the read cannot mask out, one landing in a file after the first,
    /// the largest `head` a setter takes caps nothing either.
    @Test
    void largestHeadBesideUnmaskedSkip() throws Exception {
        List<Long> expected = longRange(160, 249);
        try (ParquetFileReader reader = ParquetFileReader.openAll(MULTI_FIXTURE)) {
            assertThat(ids(reader.buildRowReader().skip(160).head(Long.MAX_VALUE))).isEqualTo(expected);
            assertThat(ids(reader.buildColumnReaders(ColumnProjection.columns("id"))
                    .skip(160).head(Long.MAX_VALUE)))
                    .isEqualTo(expected);
        }
    }

    /// The single-column builder rejects a reader over several files whatever its controls, and
    /// `rowRange` on [ColumnReadersBuilder] counts rows across the files.
    @Test
    void multiFileColumnBoundaries() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.openAll(MULTI_FIXTURE)) {
            assertThatThrownBy(reader.buildColumnReader("id").rowRange(0, 10)::build)
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessage("columnReader(String) is single-file only; use columnReaders(projection) "
                            + "for multi-file readers");
            assertThatThrownBy(reader.buildColumnReaders(ColumnProjection.columns("id"))
                    .filter(RowGroupPredicate.byteRange(0, Long.MAX_VALUE)).rowRange(0, 10)::build)
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessage("filter(RowGroupPredicate) is single-file only: "
                            + "a byte range names positions in one file");
            long count = 0;
            try (ColumnReaders columns = reader.buildColumnReaders(ColumnProjection.columns("id"))
                    .rowRange(140, 160).build()) {
                while (columns.nextBatch()) {
                    count += columns.getRecordCount();
                }
            }
            assertThat(count).isEqualTo(20);
        }
    }

    /// Multi-file boundary cells (#577, #672) — physical `skip` (no filter) is a
    /// global offset across all files, logical `skip` (with a filter) counts matches
    /// across *all* files in order, and `tail` and `filter(RowGroupPredicate)` are
    /// single-file-only (throw).
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
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessage("Tail reading is not yet supported for multi-file readers");
            assertThatThrownBy(reader.buildRowReader()
                    .filter(RowGroupPredicate.byteRange(0, Long.MAX_VALUE))::build)
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessage("filter(RowGroupPredicate) is single-file only: "
                            + "a byte range names positions in one file");
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
        // Footer-only read cost of part1, derived rather than hard-coded: a
        // single-file open reads exactly the footer via the same metadata reader
        // the multi-file seek uses lazily for a skipped file.
        CountingInputFile probe = new CountingInputFile(InputFile.of(
                Paths.get("src/test/resources/multi_file_part1.parquet")));
        try (ParquetFileReader ignored = ParquetFileReader.open(probe)) {
            // open() reads the footer
        }
        int footerOnlyReads = probe.readCount();

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
                    .as("middle file should only be read for its footer, no data pages")
                    .isEqualTo(footerOnlyReads);
            assertThat(skippedMiddle.bytesRead())
                    .as("middle file should not have data pages fetched")
                    .isLessThan(skippedMiddle.length());
            assertThat(target.readCount())
                    .as("target file should be read beyond metadata to produce a row")
                    .isGreaterThan(skippedMiddle.readCount());
        }
    }

    private static List<Long> ids(RowReaderBuilder builder) throws IOException {
        List<Long> rows = new ArrayList<>();
        try (RowReader r = builder.build()) {
            while (r.hasNext()) {
                r.next();
                rows.add(r.getLong("id"));
            }
        }
        return rows;
    }

    private static List<Long> ids(ColumnReadersBuilder builder) throws IOException {
        List<Long> rows = new ArrayList<>();
        try (ColumnReaders columns = builder.build()) {
            while (columns.nextBatch()) {
                long[] ids = columns.getColumnReader("id").getLongs();
                for (int i = 0; i < columns.getRecordCount(); i++) {
                    rows.add(ids[i]);
                }
            }
        }
        return rows;
    }

    private static List<Long> longRange(long fromInclusive, long toInclusive) {
        return LongStream.rangeClosed(fromInclusive, toInclusive).boxed().toList();
    }
}
