/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.internal.io.CountingInputFile;
import dev.hardwood.internal.reader.fetch.IoBudget;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetFileReader.ColumnReadersBuilder;
import dev.hardwood.reader.RowGroupPredicate;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;

import static org.assertj.core.api.Assertions.assertThat;

/// `rowRange` on the column readers: which rows it returns where pruning, row-group predicates,
/// file boundaries and the mask-capability gate take part, and that it fetches no data page
/// outside the range at either end. The semantics over the plain cases are checked against DuckDB
/// in [DifferentialColumnReadTest].
class ColumnReaderRowRangeTest {

    private static final Path RES = Paths.get("src/test/resources");
    /// One row group of 10000 rows in pages of 1024, with a page index; `id` is the row and
    /// `value` is `id + 1000`.
    private static final Path ONE_ROW_GROUP = RES.resolve("column_index_pushdown.parquet");
    /// Twelve row groups of 400 rows in pages of 100, with a page index; row `i` holds
    /// `c00 = i % 400` and `c01 = 20 * i + 1`.
    private static final Path ROW_GROUPS = RES.resolve("page_index_windows.parquet");
    /// Three row groups of 100 rows, `id` 1..300, with statistics and no page index.
    private static final Path PRUNABLE = RES.resolve("filter_pushdown_int.parquet");
    /// One row group of 2000 rows, a list column in v1 pages and no page index, which closes the
    /// mask-capability gate; `narrow` is the row, and each row's first tag names it.
    private static final Path NESTED_V1 = RES.resolve("nested_v1_no_index.parquet");
    /// Two row groups of 250 rows, a dictionary-encoded list column in v2 pages and no page
    /// index, which closes the gate; `narrow` is the row, and each row's first tag names it.
    private static final Path NESTED_ROW_GROUPS = RES.resolve("nested_dict_v2.parquet");
    /// `id` is the row across both files: 0..149, then 150..249.
    private static final Path PART0 = RES.resolve("multi_file_part0.parquet");
    private static final Path PART1 = RES.resolve("multi_file_part1.parquet");

    @Test
    void rangeInsideOneRowGroupFetchesOnlyThePagesItOverlaps() throws Exception {
        CountingInputFile file = counting(ONE_ROW_GROUP);
        List<Long> ids;
        try (ParquetFileReader reader = ParquetFileReader.open(file)) {
            ids = longs(reader.buildColumnReader("id").rowRange(1500, 4500).build());
        }
        assertThat(ids).containsExactlyElementsOf(range(1500, 4500));
        IoBudget.of(ONE_ROW_GROUP, List.of("id"), 1500, 4500).assertWithin(file);
    }

    @Test
    void filteredRangeFetchesOnlyThePagesTheRangeOverlaps() throws Exception {
        CountingInputFile file = counting(ONE_ROW_GROUP);
        List<Long> ids;
        try (ParquetFileReader reader = ParquetFileReader.open(file)) {
            ids = longs(reader.buildColumnReader("id")
                    .filter(FilterPredicate.gtEq("value", 3000L))
                    .rowRange(1500, 4500)
                    .build());
        }
        assertThat(ids).containsExactlyElementsOf(range(2000, 4500));
        IoBudget.of(ONE_ROW_GROUP, List.of("id", "value"), 1500, 4500).assertWithin(file);
    }

    @Test
    void rangeAcrossRowGroupsFetchesOnlyThePagesItOverlaps() throws Exception {
        CountingInputFile file = counting(ROW_GROUPS);
        List<Long> rows = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(file)) {
            for (long c01 : longs(reader.buildColumnReader("c01").rowRange(450, 1250).build())) {
                rows.add((c01 - 1) / 20);
            }
        }
        assertThat(rows).containsExactlyElementsOf(range(450, 1250));
        IoBudget.of(ROW_GROUPS, List.of("c01"), 450, 1250).assertWithin(file);
    }

    @Test
    void filteredRangeAcrossRowGroupsFetchesOnlyThePagesTheRangeOverlaps() throws Exception {
        // c00 < 100 keeps the first page of every row group; the range starts inside the first
        // page of row group 1 and ends inside the first page of row group 3
        CountingInputFile file = counting(ROW_GROUPS);
        List<Long> rows = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(file);
             ColumnReaders columns = reader.buildColumnReaders(ColumnProjection.columns("c01"))
                     .filter(FilterPredicate.lt("c00", 100L))
                     .rowRange(450, 1250)
                     .build()) {
            while (columns.nextBatch()) {
                long[] c01 = columns.getColumnReader("c01").getLongs();
                for (int i = 0; i < columns.getRecordCount(); i++) {
                    rows.add((c01[i] - 1) / 20);
                }
            }
        }
        assertThat(rows).containsExactlyElementsOf(concat(range(450, 500), range(800, 900), range(1200, 1250)));
        IoBudget.of(ROW_GROUPS, List.of("c01", "c00"), 450, 1250).assertWithin(file);
    }

    @Test
    void filteredRangeEndingBeforeTheMatchesOfItsLastRowGroup() throws Exception {
        // c00 >= 300 keeps the last page of every row group; the range ends inside row group 3
        // before its last page, so the row group it ends in returns nothing
        CountingInputFile file = counting(ROW_GROUPS);
        List<Long> rows = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(file)) {
            for (long c01 : longs(reader.buildColumnReader("c01")
                    .filter(FilterPredicate.gtEq("c00", 300L))
                    .rowRange(450, 1250)
                    .build())) {
                rows.add((c01 - 1) / 20);
            }
        }
        assertThat(rows).containsExactlyElementsOf(concat(range(700, 800), range(1100, 1200)));
        IoBudget.of(ROW_GROUPS, List.of("c01", "c00"), 450, 1250).assertWithin(file);
    }

    @Test
    void rangeStartingInRowGroupStatisticsPruneCountsItsRows() throws Exception {
        // id > 150 prunes row group 0, in which the range starts
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(PRUNABLE))) {
            assertThat(longs(reader.buildColumnReader("id")
                    .filter(FilterPredicate.gt("id", 150L))
                    .rowRange(50, 250)
                    .build()))
                    .containsExactlyElementsOf(range(151, 251));
        }
    }

    @Test
    void rangeUnderFilterCountsPositionsWhereSkipCountsMatches() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(PRUNABLE))) {
            FilterPredicate filter = FilterPredicate.gt("id", 150L);
            assertThat(longs(reader.buildColumnReader("id").filter(filter).skip(50).build()))
                    .containsExactlyElementsOf(range(201, 301));
            assertThat(longs(reader.buildColumnReader("id").filter(filter).rowRange(50, 300).build()))
                    .containsExactlyElementsOf(range(151, 301));
        }
    }

    @Test
    void rangeCountsTheRowGroupsRowGroupPredicateKeeps() throws Exception {
        long fileLength = PRUNABLE.toFile().length();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(PRUNABLE))) {
            // keeps row groups 1 and 2, ids 101..300
            RowGroupPredicate laterRowGroups = RowGroupPredicate.byteRange(
                    midpoint(reader.getFileMetaData().rowGroups().get(1)), fileLength);
            List<Long> ranged = longs(reader.buildColumnReader("id")
                    .filter(laterRowGroups)
                    .rowRange(50, 150)
                    .build());
            assertThat(ranged).containsExactlyElementsOf(range(151, 251));

            List<Long> skipped = new ArrayList<>();
            try (RowReader rows = reader.buildRowReader().filter(laterRowGroups).skip(50).head(100).build()) {
                while (rows.hasNext()) {
                    rows.next();
                    skipped.add(rows.getLong("id"));
                }
            }
            assertThat(ranged).isEqualTo(skipped);
        }
    }

    @Test
    void rangeCountsRowsAcrossFiles() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.openAll(InputFile.ofPaths(PART0, PART1))) {
            assertThat(longs(reader.buildColumnReaders(ColumnProjection.columns("id")).rowRange(125, 175), "id"))
                    .containsExactlyElementsOf(range(125, 175));
            assertThat(longs(reader.buildColumnReaders(ColumnProjection.columns("id"))
                    .filter(FilterPredicate.gt("id", 160L))
                    .rowRange(125, 175), "id"))
                    .containsExactlyElementsOf(range(161, 175));
            assertThat(longs(reader.buildColumnReaders(ColumnProjection.columns("id")).rowRange(240, 400), "id"))
                    .containsExactlyElementsOf(range(240, 250));
        }
    }

    @Test
    void rangeInLaterFileReadsNoDataOfEarlierFile() throws Exception {
        CountingInputFile part0 = counting(PART0);
        CountingInputFile part1 = counting(PART1);
        try (ParquetFileReader reader = ParquetFileReader.openAll(List.of(part0, part1))) {
            int part0ReadsAfterOpen = part0.readCount();
            assertThat(longs(reader.buildColumnReaders(ColumnProjection.columns("id"))
                    .filter(FilterPredicate.lt("id", 165L))
                    .rowRange(160, 170), "id"))
                    .containsExactlyElementsOf(range(160, 165));
            assertThat(part0.readCount())
                    .as("the first file is read for its footer only")
                    .isEqualTo(part0ReadsAfterOpen);
        }
    }

    @Test
    void rangeEndingInPrunedRowGroupListsNoFurtherFile(@TempDir Path dir) throws Exception {
        // id < 75 prunes the last row group of the first file, in which the range ends, and every
        // row group of the files after it
        CountingInputFile part0 = counting(PART0);
        CountingInputFile part1 = counting(PART1);
        CountingInputFile part2 = counting(Files.copy(PART1, dir.resolve("part2.parquet")));
        try (ParquetFileReader reader = ParquetFileReader.openAll(List.of(part0, part1, part2))) {
            int part2ReadsAfterOpen = part2.readCount();
            assertThat(longs(reader.buildColumnReaders(ColumnProjection.columns("id"))
                    .filter(FilterPredicate.lt("id", 75L))
                    .rowRange(60, 120), "id"))
                    .containsExactlyElementsOf(range(60, 75));
            assertThat(part2.readCount())
                    .as("no file past the one the range ends in is listed")
                    .isEqualTo(part2ReadsAfterOpen);
        }
    }

    static Stream<Arguments> nestedWithoutOffsetIndex() {
        return Stream.of(
                Arguments.of("range", NESTED_V1,
                        (UnaryOperator<ColumnReadersBuilder>) b -> b.rowRange(300, 1700), range(300, 1700)),
                Arguments.of("range + filter below its end", NESTED_V1,
                        (UnaryOperator<ColumnReadersBuilder>) b -> b.filter(FilterPredicate.lt("narrow", 1000))
                                .rowRange(300, 1700), range(300, 1000)),
                Arguments.of("range + filter above its start", NESTED_V1,
                        (UnaryOperator<ColumnReadersBuilder>) b -> b.filter(FilterPredicate.gt("narrow", 1500))
                                .rowRange(300, 1700), range(1501, 1700)),
                Arguments.of("skip + head", NESTED_V1,
                        (UnaryOperator<ColumnReadersBuilder>) b -> b.skip(300).head(100), range(300, 400)),
                Arguments.of("filter + skip + head", NESTED_V1,
                        (UnaryOperator<ColumnReadersBuilder>) b -> b.filter(FilterPredicate.lt("narrow", 1000))
                                .skip(10).head(5), range(10, 15)),
                Arguments.of("range across row groups", NESTED_ROW_GROUPS,
                        (UnaryOperator<ColumnReadersBuilder>) b -> b.rowRange(100, 400), range(100, 400)),
                // statistics prune row group 0, in which the range starts
                Arguments.of("range starting in a pruned row group", NESTED_ROW_GROUPS,
                        (UnaryOperator<ColumnReadersBuilder>) b -> b.filter(FilterPredicate.gtEq("narrow", 300))
                                .rowRange(100, 400), range(300, 400)));
    }

    /// A nested column without an OffsetIndex closes the mask-capability gate: the rows before the
    /// range's start or the skip are decoded and dropped by the reader, and the read ends at the
    /// range's end.
    @ParameterizedTest(name = "{0}")
    @MethodSource("nestedWithoutOffsetIndex")
    void nestedColumnWithoutOffsetIndex(String name, Path file, UnaryOperator<ColumnReadersBuilder> controls,
                                        List<Long> expected) throws Exception {
        List<Long> rows = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
             ColumnReaders columns = controls.apply(reader.buildColumnReaders(
                     ColumnProjection.columns("narrow", "tags.list.element"))).build()) {
            while (columns.nextBatch()) {
                int[] narrow = columns.getColumnReader("narrow").getInts();
                ColumnReader tags = columns.getColumnReader("tags.list.element");
                String[] tagValues = tags.getStrings();
                assertThat(tags.getValueCount()).isEqualTo(2 * columns.getRecordCount());
                for (int i = 0; i < columns.getRecordCount(); i++) {
                    assertThat(tagValues[2 * i]).isEqualTo(String.format("row=%05d", narrow[i]));
                    rows.add((long) narrow[i]);
                }
            }
        }
        assertThat(rows).containsExactlyElementsOf(expected);
    }

    private static CountingInputFile counting(Path path) throws IOException {
        CountingInputFile file = new CountingInputFile(InputFile.of(path));
        file.open();
        return file;
    }

    private static List<Long> longs(ColumnReader column) throws IOException {
        List<Long> values = new ArrayList<>();
        try (column) {
            while (column.nextBatch()) {
                long[] batch = column.getLongs();
                for (int i = 0; i < column.getRecordCount(); i++) {
                    values.add(batch[i]);
                }
            }
        }
        return values;
    }

    private static List<Long> longs(ColumnReadersBuilder builder, String column) throws IOException {
        List<Long> values = new ArrayList<>();
        try (ColumnReaders columns = builder.build()) {
            while (columns.nextBatch()) {
                long[] batch = columns.getColumnReader(column).getLongs();
                for (int i = 0; i < columns.getRecordCount(); i++) {
                    values.add(batch[i]);
                }
            }
        }
        return values;
    }

    private static List<Long> range(long from, long to) {
        return LongStream.range(from, to).boxed().toList();
    }

    @SafeVarargs
    private static List<Long> concat(List<Long>... parts) {
        return Stream.of(parts).flatMap(List::stream).toList();
    }

    private static long midpoint(RowGroup rg) {
        long compressed = 0;
        for (ColumnChunk chunk : rg.columns()) {
            compressed += chunk.metaData().totalCompressedSize();
        }
        return rg.columns().get(0).chunkStartOffset() + compressed / 2;
    }
}
