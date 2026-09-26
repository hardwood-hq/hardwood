/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;

import static org.assertj.core.api.Assertions.assertThat;

/// The page-index requests of whole reads, against the slices each read needs.
///
/// Uses page_index_windows.parquet: twelve row groups of 400 rows over twenty INT64 columns
/// `c00`..`c19`, pages of 100 rows, a page index. `c00` counts 0..399 within every row group.
/// At the default budget the twelve row groups are one window.
class IndexWindowIoTest {

    private static final Path FIXTURE = Path.of("src/test/resources/page_index_windows.parquet");
    private static final int C00 = 0;
    private static final int C05 = 5;
    private static final int[][] WINDOWS = { { 0, 11 } };

    @Test
    void aFilteredReadFetchesEachWindowInOneRequestPerStructure() throws Exception {
        FileMetaData metaData = metaData();
        CountingInputFile file = countingFile();
        int rows = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(file);
                RowReader rowReader = reader.buildRowReader()
                        .projection(ColumnProjection.columns("c00", "c05"))
                        .filter(FilterPredicate.lt("c00", 100L))
                        .build()) {
            while (rowReader.hasNext()) {
                rowReader.next();
                rows++;
            }
        }

        assertThat(rows).isEqualTo(12 * 100);
        // Per window: the ColumnIndex of the filter column, and the OffsetIndexes of the two
        // read columns, from the first member's first slice to the last member's last one.
        List<CountingInputFile.Read> expected = new ArrayList<>();
        for (int[] window : WINDOWS) {
            String reason = reason(window);
            expected.add(new CountingInputFile.Read(columnIndexStart(metaData, window[0], C00),
                    length(columnIndexStart(metaData, window[0], C00), columnIndexEnd(metaData, window[1], C00)),
                    reason));
            expected.add(new CountingInputFile.Read(offsetIndexStart(metaData, window[0], C00),
                    length(offsetIndexStart(metaData, window[0], C00), offsetIndexEnd(metaData, window[1], C05)),
                    reason));
        }
        assertThat(indexReads(file)).containsExactlyInAnyOrderElementsOf(expected);
        assertNoByteFetchedTwice(indexReads(file));
    }

    @Test
    void windowsOfAFilteredReadFetchDisjointBytesOneRequestPerStructureEach() throws Exception {
        CountingInputFile file = countingFile();
        try (ParquetFileReader reader = ParquetFileReader.open(file);
                RowReader rowReader = IndexWindowLifecycleTest.withBudget(4000, () -> reader.buildRowReader()
                        .projection(ColumnProjection.columns("c00", "c05"))
                        .filter(FilterPredicate.lt("c00", 100L))
                        .build())) {
            while (rowReader.hasNext()) {
                rowReader.next();
            }
        }

        List<CountingInputFile.Read> reads = indexReads(file);
        Map<String, Long> requestsPerWindow = reads.stream()
                .collect(Collectors.groupingBy(CountingInputFile.Read::reason, Collectors.counting()));
        assertThat(requestsPerWindow).hasSizeGreaterThan(1);
        assertThat(requestsPerWindow.values()).containsOnly(2L);
        assertNoByteFetchedTwice(reads);
    }

    @Test
    void anUnfilteredReadFetchesTheOffsetIndexesOfItsColumnsOnly() throws Exception {
        FileMetaData metaData = metaData();
        CountingInputFile file = countingFile();
        try (ParquetFileReader reader = ParquetFileReader.open(file);
                RowReader rowReader = reader.buildRowReader()
                        .projection(ColumnProjection.columns("c05"))
                        .build()) {
            while (rowReader.hasNext()) {
                rowReader.next();
            }
        }

        List<CountingInputFile.Read> expected = new ArrayList<>();
        for (int[] window : WINDOWS) {
            expected.add(new CountingInputFile.Read(offsetIndexStart(metaData, window[0], C05),
                    length(offsetIndexStart(metaData, window[0], C05), offsetIndexEnd(metaData, window[1], C05)),
                    reason(window)));
        }
        assertThat(indexReads(file)).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    void aReadStatisticsProveFetchesNoColumnIndexAndNoIndexOfItsFilterOnlyColumn() throws Exception {
        FileMetaData metaData = metaData();
        CountingInputFile file = countingFile();
        int rows = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(file);
                RowReader rowReader = reader.buildRowReader()
                        .projection(ColumnProjection.columns("c05"))
                        .filter(FilterPredicate.gtEq("c00", 0L))
                        .build()) {
            while (rowReader.hasNext()) {
                rowReader.next();
                rows++;
            }
        }

        // Statistics prove every row group, so the read is the unfiltered read of `c05`.
        assertThat(rows).isEqualTo(12 * 400);
        List<CountingInputFile.Read> expected = new ArrayList<>();
        for (int[] window : WINDOWS) {
            expected.add(new CountingInputFile.Read(offsetIndexStart(metaData, window[0], C05),
                    length(offsetIndexStart(metaData, window[0], C05), offsetIndexEnd(metaData, window[1], C05)),
                    reason(window)));
        }
        assertThat(indexReads(file)).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    void aHeadWithinTheFirstRowGroupFetchesThatRowGroupsSlicesOnly() throws Exception {
        FileMetaData metaData = metaData();
        CountingInputFile file = countingFile();
        try (ParquetFileReader reader = ParquetFileReader.open(file);
                RowReader rowReader = reader.buildRowReader()
                        .projection(ColumnProjection.columns("c05"))
                        .head(50)
                        .build()) {
            while (rowReader.hasNext()) {
                rowReader.next();
            }
        }

        ColumnChunk chunk = metaData.rowGroups().get(0).columns().get(C05);
        assertThat(indexReads(file)).containsExactly(new CountingInputFile.Read(
                chunk.offsetIndexOffset(), chunk.offsetIndexLength(), "rg=0 indexes"));
    }

    private static List<CountingInputFile.Read> indexReads(CountingInputFile file) {
        return file.reads().stream().filter(read -> read.reason().endsWith(" indexes")).toList();
    }

    private static void assertNoByteFetchedTwice(List<CountingInputFile.Read> reads) {
        for (int i = 0; i < reads.size(); i++) {
            for (int j = i + 1; j < reads.size(); j++) {
                CountingInputFile.Read a = reads.get(i);
                CountingInputFile.Read b = reads.get(j);
                assertThat(a.end() <= b.offset() || b.end() <= a.offset())
                        .as("%s and %s overlap", a, b)
                        .isTrue();
            }
        }
    }

    private static String reason(int[] window) {
        return window[0] == window[1]
                ? "rg=" + window[0] + " indexes"
                : "rg=" + window[0] + "-" + window[1] + " indexes";
    }

    private static long columnIndexStart(FileMetaData metaData, int rowGroup, int column) {
        return chunk(metaData, rowGroup, column).columnIndexOffset();
    }

    private static long columnIndexEnd(FileMetaData metaData, int rowGroup, int column) {
        ColumnChunk chunk = chunk(metaData, rowGroup, column);
        return chunk.columnIndexOffset() + chunk.columnIndexLength();
    }

    private static long offsetIndexStart(FileMetaData metaData, int rowGroup, int column) {
        return chunk(metaData, rowGroup, column).offsetIndexOffset();
    }

    private static long offsetIndexEnd(FileMetaData metaData, int rowGroup, int column) {
        ColumnChunk chunk = chunk(metaData, rowGroup, column);
        return chunk.offsetIndexOffset() + chunk.offsetIndexLength();
    }

    private static ColumnChunk chunk(FileMetaData metaData, int rowGroup, int column) {
        return metaData.rowGroups().get(rowGroup).columns().get(column);
    }

    private static int length(long start, long end) {
        return Math.toIntExact(end - start);
    }

    private static FileMetaData metaData() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE))) {
            return reader.getFileMetaData();
        }
    }

    private static CountingInputFile countingFile() throws Exception {
        CountingInputFile file = new CountingInputFile(InputFile.of(FIXTURE));
        file.open();
        return file;
    }
}
