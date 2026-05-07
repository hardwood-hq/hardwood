/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowGroupPredicate;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Integration tests for [RowGroupPredicate] across all reader builders.
///
/// Exercises `filter_pushdown_int.parquet` — a 3-row-group file with `id` 1-100, 101-200,
/// 201-300 — and verifies that byte-range filtering yields the expected row-group subsets
/// for [ParquetFileReader#buildColumnReader], [ParquetFileReader#buildColumnReaders], and
/// [ParquetFileReader#buildRowReader].
class RowGroupFilterTest {

    private static final Path FIXTURE = Paths.get("src/test/resources/filter_pushdown_int.parquet");

    private static long rg0Mid;
    private static long rg1Mid;
    private static long rg2Mid;
    private static long fileLen;

    @BeforeAll
    static void readMidpoints() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE))) {
            List<RowGroup> rgs = reader.getFileMetaData().rowGroups();
            assertThat(rgs).hasSize(3);
            rg0Mid = midpoint(rgs.get(0));
            rg1Mid = midpoint(rgs.get(1));
            rg2Mid = midpoint(rgs.get(2));
            fileLen = FIXTURE.toFile().length();
        }
        assertThat(rg0Mid).isLessThan(rg1Mid);
        assertThat(rg1Mid).isLessThan(rg2Mid);
    }

    private static long midpoint(RowGroup rg) {
        long start = rg.columns().get(0).chunkStartOffset();
        long compressed = 0;
        for (var c : rg.columns()) {
            compressed += c.metaData().totalCompressedSize();
        }
        return start + compressed / 2;
    }

    // ==================== ColumnReader path ====================

    @Test
    void columnReaderByteRangeCoveringEverythingReadsAllRows() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE));
             ColumnReader col = reader.buildColumnReader("id")
                     .filter(RowGroupPredicate.byteRange(0, fileLen))
                     .build()) {
            assertThat(countRows(col)).isEqualTo(300);
        }
    }

    @Test
    void columnReaderByteRangeCoveringNothingReadsZeroRows() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE));
             ColumnReader col = reader.buildColumnReader("id")
                     .filter(RowGroupPredicate.byteRange(fileLen + 1, fileLen + 100))
                     .build()) {
            assertThat(countRows(col)).isZero();
        }
    }

    @Test
    void columnReaderEmptyByteRangeReadsZeroRows() throws Exception {
        // endExclusive < startInclusive → empty range. Used by callers that pass
        // splitStart + splitLength and tolerate long overflow on tail splits.
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE));
             ColumnReader col = reader.buildColumnReader("id")
                     .filter(RowGroupPredicate.byteRange(100, 50))
                     .build()) {
            assertThat(countRows(col)).isZero();
        }
    }

    @Test
    void columnReaderByteRangeKeepsOnlyRowGroupsByMidpointRule() throws Exception {
        // Range covering only rg0 (up to but not including rg1's midpoint).
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE));
             ColumnReader col = reader.buildColumnReader("id")
                     .filter(RowGroupPredicate.byteRange(0, rg1Mid))
                     .build()) {
            assertThat(firstAndLastIds(col)).containsExactly(1L, 100L);
        }
    }

    @Test
    void columnReaderByteRangeKeepsTwoComplementaryHalvesExactly() throws Exception {
        // Two disjoint ranges should partition the file exactly: every row group lands in
        // exactly one of them.
        long boundary = rg1Mid; // midpoints: rg0 < rg1 < rg2; boundary at rg1's midpoint
        long firstHalfRows = countWithRange(0, boundary);
        long secondHalfRows = countWithRange(boundary, Long.MAX_VALUE);
        assertThat(firstHalfRows + secondHalfRows).isEqualTo(300L);

        // Sanity: the boundary cuts between rg0 and rg1, so first half = rg0 only.
        assertThat(firstHalfRows).isEqualTo(100L);
        assertThat(secondHalfRows).isEqualTo(200L);
    }

    @Test
    void columnReaderAndCombinatorIntersectsRanges() throws Exception {
        // Range A covers rg0+rg1; range B covers rg1+rg2; intersection is rg1.
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE));
             ColumnReader col = reader.buildColumnReader("id")
                     .filter(RowGroupPredicate.and(
                             RowGroupPredicate.byteRange(0, rg2Mid),
                             RowGroupPredicate.byteRange(rg1Mid, fileLen)))
                     .build()) {
            assertThat(firstAndLastIds(col)).containsExactly(101L, 200L);
        }
    }

    @Test
    void columnReaderRowGroupFilterComposesWithFilterPredicate() throws Exception {
        // Byte range keeps rg0+rg1 (id 1-200). FilterPredicate gt(id, 150) drops rg0
        // (max=100) and keeps rg1 (max=200). Intersection: rg1 only.
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE));
             ColumnReader col = reader.buildColumnReader("id")
                     .filter(FilterPredicate.gt("id", 150L))
                     .filter(RowGroupPredicate.byteRange(0, rg2Mid))
                     .build()) {
            assertThat(firstAndLastIds(col)).containsExactly(101L, 200L);
        }
    }

    // ==================== ColumnReaders path ====================

    @Test
    void columnReadersByteRangeKeepsOnlyExpectedRowGroups() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE));
             ColumnReaders cols = reader.buildColumnReaders(ColumnProjection.columns("id"))
                     .filter(RowGroupPredicate.byteRange(0, rg1Mid))
                     .build()) {
            ColumnReader col = cols.getColumnReader("id");
            assertThat(firstAndLastIds(col)).containsExactly(1L, 100L);
        }
    }

    // ==================== RowReader path ====================

    @Test
    void rowReaderByteRangeKeepsOnlyExpectedRowGroups() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rows = reader.buildRowReader()
                     .projection(ColumnProjection.columns("id"))
                     .filter(RowGroupPredicate.byteRange(0, rg1Mid))
                     .build()) {
            int n = 0;
            long firstId = -1;
            long lastId = -1;
            while (rows.hasNext()) {
                rows.next();
                long id = rows.getLong("id");
                if (firstId < 0) firstId = id;
                lastId = id;
                n++;
            }
            assertThat(n).isEqualTo(100);
            assertThat(firstId).isEqualTo(1L);
            assertThat(lastId).isEqualTo(100L);
        }
    }

    @Test
    void rowReaderHeadComposesOverFilteredRowGroups() throws Exception {
        // RowGroupPredicate keeps rg1+rg2 (rows 101..300); head(50) caps at first 50 of those.
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rows = reader.buildRowReader()
                     .projection(ColumnProjection.columns("id"))
                     .filter(RowGroupPredicate.byteRange(rg1Mid, fileLen))
                     .head(50)
                     .build()) {
            long firstId = -1;
            long lastId = -1;
            int n = 0;
            while (rows.hasNext()) {
                rows.next();
                long id = rows.getLong("id");
                if (firstId < 0) firstId = id;
                lastId = id;
                n++;
            }
            assertThat(n).isEqualTo(50);
            assertThat(firstId).isEqualTo(101L);
            assertThat(lastId).isEqualTo(150L);
        }
    }

    @Test
    void rowReaderFirstRowIndexesIntoFilteredSequence() throws Exception {
        // RowGroupPredicate keeps rg1+rg2 (rows 101..300 in id order). firstRow(10) skips
        // 10 rows of the kept set, so we resume at id 111.
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rows = reader.buildRowReader()
                     .projection(ColumnProjection.columns("id"))
                     .filter(RowGroupPredicate.byteRange(rg1Mid, fileLen))
                     .firstRow(10)
                     .build()) {
            assertThat(rows.hasNext()).isTrue();
            rows.next();
            assertThat(rows.getLong("id")).isEqualTo(111L);
        }
    }

    @Test
    void rowReaderTailRejectsRowGroupPredicate() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE))) {
            assertThatThrownBy(() -> reader.buildRowReader()
                    .projection(ColumnProjection.columns("id"))
                    .filter(RowGroupPredicate.byteRange(0, fileLen))
                    .tail(50)
                    .build())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("tail")
                    .hasMessageContaining("row-group filter");
        }
    }

    // ==================== Helpers ====================

    private static long countRows(ColumnReader col) {
        long n = 0;
        while (col.nextBatch()) {
            n += col.getRecordCount();
        }
        return n;
    }

    private static long countWithRange(long start, long end) throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE));
             ColumnReader col = reader.buildColumnReader("id")
                     .filter(RowGroupPredicate.byteRange(start, end))
                     .build()) {
            return countRows(col);
        }
    }

    private static long[] firstAndLastIds(ColumnReader col) {
        long first = -1;
        long last = -1;
        while (col.nextBatch()) {
            int n = col.getRecordCount();
            long[] ids = col.getLongs();
            if (first < 0 && n > 0) first = ids[0];
            if (n > 0) last = ids[n - 1];
        }
        return new long[] {first, last};
    }
}
