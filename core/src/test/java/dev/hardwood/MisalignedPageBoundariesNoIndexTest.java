/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.reader.ParquetMetadataReader;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;

/// Mirrors [MisalignedPageBoundariesTest] against a fixture written without
/// a Page Index. Both columns are flat, so the per-page row mask machinery
/// in [dev.hardwood.internal.reader.SequentialFetchPlan] is responsible for
/// keeping rows aligned across columns under filter pushdown and tail-mode
/// reads — there is no `IndexedFetchPlan` to fall back on.
class MisalignedPageBoundariesNoIndexTest {

    private static final Path FIXTURE =
            Paths.get("src/test/resources/misaligned_pages_no_index.parquet");
    private static final int TOTAL_ROWS = 2_000;
    /// Width of the `row=NNNNN-` prefix produced by the fixture writer.
    private static final int ROW_PREFIX_LEN = 10;

    /// Sanity check: the fixture must not carry a Page Index, otherwise this
    /// test is silently exercising the `IndexedFetchPlan` path and would say
    /// nothing about `SequentialFetchPlan`.
    @Test
    void testFixtureLacksOffsetIndex() throws Exception {
        try (InputFile file = InputFile.of(FIXTURE)) {
            file.open();
            FileMetaData meta = ParquetMetadataReader.readMetadata(file);
            RowGroup rg = meta.rowGroups().get(0);
            for (int c = 0; c < rg.columns().size(); c++) {
                assertThat(rg.columns().get(c).offsetIndexOffset())
                        .as("column %d offsetIndexOffset", c)
                        .isNull();
            }
        }
    }

    /// Filter pushdown across two flat columns whose pages flush at different
    /// row positions. Without per-page masking on the no-index path the wide
    /// column would emit every row from offset 0 while the narrow column's
    /// inline-stats drop trimmed its first matching page. The mask path keeps
    /// the columns row-aligned.
    @Test
    void testFilterOnNarrowReturnsCorrectlyAlignedWideValues() throws Exception {
        int lo = 400;
        int hi = 1600;
        FilterPredicate filter = FilterPredicate.and(
                FilterPredicate.gtEq("narrow", lo),
                FilterPredicate.lt("narrow", hi));

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rows = reader.buildRowReader().filter(filter).build()) {
            int count = 0;
            while (rows.hasNext()) {
                rows.next();
                int narrow = rows.getInt("narrow");
                byte[] wide = rows.getBinary("wide");

                assertThat(narrow)
                        .as("row %d: narrow outside requested range", count)
                        .isGreaterThanOrEqualTo(lo)
                        .isLessThan(hi);

                String expectedPrefix = String.format("row=%05d-", narrow);
                String actualPrefix = new String(wide, 0, Math.min(ROW_PREFIX_LEN, wide.length),
                        StandardCharsets.UTF_8);
                assertThat(actualPrefix)
                        .as("row %d: wide value does not correspond to narrow=%d",
                                count, narrow)
                        .isEqualTo(expectedPrefix);
                count++;
            }
            assertThat(count)
                    .as("row count for range [%d, %d)", lo, hi)
                    .isEqualTo(hi - lo);
        }
    }

    /// Tail-mode fast path on a fixture without OffsetIndex. Synthesizes a
    /// `[skip, numRows)` matching range that flows through the same per-page
    /// mask machinery exercised by filter pushdown above; no decode-and-discard
    /// loop runs at the row-reader level.
    @Test
    void testTailReadReturnsAlignedRowsViaPerPageMaskFastPath() throws Exception {
        int tailRows = 600;
        int firstExpectedRow = TOTAL_ROWS - tailRows;

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rows = reader.buildRowReader().tail(tailRows).build()) {
            int expected = firstExpectedRow;
            while (rows.hasNext()) {
                rows.next();
                int narrow = rows.getInt("narrow");
                byte[] wide = rows.getBinary("wide");
                assertThat(narrow).as("row offset %d narrow", expected - firstExpectedRow)
                        .isEqualTo(expected);

                String expectedPrefix = String.format("row=%05d-", expected);
                String actualPrefix = new String(wide, 0, ROW_PREFIX_LEN, StandardCharsets.UTF_8);
                assertThat(actualPrefix).as("row offset %d wide", expected - firstExpectedRow)
                        .isEqualTo(expectedPrefix);
                expected++;
            }
            assertThat(expected - firstExpectedRow)
                    .as("tail row count")
                    .isEqualTo(tailRows);
        }
    }

    /// Regression guard: an unfiltered full scan must keep columns paired
    /// when masks are inactive (the fast path that existed before any of
    /// this work).
    @Test
    void testFullScanStillPairsColumnsCorrectly() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rows = reader.rowReader()) {
            int expected = 0;
            while (rows.hasNext()) {
                rows.next();
                int narrow = rows.getInt("narrow");
                byte[] wide = rows.getBinary("wide");
                assertThat(narrow).as("row %d narrow", expected).isEqualTo(expected);

                String expectedPrefix = String.format("row=%05d-", expected);
                String actualPrefix = new String(wide, 0, ROW_PREFIX_LEN, StandardCharsets.UTF_8);
                assertThat(actualPrefix).as("row %d wide", expected).isEqualTo(expectedPrefix);
                expected++;
            }
            assertThat(expected).isEqualTo(TOTAL_ROWS);
        }
    }
}
