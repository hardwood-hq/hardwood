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
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.reader.ColumnIndexBuffers;
import dev.hardwood.internal.reader.ParquetMetadataReader;
import dev.hardwood.internal.reader.RowGroupIndexBuffers;
import dev.hardwood.internal.thrift.OffsetIndexReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.OffsetIndex;
import dev.hardwood.metadata.PageLocation;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;

/// Reproduces the silent data-corruption bug when per-column page boundaries diverge
/// within a row group (hardwood-hq/hardwood#277).
///
/// The fixture has two columns whose pages flush at different row positions
/// because per-column page size is driven by uncompressed byte count:
///
/// - `narrow` INT32 (4 B/value): 10 pages, boundaries at multiples of 1037
/// - `wide`   BYTE_ARRAY (~96 B/value): ~197 pages, boundaries at multiples of 51
///
/// `gcd(1037, 51) = 17`, so narrow page boundaries do not coincide with wide
/// boundaries — any filter threshold produces different first-kept-page start
/// rows on the two columns. Each `wide` row encodes its own row index so
/// row-to-row alignment across columns is directly verifiable.
class MisalignedPageBoundariesTest {

    private static final Path MISALIGNED_FILE =
            Paths.get("src/test/resources/misaligned_pages.parquet");
    /// Flat fixture without ColumnIndex/OffsetIndex. Both projected columns
    /// are flat, so the per-page mask gate opens for tail-mode reads and the
    /// fast path applies even though there is no Page Index.
    private static final Path INLINE_STATS_FILE =
            Paths.get("src/test/resources/inline_page_stats.parquet");
    private static final int TOTAL_ROWS = 10_000;

    @Test
    void fixtureHasDivergentPageBoundaries() throws Exception {
        try (InputFile file = InputFile.of(MISALIGNED_FILE)) {
            file.open();
            FileMetaData meta = ParquetMetadataReader.readMetadata(file, null, null);
            RowGroup rg = meta.rowGroups().get(0);
            RowGroupIndexBuffers buffers = RowGroupIndexBuffers.fetch(file, rg);

            OffsetIndex narrowIndex = readOffsetIndex(buffers.forColumn(0));
            OffsetIndex wideIndex = readOffsetIndex(buffers.forColumn(1));

            int narrowPages = narrowIndex.pageLocations().size();
            int widePages = wideIndex.pageLocations().size();

            assertThat(narrowPages)
                    .as("narrow column should have far fewer pages than wide")
                    .isLessThan(widePages / 4);

            // For the corruption to reproduce, the first page that overlaps the
            // filter threshold must start at different rows on the two columns.
            long narrowStart = firstPageStartCovering(narrowIndex, 2000);
            long wideStart = firstPageStartCovering(wideIndex, 2000);
            assertThat(narrowStart)
                    .as("narrow and wide should not agree on the first kept page for row 2000")
                    .isNotEqualTo(wideStart);
        }
    }

    @Test
    void filterOnNarrowReturnsCorrectlyAlignedWideValues() throws Exception {
        int lo = 2000;
        int hi = 8000;
        FilterPredicate filter = FilterPredicate.and(
                FilterPredicate.gtEq("narrow", lo),
                FilterPredicate.lt("narrow", hi));

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(MISALIGNED_FILE));
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

                String expectedPrefix = String.format("row=%08d-", narrow);
                String actualPrefix = new String(wide, 0, Math.min(13, wide.length),
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

    @Test
    void tailReadReturnsAlignedRowsViaPerPageMaskFastPath() throws Exception {
        // Read the last 3000 rows; misaligned per-column page boundaries mean
        // narrow's first kept page starts at a different global row from wide's,
        // so the tail-read fast path must apply per-page leading-skip on each
        // column independently to keep them aligned.
        int tailRows = 3000;
        int firstExpectedRow = TOTAL_ROWS - tailRows;

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(MISALIGNED_FILE));
             RowReader rows = reader.buildRowReader().tail(tailRows).build()) {
            int expected = firstExpectedRow;
            while (rows.hasNext()) {
                rows.next();
                int narrow = rows.getInt("narrow");
                byte[] wide = rows.getBinary("wide");
                assertThat(narrow).as("row offset %d narrow", expected - firstExpectedRow)
                        .isEqualTo(expected);

                String expectedPrefix = String.format("row=%08d-", expected);
                String actualPrefix = new String(wide, 0, 13, StandardCharsets.UTF_8);
                assertThat(actualPrefix).as("row offset %d wide", expected - firstExpectedRow)
                        .isEqualTo(expectedPrefix);
                expected++;
            }
            assertThat(expected - firstExpectedRow)
                    .as("tail row count")
                    .isEqualTo(tailRows);
        }
    }

    @Test
    void tailReadAppliesPageMaskFastPathOnNoIndexFlatFile() throws Exception {
        // inline_page_stats.parquet has no OffsetIndex but its columns are
        // flat, so the row-group-wide mask gate opens and tail-mode runs
        // through the SequentialFetchPlan per-page mask path. The original
        // shape of this test asserted decode-and-discard fallback; with
        // mask support extended to flat columns the fast path now applies
        // here, and correctness is verified the same way.
        int tailRows = 1500;
        int firstExpectedRow = TOTAL_ROWS - tailRows;

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(INLINE_STATS_FILE));
             RowReader rows = reader.buildRowReader().tail(tailRows).build()) {
            int expected = firstExpectedRow;
            while (rows.hasNext()) {
                rows.next();
                long id = rows.getLong("id");
                long value = rows.getLong("value");
                assertThat(id).as("row offset %d id", expected - firstExpectedRow)
                        .isEqualTo(expected);
                assertThat(value).as("row offset %d value", expected - firstExpectedRow)
                        .isEqualTo(expected + 1000L);
                expected++;
            }
            assertThat(expected - firstExpectedRow)
                    .as("tail row count")
                    .isEqualTo(tailRows);
        }
    }

    @Test
    void fullScanStillPairsColumnsCorrectly() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(MISALIGNED_FILE));
             RowReader rows = reader.rowReader()) {
            int expected = 0;
            while (rows.hasNext()) {
                rows.next();
                int narrow = rows.getInt("narrow");
                byte[] wide = rows.getBinary("wide");
                assertThat(narrow).as("row %d narrow", expected).isEqualTo(expected);

                String expectedPrefix = String.format("row=%08d-", expected);
                String actualPrefix = new String(wide, 0, 13, StandardCharsets.UTF_8);
                assertThat(actualPrefix).as("row %d wide", expected).isEqualTo(expectedPrefix);
                expected++;
            }
            assertThat(expected).isEqualTo(TOTAL_ROWS);
        }
    }

    private static OffsetIndex readOffsetIndex(ColumnIndexBuffers buffers) throws Exception {
        assertThat(buffers).isNotNull();
        assertThat(buffers.offsetIndex()).isNotNull();
        return OffsetIndexReader.read(new ThriftCompactReader(buffers.offsetIndex()));
    }

    /// First kept page's `firstRowIndex` for a filter that matches starting at
    /// `row` — the first page whose row range covers (or lies at/after) `row`.
    private static long firstPageStartCovering(OffsetIndex index, long row) {
        List<PageLocation> pages = index.pageLocations();
        for (int i = 0; i < pages.size(); i++) {
            long next = i + 1 < pages.size() ? pages.get(i + 1).firstRowIndex() : Long.MAX_VALUE;
            if (next > row) {
                return pages.get(i).firstRowIndex();
            }
        }
        return pages.getLast().firstRowIndex();
    }
}
