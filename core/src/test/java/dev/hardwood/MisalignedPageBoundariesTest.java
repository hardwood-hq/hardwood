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
    private static final int TOTAL_ROWS = 10_000;

    @Test
    void fixtureHasDivergentPageBoundaries() throws Exception {
        try (InputFile file = InputFile.of(MISALIGNED_FILE)) {
            file.open();
            FileMetaData meta = ParquetMetadataReader.readMetadata(file);
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
             RowReader rows = reader.createRowReader(filter)) {
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
    void fullScanStillPairsColumnsCorrectly() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(MISALIGNED_FILE));
             RowReader rows = reader.createRowReader()) {
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
