/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.OffsetIndex;
import dev.hardwood.metadata.PageLocation;
import dev.hardwood.metadata.PhysicalType;

import static org.assertj.core.api.Assertions.assertThat;

class PageRangeTest {

    @Test
    void contiguousPagesCoalesceIntoOneRange() {
        var pages = List.of(
                new PageLocation(100, 100, 0),
                new PageLocation(200, 100, 10),
                new PageLocation(300, 100, 20));
        List<PageRange> ranges = PageRange.coalesce(pages, 1024 * 1024);

        assertThat(ranges).hasSize(1);
        assertThat(ranges.get(0).offset()).isEqualTo(100);
        assertThat(ranges.get(0).length()).isEqualTo(300);
        assertThat(ranges.get(0).pages()).hasSize(3);
    }

    @Test
    void distantPagesBecomesSeparateRanges() {
        var pages = List.of(
                new PageLocation(100, 100, 0),
                new PageLocation(5_000_000, 100, 10));
        List<PageRange> ranges = PageRange.coalesce(pages, 1024 * 1024);

        assertThat(ranges).hasSize(2);
        assertThat(ranges.get(0).offset()).isEqualTo(100);
        assertThat(ranges.get(0).length()).isEqualTo(100);
        assertThat(ranges.get(0).pages()).hasSize(1);
        assertThat(ranges.get(1).offset()).isEqualTo(5_000_000);
        assertThat(ranges.get(1).length()).isEqualTo(100);
        assertThat(ranges.get(1).pages()).hasSize(1);
    }

    @Test
    void smallGapPagesMerge() {
        var pages = List.of(
                new PageLocation(100, 100, 0),
                new PageLocation(500, 100, 10));
        List<PageRange> ranges = PageRange.coalesce(pages, 1024 * 1024);

        assertThat(ranges).hasSize(1);
        assertThat(ranges.get(0).offset()).isEqualTo(100);
        assertThat(ranges.get(0).length()).isEqualTo(500); // [100, 600)
        assertThat(ranges.get(0).pages()).hasSize(2);
    }

    @Test
    void singlePageProducesOneRange() {
        var pages = List.of(new PageLocation(100, 100, 0));
        List<PageRange> ranges = PageRange.coalesce(pages, 1024 * 1024);

        assertThat(ranges).hasSize(1);
        assertThat(ranges.get(0).offset()).isEqualTo(100);
        assertThat(ranges.get(0).length()).isEqualTo(100);
    }

    @Test
    void zeroGapToleranceNeverMergesNonContiguous() {
        var pages = List.of(
                new PageLocation(100, 100, 0),
                new PageLocation(201, 100, 10)); // 1-byte gap
        List<PageRange> ranges = PageRange.coalesce(pages, 0);

        assertThat(ranges).hasSize(2);
    }

    @Test
    void forColumnReturnsEmptyWhenAllPagesMatch() {
        // Filter matches everything → caller should use full chunk path
        OffsetIndex oi = new OffsetIndex(List.of(
                new PageLocation(100, 200, 0),
                new PageLocation(300, 200, 50),
                new PageLocation(500, 200, 100)));
        RowRanges allRows = RowRanges.all(150);

        List<PageRange> ranges = PageRange.forColumn(oi, allRows, chunkWithoutDict(100), 150, 1024 * 1024);

        assertThat(ranges).isEmpty();
    }

    @Test
    void forColumnReturnsEmptyWhenNoPageIsSkipped() {
        // RowRanges covers all rows → no pages skipped
        OffsetIndex oi = new OffsetIndex(List.of(
                new PageLocation(100, 200, 0),
                new PageLocation(300, 200, 50)));
        RowRanges allRows = RowRanges.fromPages(oi.pageLocations(),
                new boolean[]{ true, true }, 100);

        List<PageRange> ranges = PageRange.forColumn(oi, allRows, chunkWithoutDict(100), 100, 1024 * 1024);

        assertThat(ranges).isEmpty();
    }

    @Test
    void forColumnReturnsMatchingPagesOnly() {
        // 3 pages, keep only the first → one range covering page 0
        OffsetIndex oi = new OffsetIndex(List.of(
                new PageLocation(100, 200, 0),
                new PageLocation(300, 200, 50),
                new PageLocation(500, 200, 100)));
        RowRanges firstPageOnly = RowRanges.fromPages(oi.pageLocations(),
                new boolean[]{ true, false, false }, 150);

        List<PageRange> ranges = PageRange.forColumn(oi, firstPageOnly, chunkWithoutDict(100), 150, 1024 * 1024);

        assertThat(ranges).hasSize(1);
        assertThat(ranges.get(0).offset()).isEqualTo(100);
        assertThat(ranges.get(0).length()).isEqualTo(200);
        assertThat(ranges.get(0).pages()).hasSize(1);
    }

    @Test
    void forColumnCoalescesAdjacentMatchingPages() {
        // 3 pages, keep first two → one coalesced range
        OffsetIndex oi = new OffsetIndex(List.of(
                new PageLocation(100, 200, 0),
                new PageLocation(300, 200, 50),
                new PageLocation(500, 200, 100)));
        RowRanges firstTwoPages = RowRanges.fromPages(oi.pageLocations(),
                new boolean[]{ true, true, false }, 150);

        List<PageRange> ranges = PageRange.forColumn(oi, firstTwoPages, chunkWithoutDict(100), 150, 1024 * 1024);

        assertThat(ranges).hasSize(1);
        assertThat(ranges.get(0).offset()).isEqualTo(100);
        assertThat(ranges.get(0).length()).isEqualTo(400);
        assertThat(ranges.get(0).pages()).hasSize(2);
    }

    @Test
    void forColumnProducesDisjointRangesForDistantPages() {
        // Pages 0 and 2 match but are far apart → two separate ranges
        OffsetIndex oi = new OffsetIndex(List.of(
                new PageLocation(100, 200, 0),
                new PageLocation(5_000_000, 200, 50),
                new PageLocation(10_000_000, 200, 100)));
        RowRanges firstAndLast = RowRanges.fromPages(oi.pageLocations(),
                new boolean[]{ true, false, true }, 150);

        List<PageRange> ranges = PageRange.forColumn(oi, firstAndLast, chunkWithoutDict(100), 150, 1024 * 1024);

        assertThat(ranges).hasSize(2);
        assertThat(ranges.get(0).offset()).isEqualTo(100);
        assertThat(ranges.get(1).offset()).isEqualTo(10_000_000);
    }

    @Test
    void forColumnExtendsDictionaryPrefix() {
        // Dictionary at offset 50, first data page at offset 100
        OffsetIndex oi = new OffsetIndex(List.of(
                new PageLocation(100, 200, 0),
                new PageLocation(300, 200, 50)));
        RowRanges firstPageOnly = RowRanges.fromPages(oi.pageLocations(),
                new boolean[]{ true, false }, 100);

        ColumnChunk chunkWithDict = chunkWithDict(100, 50);

        List<PageRange> ranges = PageRange.forColumn(oi, firstPageOnly, chunkWithDict, 100, 1024 * 1024);

        assertThat(ranges).hasSize(1);
        // Range should start at dictionary offset (50), not first data page (100)
        assertThat(ranges.get(0).offset()).isEqualTo(50);
        // Length should cover dictionary (50 bytes) + page (200 bytes)
        assertThat(ranges.get(0).length()).isEqualTo(250);
    }

    @Test
    void forColumnReturnsEmptyWhenNoPagesMatch() {
        OffsetIndex oi = new OffsetIndex(List.of(
                new PageLocation(100, 200, 0),
                new PageLocation(300, 200, 50)));
        RowRanges noPages = RowRanges.fromPages(oi.pageLocations(),
                new boolean[]{ false, false }, 100);

        List<PageRange> ranges = PageRange.forColumn(oi, noPages, chunkWithoutDict(100), 100, 1024 * 1024);

        assertThat(ranges).isEmpty();
    }

    private static ColumnChunk chunkWithoutDict(long dataPageOffset) {
        ColumnMetaData meta = new ColumnMetaData(
                PhysicalType.INT64, List.of(Encoding.PLAIN), FieldPath.of("col"),
                CompressionCodec.UNCOMPRESSED, 100, 1000, 1000, Map.of(),
                dataPageOffset, null, null, null);
        return new ColumnChunk(meta, null, null, null, null);
    }

    private static ColumnChunk chunkWithDict(long dataPageOffset, long dictOffset) {
        ColumnMetaData meta = new ColumnMetaData(
                PhysicalType.INT64, List.of(Encoding.PLAIN), FieldPath.of("col"),
                CompressionCodec.UNCOMPRESSED, 100, 1000, 1000, Map.of(),
                dataPageOffset, dictOffset, null, null);
        return new ColumnChunk(meta, null, null, null, null);
    }

    @Test
    void extendStartIncludesDictionaryPrefix() {
        var range = new PageRange(500, 1000, List.of(new PageLocation(500, 1000, 0)));
        PageRange extended = range.extendStart(200);

        assertThat(extended.offset()).isEqualTo(200);
        assertThat(extended.length()).isEqualTo(1300);
        assertThat(extended.pages()).isEqualTo(range.pages());
    }
}
