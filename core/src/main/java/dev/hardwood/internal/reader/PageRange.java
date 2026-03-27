/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.ArrayList;
import java.util.List;

import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.OffsetIndex;
import dev.hardwood.metadata.PageLocation;

/// A contiguous byte range covering one or more Parquet data pages.
///
/// Produced by coalescing nearby [PageLocation] entries so that a single
/// `readRange()` call can fetch multiple pages at once. The individual
/// page locations are retained for slicing after the read. Used by page-level
/// predicate pushdown to fetch only matching pages instead of entire column
/// chunks, reducing network I/O on remote backends.
///
/// @param offset absolute file offset of the first byte in this range
/// @param length total number of bytes in this range
/// @param pages  the page locations covered by this range, in file order
record PageRange(long offset, int length, List<PageLocation> pages) {

    /// Computes the page ranges to fetch for a single column, given the matching row ranges
    /// from Column Index filtering. Returns an empty list if no pages are skipped (signaling
    /// the caller should use the full chunk fetch path instead).
    ///
    /// When pages are skipped, the result includes the dictionary page prefix (if present)
    /// via `extendStart()` on the first range.
    ///
    /// @param offsetIndex    the column's offset index (page locations)
    /// @param matchingRows   row ranges that might match the filter
    /// @param columnChunk    the column chunk metadata (for dictionary detection)
    /// @param rowGroupRowCount total rows in the row group
    /// @param maxGapBytes    maximum gap to bridge when coalescing (same as [ChunkRange.MAX_GAP_BYTES])
    /// @return coalesced page ranges to fetch, or empty list if all pages match
    static List<PageRange> forColumn(OffsetIndex offsetIndex, RowRanges matchingRows,
            ColumnChunk columnChunk, long rowGroupRowCount, int maxGapBytes) {

        List<PageLocation> allPages = offsetIndex.pageLocations();
        if (allPages.isEmpty() || matchingRows.isAll()) {
            return List.of();
        }

        // Filter pages by matching row ranges
        List<PageLocation> matchingPages = new ArrayList<>();
        for (int i = 0; i < allPages.size(); i++) {
            long pageFirstRow = allPages.get(i).firstRowIndex();
            long pageLastRow = (i + 1 < allPages.size())
                    ? allPages.get(i + 1).firstRowIndex()
                    : rowGroupRowCount;
            if (matchingRows.overlapsPage(pageFirstRow, pageLastRow)) {
                matchingPages.add(allPages.get(i));
            }
        }

        // If no pages were skipped, signal caller to use full chunk path
        if (matchingPages.size() == allPages.size()) {
            return List.of();
        }

        // If no pages match, return empty — no data to fetch
        if (matchingPages.isEmpty()) {
            return List.of();
        }

        // Coalesce matching pages into minimal ranges
        List<PageRange> ranges = coalesce(matchingPages, maxGapBytes);

        // Extend the first range to include the dictionary page prefix if present
        long firstDataPageOffset = allPages.get(0).offset();
        Long dictOffset = columnChunk.metaData().dictionaryPageOffset();
        long chunkStart = (dictOffset != null && dictOffset > 0)
                ? dictOffset
                : columnChunk.metaData().dataPageOffset();

        if (chunkStart < firstDataPageOffset) {
            ranges.set(0, ranges.get(0).extendStart(chunkStart));
        }

        return ranges;
    }

    /// Coalesces nearby page locations into `PageRange`s.
    ///
    /// Two consecutive pages are merged when the gap between the end of one
    /// page and the start of the next is at most `maxGapBytes`. When
    /// pages are truly contiguous (the common case), everything collapses
    /// into a single range.
    ///
    /// @param pages       page locations in file order
    /// @param maxGapBytes maximum gap (in bytes) to bridge when merging
    /// @return coalesced ranges, each covering one or more pages
    static List<PageRange> coalesce(List<PageLocation> pages, int maxGapBytes) {
        List<PageRange> ranges = new ArrayList<>();

        long rangeStart = pages.get(0).offset();
        long rangeEnd = rangeStart + pages.get(0).compressedPageSize();
        List<PageLocation> rangePages = new ArrayList<>();
        rangePages.add(pages.get(0));

        for (int i = 1; i < pages.size(); i++) {
            PageLocation page = pages.get(i);
            long gap = page.offset() - rangeEnd;

            if (gap <= maxGapBytes) {
                rangeEnd = page.offset() + page.compressedPageSize();
                rangePages.add(page);
            }
            else {
                ranges.add(new PageRange(rangeStart,
                        Math.toIntExact(rangeEnd - rangeStart), List.copyOf(rangePages)));
                rangeStart = page.offset();
                rangeEnd = rangeStart + page.compressedPageSize();
                rangePages.clear();
                rangePages.add(page);
            }
        }

        ranges.add(new PageRange(rangeStart,
                Math.toIntExact(rangeEnd - rangeStart), List.copyOf(rangePages)));
        return ranges;
    }

    /// Extends this range backwards to include a prefix region (e.g. a
    /// dictionary page that sits before the first data page).
    ///
    /// @param newOffset the new start offset (must be &le; current offset)
    /// @return a new PageRange covering [newOffset, offset + length)
    PageRange extendStart(long newOffset) {
        int extraPrefix = Math.toIntExact(offset - newOffset);
        return new PageRange(newOffset, length + extraPrefix, pages);
    }
}
