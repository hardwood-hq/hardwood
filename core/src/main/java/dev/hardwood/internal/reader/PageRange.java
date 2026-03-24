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

import dev.hardwood.metadata.PageLocation;

/// A contiguous byte range covering one or more Parquet data pages.
///
/// Produced by coalescing nearby [PageLocation] entries so that a single
/// `readRange()` call can fetch multiple pages at once. The individual
/// page locations are retained for slicing after the read.
///
/// **Currently unused.** Column chunk data is now pre-fetched in bulk via
/// [ChunkRange], so page-level coalescing is not needed on the current
/// read path. This type is retained for page-level predicate pushdown (#118):
/// when Column Index filtering skips pages within a chunk, only matching page
/// groups should be fetched rather than the entire chunk. `PageRange`
/// will coalesce those matching groups into minimal range requests.
///
/// @param offset absolute file offset of the first byte in this range
/// @param length total number of bytes in this range
/// @param pages  the page locations covered by this range, in file order
record PageRange(long offset, int length, List<PageLocation> pages) {

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
