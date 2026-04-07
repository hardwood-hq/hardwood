/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.Arrays;
import java.util.List;

import dev.hardwood.metadata.PageLocation;

/// A sorted, non-overlapping set of `[startRow, endRow)` intervals representing
/// rows that might match a filter predicate.
///
/// Used by page-level Column Index filtering to translate per-page filter results
/// into row ranges that can be checked against any column's Offset Index.
public class RowRanges {

    /// Sentinel representing all rows — used when no page-level filtering is active.
    public static final RowRanges ALL = new RowRanges(new long[0], true);

    private final long[] ranges; // [start0, end0, start1, end1, ...]
    private final boolean all;

    private RowRanges(long[] ranges, boolean all) {
        this.ranges = ranges;
        this.all = all;
    }

    /// Creates a RowRanges that matches all rows in a row group.
    /// Used as a conservative fallback when Column Index is absent.
    public static RowRanges all(long rowGroupRowCount) {
        return new RowRanges(new long[]{ 0, rowGroupRowCount }, true);
    }

    /// Returns `true` if this instance represents all rows (no filtering).
    public boolean isAll() {
        return all;
    }

    /// Creates RowRanges from page locations and a keep bitmap.
    ///
    /// For each page where `keep[i]` is true, the row range
    /// `[firstRowIndex[i], firstRowIndex[i+1])` is included. The last page's
    /// end is `rowGroupRowCount`. Adjacent kept ranges are merged.
    ///
    /// @param pages page locations from the Offset Index
    /// @param keep bitmap indicating which pages to keep
    /// @param rowGroupRowCount total number of rows in the row group
    public static RowRanges fromPages(List<PageLocation> pages, boolean[] keep, long rowGroupRowCount) {
        long[] result = new long[pages.size() * 2];
        int pos = 0;

        // Iterate through all the available pages
        for (int i = 0; i < pages.size(); i++) {
            // Ignore pages that do not need to be kept
            if (!keep[i]) {
                continue;
            }

            long rangeStart = pages.get(i).firstRowIndex();
            long rangeEnd = (i + 1 < pages.size()) ? pages.get(i + 1).firstRowIndex() : rowGroupRowCount;

            // Merge with previous interval if adjacent
            if (pos > 0 && result[pos - 1] >= rangeStart) {
                result[pos - 1] = Math.max(result[pos - 1], rangeEnd);
            }
            else {
                result[pos++] = rangeStart;
                result[pos++] = rangeEnd;
            }
        }

        return new RowRanges(pos == result.length ? result : Arrays.copyOf(result, pos), false);
    }

    /// Returns `true` if a page with the given row range overlaps any matching interval.
    public boolean overlapsPage(long pageFirstRow, long pageLastRow) {
        for (int i = 0; i < ranges.length; i += 2) {
            if (pageLastRow <= ranges[i]) {
                return false;
            }
            if (pageFirstRow < ranges[i + 1]) {
                return true;
            }
        }
        return false;
    }

    /// Returns this RowRanges intersected with `other` (to support AND predicates).
    public RowRanges intersect(RowRanges other) {
        if (this.all) {
            return other;
        }
        if (other.all) {
            return this;
        }

        long[] result = new long[this.ranges.length + other.ranges.length];
        int pos = 0;
        int i = 0;
        int j = 0;
        while (i < this.ranges.length && j < other.ranges.length) {
            long rangeStart = Math.max(this.ranges[i], other.ranges[j]);
            long rangeEnd = Math.min(this.ranges[i + 1], other.ranges[j + 1]);
            if (rangeStart < rangeEnd) {
                result[pos++] = rangeStart;
                result[pos++] = rangeEnd;
            }

            // Advance the interval that ends first
            if (this.ranges[i + 1] < other.ranges[j + 1]) {
                i += 2;
            }
            else {
                j += 2;
            }
        }

        return new RowRanges(pos == result.length ? result : Arrays.copyOf(result, pos), false);
    }

    /// Returns the union of this RowRanges with `other` (to support OR predicates).
    /// Both inputs are already sorted and non-overlapping, so a single-pass merge suffices.
    public RowRanges union(RowRanges other) {
        if (this.all || other.all) {
            return this.all ? this : other;
        }

        long[] result = new long[this.ranges.length + other.ranges.length];
        int pos = 0;
        int i = 0;
        int j = 0;
        while (i < this.ranges.length || j < other.ranges.length) {
            // Pick the interval with the smaller start from either input
            long start;
            long end;
            if (j >= other.ranges.length || (i < this.ranges.length && this.ranges[i] <= other.ranges[j])) {
                start = this.ranges[i];
                end = this.ranges[i + 1];
                i += 2;
            }
            else {
                start = other.ranges[j];
                end = other.ranges[j + 1];
                j += 2;
            }

            // Merge with previous interval if overlapping or adjacent
            if (pos > 0 && result[pos - 1] >= start) {
                result[pos - 1] = Math.max(result[pos - 1], end);
            }
            else {
                result[pos++] = start;
                result[pos++] = end;
            }
        }

        return new RowRanges(pos == result.length ? result : Arrays.copyOf(result, pos), false);
    }

    /// Returns the number of intervals in this set.
    public int intervalCount() {
        return ranges.length / 2;
    }
}
