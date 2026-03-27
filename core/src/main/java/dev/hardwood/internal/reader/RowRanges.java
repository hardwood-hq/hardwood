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

/// A sorted, non-overlapping set of `[startRow, endRow)` intervals representing
/// rows that might match a filter predicate.
///
/// Used by page-level Column Index filtering to translate per-page filter results
/// into row ranges that can be checked against any column's Offset Index.
public class RowRanges {

    private final long[] ranges; // [start0, end0, start1, end1, ...]
    private final boolean all;

    private RowRanges(long[] ranges, boolean all) {
        this.ranges = ranges;
        this.all = all;
    }

    /// Creates a RowRanges that matches all rows in a row group.
    /// Used as a conservative fallback when Column Index is absent.
    static RowRanges all(long rowGroupRowCount) {
        return new RowRanges(new long[]{ 0, rowGroupRowCount }, true);
    }

    /// Returns `true` if this instance represents all rows (no filtering).
    boolean isAll() {
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
    static RowRanges fromPages(List<PageLocation> pages, boolean[] keep, long rowGroupRowCount) {
        List<long[]> intervals = new ArrayList<>();

        // Iterate through all the available pages
        for (int i = 0; i < pages.size(); i++) {
            // Ignore pages that do not need to be kept
            if (!keep[i]) {
                continue;
            }

            long rangeStart = pages.get(i).firstRowIndex();
            long rangeEnd = (i + 1 < pages.size()) ? pages.get(i + 1).firstRowIndex() : rowGroupRowCount;

            // Merge with previous interval if adjacent
            if (!intervals.isEmpty()) {
                long[] last = intervals.getLast();
                if (last[1] >= rangeStart) {
                    last[1] = Math.max(last[1], rangeEnd);
                    continue;
                }
            }
            intervals.add(new long[]{ rangeStart, rangeEnd });
        }

        return new RowRanges(flatten(intervals), false);
    }

    /// Returns `true` if a page with the given row range overlaps any matching interval.
    boolean overlapsPage(long pageFirstRow, long pageLastRow) {
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
    RowRanges intersect(RowRanges other) {
        if (this.all) {
            return other;
        }
        if (other.all) {
            return this;
        }

        List<long[]> result = new ArrayList<>();
        int i = 0;
        int j = 0;
        while (i < this.ranges.length && j < other.ranges.length) {
            long rangeStart = Math.max(this.ranges[i], other.ranges[j]);
            long rangeEnd = Math.min(this.ranges[i + 1], other.ranges[j + 1]);
            if (rangeStart < rangeEnd) {
                result.add(new long[]{ rangeStart, rangeEnd });
            }

            // Advance the interval that ends first
            if (this.ranges[i + 1] < other.ranges[j + 1]) {
                i += 2;
            }
            else {
                j += 2;
            }
        }

        return new RowRanges(flatten(result), false);
    }

    /// Returns the union of this RowRanges with `other` (to support OR predicates).
    /// Both inputs are already sorted and non-overlapping, so a single-pass merge suffices.
    RowRanges union(RowRanges other) {
        if (this.all || other.all) {
            return this.all ? this : other;
        }

        List<long[]> merged = new ArrayList<>();
        int i = 0;
        int j = 0;
        while (i < this.ranges.length || j < other.ranges.length) {
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

            if (!merged.isEmpty() && merged.getLast()[1] >= start) {
                merged.getLast()[1] = Math.max(merged.getLast()[1], end);
            }
            else {
                merged.add(new long[]{ start, end });
            }
        }

        return new RowRanges(flatten(merged), false);
    }

    /// Returns the number of intervals in this set.
    int intervalCount() {
        return ranges.length / 2;
    }

    /// Flattens a list of `[start, end)` pairs into a single interleaved array.
    private static long[] flatten(List<long[]> intervals) {
        long[] flat = new long[intervals.size() * 2];
        for (int i = 0; i < intervals.size(); i++) {
            flat[i * 2] = intervals.get(i)[0];
            flat[i * 2 + 1] = intervals.get(i)[1];
        }
        return flat;
    }
}
