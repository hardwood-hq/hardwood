/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

/// Per-page selection of records to keep, expressed as a sorted, non-overlapping
/// list of `[start, end)` row offsets *relative to the page's first row*.
///
/// For flat columns the offsets are value indices (one value per record).
/// For nested columns they are top-level record indices, identified by
/// repetition level zero in the page's rep-level stream.
///
/// The sentinel [#ALL] indicates "keep every row in the page" — assembly takes
/// the existing arraycopy fast path without per-interval iteration. Callers that
/// drop a page entirely use a `null` mask reference (no `PageRowMask` exists).
public final class PageRowMask {

    /// Sentinel value indicating "keep every row in the page". Shared across all
    /// pages in the unfiltered path — no per-page allocation.
    public static final PageRowMask ALL = new PageRowMask(null);

    /// Pairs of `[start, end)` row offsets relative to the page's first row.
    /// `null` only for the [#ALL] sentinel.
    private final int[] intervals;

    private PageRowMask(int[] intervals) {
        this.intervals = intervals;
    }

    /// Constructs a mask from an interleaved `[start0, end0, start1, end1, ...]`
    /// array. The array is taken as-is (no defensive copy) — callers must not
    /// mutate it. Validates sortedness, non-overlap, and `start < end` per pair.
    public static PageRowMask of(int[] intervals) {
        if (intervals == null) {
            throw new IllegalArgumentException("intervals must not be null; use PageRowMask.ALL");
        }
        if (intervals.length == 0 || (intervals.length & 1) != 0) {
            throw new IllegalArgumentException(
                    "intervals length must be positive and even, got " + intervals.length);
        }
        int prevEnd = -1;
        for (int i = 0; i < intervals.length; i += 2) {
            int s = intervals[i];
            int e = intervals[i + 1];
            if (s < 0 || s >= e) {
                throw new IllegalArgumentException(
                        "invalid interval at index " + i + ": [" + s + ", " + e + ")");
            }
            if (s < prevEnd) {
                throw new IllegalArgumentException(
                        "intervals must be sorted and non-overlapping; "
                        + "interval at index " + i + " starts at " + s
                        + " but previous ended at " + prevEnd);
            }
            prevEnd = e;
        }
        return new PageRowMask(intervals);
    }

    /// Whether this mask keeps every row (i.e. is the [#ALL] sentinel).
    public boolean isAll() {
        return intervals == null;
    }

    /// Number of intervals. Throws on [#ALL] — callers should branch on
    /// [#isAll()] first.
    public int intervalCount() {
        return intervals.length / 2;
    }

    /// Start of interval `i` (inclusive, page-relative).
    public int start(int i) {
        return intervals[i * 2];
    }

    /// End of interval `i` (exclusive, page-relative).
    public int end(int i) {
        return intervals[i * 2 + 1];
    }

    /// Sum of interval lengths — the total number of records the mask keeps.
    public int totalRecords() {
        int sum = 0;
        for (int i = 0; i < intervals.length; i += 2) {
            sum += intervals[i + 1] - intervals[i];
        }
        return sum;
    }
}
