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
import java.util.Map;
import java.util.TreeMap;

/// A sorted set of half-open `[start, end)` byte ranges, kept canonical
/// by merging overlapping or touching entries on [#add]. Used by
/// [RangeBackedInputFile] (#373) to track which portions of a remote
/// file have been fetched into the local cache.
///
/// Not thread-safe — callers serialise access through their own monitor.
public final class RangeSet {

    /// Map from `start` → `end`. Invariant after every mutation: no two
    /// entries overlap or touch. `(a, b)` and `(b, c)` get merged into
    /// `(a, c)`.
    private final TreeMap<Long, Long> ranges = new TreeMap<>();

    /// Returns true iff every byte in `[start, end)` is covered by some
    /// stored range. Empty input ranges (`start >= end`) return true.
    public boolean contains(long start, long end) {
        if (start >= end) {
            return true;
        }
        Map.Entry<Long, Long> floor = ranges.floorEntry(start);
        return floor != null && floor.getValue() >= end;
    }

    /// Inserts `[start, end)`, merging with any overlapping or touching
    /// existing ranges. No-op for empty inputs.
    public void add(long start, long end) {
        if (start >= end) {
            return;
        }
        long mergedStart = start;
        long mergedEnd = end;

        Map.Entry<Long, Long> floor = ranges.floorEntry(start);
        if (floor != null && floor.getValue() >= start) {
            mergedStart = floor.getKey();
            mergedEnd = Math.max(mergedEnd, floor.getValue());
            ranges.remove(floor.getKey());
        }

        Map.Entry<Long, Long> next = ranges.ceilingEntry(mergedStart);
        while (next != null && next.getKey() <= mergedEnd) {
            mergedEnd = Math.max(mergedEnd, next.getValue());
            ranges.remove(next.getKey());
            next = ranges.ceilingEntry(mergedStart);
        }

        ranges.put(mergedStart, mergedEnd);
    }

    /// Returns the gaps in `[start, end)` not covered by any stored
    /// range, in ascending offset order. An empty list means the
    /// requested range is fully populated.
    public List<long[]> missing(long start, long end) {
        if (start >= end) {
            return List.of();
        }
        List<long[]> gaps = new ArrayList<>();
        long cursor = start;

        Map.Entry<Long, Long> entry = ranges.floorEntry(cursor);
        if (entry == null || entry.getValue() <= cursor) {
            entry = ranges.ceilingEntry(cursor);
        }
        else {
            cursor = entry.getValue();
            entry = ranges.ceilingEntry(cursor);
        }

        while (cursor < end && entry != null && entry.getKey() < end) {
            if (entry.getKey() > cursor) {
                gaps.add(new long[]{ cursor, entry.getKey() });
            }
            cursor = Math.max(cursor, entry.getValue());
            entry = ranges.ceilingEntry(cursor);
        }
        if (cursor < end) {
            gaps.add(new long[]{ cursor, end });
        }
        return gaps;
    }

    /// Number of distinct ranges currently stored. Test / diagnostic only.
    public int size() {
        return ranges.size();
    }
}
