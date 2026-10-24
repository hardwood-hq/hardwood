/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.fetch;

import java.util.ArrayList;
import java.util.List;

import dev.hardwood.internal.reader.memory.InternalProperty;

/// Decides whether two byte ranges a read needs are fetched as one request: when the gap between
/// them is at most [#GAP_BYTES] (G) and the merged span at most [#maxSpanBytes()] (S). Every merge
/// on the read path applies this rule: a row group's needed data ranges across its columns
/// ([#coalesce]), and the slices of a page-index window ([CoalescedRanges]).
///
/// A merge only bridges the gap between two needed ranges; it never extends a request before the
/// first needed byte or past the last one.
public final class CoalescingPolicy {

    /// Largest gap bridged. Roughly the serial break-even of a remote request — the bytes that
    /// take as long to transfer as a round trip, e.g. 30 ms at 50 MiB/s — so fetching the gap
    /// costs no more than issuing a second request.
    static final int GAP_BYTES = 1024 * 1024;

    public static final String MAX_SPAN_BYTES_PROPERTY = "hardwood.internal.maxCoalescedBytes";

    /// Largest request without the override, so that each `readRange()` stays bounded, enabling
    /// read-ahead to overlap decode and a read that stops early to leave the rest unfetched.
    private static final int DEFAULT_MAX_SPAN_BYTES = 128 * 1024 * 1024;

    private CoalescingPolicy() {
    }

    /// The largest request, S: [#DEFAULT_MAX_SPAN_BYTES] unless overridden through
    /// [#MAX_SPAN_BYTES_PROPERTY]. Read on each call rather than cached, so that the override holds
    /// whenever it is set; a read takes it once, when its iterator is built. A value that is not a
    /// positive byte count is rejected rather than replaced by the default: a span limit that does
    /// not take effect produces a plausible read with the wrong requests.
    public static int maxSpanBytes() {
        return InternalProperty.intValue(MAX_SPAN_BYTES_PROPERTY, DEFAULT_MAX_SPAN_BYTES, 1, "a positive byte count");
    }

    /// Whether the range `[nextStart, nextEnd)` joins a request spanning `[start, end)` under the
    /// span limit `maxSpan`.
    ///
    /// @param start first byte of the request so far
    /// @param end end of the request so far, exclusive
    /// @param nextStart first byte of the range to add, at or after `end`
    /// @param nextEnd end of the range to add, exclusive
    public static boolean merges(long start, long end, long nextStart, long nextEnd, long maxSpan) {
        return nextStart - end <= GAP_BYTES && nextEnd - start <= maxSpan;
    }

    /// [#merges(long, long, long, long, long)] under [#maxSpanBytes()].
    static boolean merges(long start, long end, long nextStart, long nextEnd) {
        return merges(start, end, nextStart, nextEnd, maxSpanBytes());
    }

    /// A range of the file one decoded column needs.
    public record Range(int column, long start, long end) {
    }

    /// The bytes of a decoded column's chunk, `[start, end)`.
    public record Extent(int column, String name, long start, long end) {
    }

    /// Coalesces `ranges`, in offset order, into the spans of a row group's entries: a range joins
    /// the span before it when [#merges] allows, so a span bridges gaps of at most
    /// G and is at most S long unless one range alone is longer. Each span is held by every column
    /// whose extent overlaps it, and its reason names every column whose range it holds.
    ///
    /// No span crosses `tailOffset`, where the tail the file's footer entry read begins
    /// ([TailedInputFile]): a span inside the tail is sliced from it, so a range that straddles
    /// the tail's start is cut there, and the part before it is all that is requested.
    public static List<FetchSequence.Span> coalesce(List<Range> ranges, Extent[] extents, String[] names,
                                             int rowGroupIndex, int maxSpan, long tailOffset) {
        ranges = cutAt(ranges, tailOffset);
        List<FetchSequence.Span> spans = new ArrayList<>();
        int firstExtent = 0;
        long start = -1;
        long end = -1;
        boolean[] served = new boolean[names.length];
        List<Integer> servedOrder = new ArrayList<>();
        for (int i = 0; i <= ranges.size(); i++) {
            Range range = i < ranges.size() ? ranges.get(i) : null;
            if (range != null && start >= 0 && merges(start, end, range.start(), range.end(), maxSpan)
                    && (start >= tailOffset || range.end() <= tailOffset)) {
                end = Math.max(end, range.end());
            }
            else {
                if (start >= 0) {
                    while (firstExtent < extents.length && extents[firstExtent].end() <= start) {
                        firstExtent++;
                    }
                    int holders = 0;
                    for (int e = firstExtent; e < extents.length && extents[e].start() < end; e++) {
                        if (extents[e].end() > start) {
                            holders++;
                        }
                    }
                    StringBuilder reason = new StringBuilder("rg=").append(rowGroupIndex);
                    for (int column : servedOrder) {
                        reason.append(" col=").append(names[column]);
                        served[column] = false;
                    }
                    servedOrder.clear();
                    spans.add(new FetchSequence.Span(start, Math.toIntExact(end - start), reason.toString(), holders));
                }
                if (range == null) {
                    break;
                }
                start = range.start();
                end = range.end();
            }
            if (!served[range.column()]) {
                served[range.column()] = true;
                servedOrder.add(range.column());
            }
        }
        return spans;
    }

    /// `ranges`, in offset order, with the one that straddles `offset` cut in two there.
    private static List<Range> cutAt(List<Range> ranges, long offset) {
        for (int i = 0; i < ranges.size(); i++) {
            Range range = ranges.get(i);
            if (range.start() < offset && range.end() > offset) {
                List<Range> cut = new ArrayList<>(ranges);
                cut.set(i, new Range(range.column(), range.start(), offset));
                cut.add(i + 1, new Range(range.column(), offset, range.end()));
                return cut;
            }
        }
        return ranges;
    }
}
