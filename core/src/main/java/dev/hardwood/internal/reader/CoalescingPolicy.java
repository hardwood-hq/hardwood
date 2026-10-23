/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

/// Decides whether two byte ranges a read needs are fetched as one request: when the gap between
/// them is at most [#GAP_BYTES] (G) and the merged span at most [#maxSpanBytes()] (S). Every merge on
/// the read path applies this rule: a row group's needed data ranges across its columns
/// ([FetchPlanBuilder]), and the slices of a page-index window ([CoalescedRanges]).
///
/// A merge only bridges the gap between two needed ranges; it never extends a request before the
/// first needed byte or past the last one.
final class CoalescingPolicy {

    /// Largest gap bridged. Roughly the serial break-even of a remote request — the bytes that
    /// take as long to transfer as a round trip, e.g. 30 ms at 50 MiB/s — so fetching the gap
    /// costs no more than issuing a second request.
    static final int GAP_BYTES = 1024 * 1024;

    static final String MAX_SPAN_BYTES_PROPERTY = "hardwood.internal.maxCoalescedBytes";

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
    static int maxSpanBytes() {
        String override = System.getProperty(MAX_SPAN_BYTES_PROPERTY);
        if (override == null) {
            return DEFAULT_MAX_SPAN_BYTES;
        }
        int bytes;
        try {
            bytes = Integer.parseInt(override);
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    MAX_SPAN_BYTES_PROPERTY + " must be a positive byte count, but was: " + override, e);
        }
        if (bytes < 1) {
            throw new IllegalArgumentException(
                    MAX_SPAN_BYTES_PROPERTY + " must be a positive byte count, but was: " + override);
        }
        return bytes;
    }

    /// Whether the range `[nextStart, nextEnd)` joins a request spanning `[start, end)` under the
    /// span limit `maxSpan`.
    ///
    /// @param start first byte of the request so far
    /// @param end end of the request so far, exclusive
    /// @param nextStart first byte of the range to add, at or after `end`
    /// @param nextEnd end of the range to add, exclusive
    static boolean merges(long start, long end, long nextStart, long nextEnd, long maxSpan) {
        return nextStart - end <= GAP_BYTES && nextEnd - start <= maxSpan;
    }

    /// [#merges(long, long, long, long, long)] under [#maxSpanBytes()].
    static boolean merges(long start, long end, long nextStart, long nextEnd) {
        return merges(start, end, nextStart, nextEnd, maxSpanBytes());
    }
}
