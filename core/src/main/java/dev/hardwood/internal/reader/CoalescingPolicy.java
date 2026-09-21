/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

/// Decides whether two byte ranges a read needs are fetched as one request: when the gap between
/// them is at most [#GAP_BYTES] and the merged span at most [#MAX_SPAN_BYTES]. Every merge on the
/// read path applies this rule — pages within a column, the dictionary page into the first page
/// group, adjacent column chunks.
///
/// A merge only bridges the gap between two needed ranges; it never extends a request before the
/// first needed byte or past the last one.
final class CoalescingPolicy {

    /// Largest gap bridged. Roughly the serial break-even of a remote request — the bytes that
    /// take as long to transfer as a round trip, e.g. 30 ms at 50 MiB/s — so fetching the gap
    /// costs no more than issuing a second request.
    static final int GAP_BYTES = 1024 * 1024;

    /// Largest span of one merged request, so that each `readRange()` stays bounded, enabling
    /// pre-fetch overlap and early cancellation.
    static final int MAX_SPAN_BYTES =
            Integer.getInteger("hardwood.internal.maxCoalescedBytes", 128 * 1024 * 1024);

    private CoalescingPolicy() {
    }

    /// Whether the range `[nextStart, nextEnd)` joins a request spanning `[start, end)`.
    ///
    /// @param start first byte of the request so far
    /// @param end end of the request so far, exclusive
    /// @param nextStart first byte of the range to add, at or after `end`
    /// @param nextEnd end of the range to add, exclusive
    static boolean merges(long start, long end, long nextStart, long nextEnd) {
        return nextStart - end <= GAP_BYTES && nextEnd - start <= MAX_SPAN_BYTES;
    }
}
