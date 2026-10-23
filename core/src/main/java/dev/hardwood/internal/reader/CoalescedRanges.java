/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.Arrays;

/// Byte ranges a read needs from one file, merged into as few requests as [CoalescingPolicy]
/// allows, each range then served from the request that holds it ([#requestHolding]).
///
/// Ranges that overlap are first joined into extents, so every range lies within exactly one
/// request and no byte is fetched twice. Consecutive extents, touching ones included, are then
/// merged under [CoalescingPolicy]: a request spans more than [CoalescingPolicy#MAX_SPAN_BYTES]
/// only when one extent alone is longer than that, which takes a range longer than that or ranges
/// overlapping each other across it.
final class CoalescedRanges {

    private final long[] requestStarts;
    private final long[] requestEnds;

    private CoalescedRanges(long[] requestStarts, long[] requestEnds) {
        this.requestStarts = requestStarts;
        this.requestEnds = requestEnds;
    }

    static Builder builder() {
        return new Builder();
    }

    /// The number of requests.
    int requestCount() {
        return requestStarts.length;
    }

    long requestStart(int request) {
        return requestStarts[request];
    }

    long requestEnd(int request) {
        return requestEnds[request];
    }

    /// The bytes the requests read: the ranges plus the gaps the merges bridge between them.
    long fetchedBytes() {
        long bytes = 0;
        for (int i = 0; i < requestStarts.length; i++) {
            bytes += requestEnds[i] - requestStarts[i];
        }
        return bytes;
    }

    /// The request holding the range `[offset, offset + length)`.
    ///
    /// @throws IllegalArgumentException if no request holds the range
    int requestHolding(long offset, int length) {
        int found = Arrays.binarySearch(requestStarts, offset);
        int request = found >= 0 ? found : -found - 2;
        if (request < 0 || offset + length > requestEnds[request]) {
            throw new IllegalArgumentException("No request holds the range [" + offset + ", "
                    + (offset + length) + ")");
        }
        return request;
    }

    /// Collects the ranges, in any order, and plans the requests for them.
    static final class Builder {

        private long[] starts = new long[16];
        private long[] ends = new long[16];
        private int size;

        private Builder() {
        }

        /// Adds the range `[offset, offset + length)`.
        ///
        /// @throws IllegalArgumentException if `offset` or `length` is negative
        Builder add(long offset, int length) {
            if (offset < 0 || length < 0) {
                throw new IllegalArgumentException(
                        "Invalid range: offset " + offset + ", length " + length);
            }
            if (size == starts.length) {
                starts = Arrays.copyOf(starts, size * 2);
                ends = Arrays.copyOf(ends, size * 2);
            }
            starts[size] = offset;
            ends[size] = offset + length;
            size++;
            return this;
        }

        boolean isEmpty() {
            return size == 0;
        }

        /// Plans the requests: joins overlapping ranges into extents, then merges consecutive
        /// extents under [CoalescingPolicy].
        CoalescedRanges build() {
            // Starts and ends sorted independently still give the union of the ranges: a sweep
            // that opens an extent at a start and closes it once as many ends as starts passed.
            // Touching ranges stay apart here, so the span limit can split between them.
            long[] sortedStarts = Arrays.copyOf(starts, size);
            long[] sortedEnds = Arrays.copyOf(ends, size);
            Arrays.sort(sortedStarts);
            Arrays.sort(sortedEnds);

            long[] requestStarts = new long[size];
            long[] requestEnds = new long[size];
            int requests = 0;
            int s = 0;
            int e = 0;
            while (s < size) {
                long extentStart = sortedStarts[s];
                int open = 0;
                do {
                    // An extent opens with a start; within one, an end at a start's coordinate
                    // goes first, so a range that merely touches the next closes the extent.
                    if (s < size && (open == 0 || sortedStarts[s] < sortedEnds[e])) {
                        s++;
                        open++;
                    }
                    else {
                        e++;
                        open--;
                    }
                } while (open > 0);
                long extentEnd = sortedEnds[e - 1];
                if (requests > 0 && CoalescingPolicy.merges(requestStarts[requests - 1],
                        requestEnds[requests - 1], extentStart, extentEnd)) {
                    requestEnds[requests - 1] = extentEnd;
                }
                else {
                    requestStarts[requests] = extentStart;
                    requestEnds[requests] = extentEnd;
                    requests++;
                }
            }
            return new CoalescedRanges(Arrays.copyOf(requestStarts, requests),
                    Arrays.copyOf(requestEnds, requests));
        }
    }
}
