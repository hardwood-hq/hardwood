/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate.dictionary;

/// A fixed set of integer keys as one bit per value of `[min, max]`, for literals that lie close
/// together, the shape of an id list. A probe is a range check and one bit test.
final class RangeBitmask {

    /// Widest `max - min` a bitmask is built for, in bits: 128 KiB of words. Tuning: #1594.
    static final long SPAN_LIMIT = 1L << 20;

    private final long min;
    private final long span;
    private final long[] words;

    /// The bitmask over `keys`, or `null` when their span exceeds [#SPAN_LIMIT] or does not fit a
    /// `long`, in which case the caller indexes them another way.
    static RangeBitmask of(long[] keys) {
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        for (long key : keys) {
            min = Math.min(min, key);
            max = Math.max(max, key);
        }
        long span = max - min;
        if (span < 0 || span >= SPAN_LIMIT) {
            return null;
        }
        return new RangeBitmask(keys, min, span);
    }

    private RangeBitmask(long[] keys, long min, long span) {
        this.min = min;
        this.span = span;
        words = new long[(int) (span >>> 6) + 1];
        for (long key : keys) {
            long offset = key - min;
            words[(int) (offset >>> 6)] |= 1L << offset;
        }
    }

    boolean contains(long key) {
        // A key below min wraps to a large unsigned offset and fails the range check like one above max.
        long offset = key - min;
        if (Long.compareUnsigned(offset, span) > 0) {
            return false;
        }
        return (words[(int) (offset >>> 6)] & (1L << offset)) != 0;
    }
}
