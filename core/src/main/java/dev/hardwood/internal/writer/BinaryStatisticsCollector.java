/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import java.util.Arrays;

import dev.hardwood.internal.predicate.BinaryComparator;
import dev.hardwood.metadata.Statistics;

/// Accumulates a binary column chunk's `min` / `max` / `null_count` in one of the two orders a
/// byte string is compared in: unsigned lexicographic — the type-defined order for an
/// unannotated `BYTE_ARRAY` / `FIXED_LEN_BYTE_ARRAY` and for the string-like annotations — or
/// signed big-endian two's complement, the order of a `DECIMAL`'s represented value. Both are
/// the orders `BinaryComparator` compares in, which the signed arm calls directly.
///
/// Bounds are **truncated** to at most `truncationLength` bytes so a chunk of long values does
/// not bloat the footer. A truncated `min` keeps the value's first *N* bytes — a prefix is `<=`
/// the original, so it stays a valid lower bound. A truncated `max` keeps the first *N* bytes and
/// increments the last byte that is not `0xFF`, dropping the trailing bytes, yielding the
/// smallest length-`<= N` value that is `>=` the original; if every kept byte is `0xFF` no valid
/// truncated upper bound exists and the `max` bound is omitted. A truncated bound is flagged
/// inexact; an untruncated bound stays exact. The `min` / `max` references are copied on update,
/// so a caller that mutates a written batch's arrays before the row group flushes cannot corrupt
/// the bounds.
final class BinaryStatisticsCollector implements BinaryStatistics {

    /// The orders a byte string is compared in.
    enum Order {
        /// Unsigned byte-wise comparison.
        LEXICOGRAPHIC,
        /// Signed big-endian two's complement comparison of the represented value.
        SIGNED_BIG_ENDIAN
    }

    private final Order order;
    private final int truncationLength;
    private byte[] min;
    private byte[] max;
    private long nullCount;
    private boolean hasValues;

    BinaryStatisticsCollector(Order order, int truncationLength) {
        this.order = order;
        this.truncationLength = truncationLength;
    }

    @Override
    public void accept(byte[] value) {
        if (!hasValues) {
            min = value.clone();
            max = value.clone();
            hasValues = true;
            return;
        }
        if (compare(value, min) < 0) {
            min = value.clone();
        }
        if (compare(value, max) > 0) {
            max = value.clone();
        }
    }

    private int compare(byte[] left, byte[] right) {
        return order == Order.LEXICOGRAPHIC
                ? Arrays.compareUnsigned(left, right)
                : BinaryComparator.compareSigned(left, right);
    }

    @Override
    public void acceptNull() {
        nullCount++;
    }

    @Override
    public Statistics toStatistics() {
        if (!hasValues) {
            return new Statistics(null, null, nullCount, null, false);
        }
        byte[] minValue = min;
        boolean minExact = true;
        if (min.length > truncationLength) {
            minValue = Arrays.copyOf(min, truncationLength);
            minExact = false;
        }
        byte[] maxValue = max;
        boolean maxExact = true;
        if (max.length > truncationLength) {
            maxValue = truncateMax(max, truncationLength);
            maxExact = false;
        }
        return new Statistics(minValue, maxValue, nullCount, null, false, minExact, maxExact, null);
    }

    /// The smallest length-`<= n` byte string that is `>=` `value`: the first `n` bytes with the
    /// last non-`0xFF` byte incremented and the trailing bytes dropped, or `null` when every kept
    /// byte is `0xFF` (no valid truncated upper bound exists).
    private static byte[] truncateMax(byte[] value, int n) {
        byte[] prefix = Arrays.copyOf(value, n);
        for (int i = n - 1; i >= 0; i--) {
            if (prefix[i] != (byte) 0xFF) {
                byte[] result = Arrays.copyOf(prefix, i + 1);
                result[i]++;
                return result;
            }
        }
        return null;
    }
}
