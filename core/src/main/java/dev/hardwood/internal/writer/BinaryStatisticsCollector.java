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
import dev.hardwood.internal.schema.ByteColumnOrder;
import dev.hardwood.metadata.Statistics;

/// Accumulates a binary column chunk's `min` / `max` / `null_count` in the column's
/// [ByteColumnOrder], compared through `BinaryComparator`. The writer uses three:
/// unsigned lexicographic — the type-defined order for an
/// unannotated `BYTE_ARRAY` / `FIXED_LEN_BYTE_ARRAY` and for the string-like annotations —,
/// signed big-endian two's complement, the order of a `DECIMAL`'s represented value, or signed
/// little-endian two's complement, the order of a `FIXED_LEN_BYTE_ARRAY(12)` `TIMESTAMP`.
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

    private final ByteColumnOrder order;
    private final int truncationLength;
    private byte[] min;
    private byte[] max;
    private long nullCount;
    private boolean hasValues;

    BinaryStatisticsCollector(ByteColumnOrder order, int truncationLength) {
        this.order = order;
        this.truncationLength = truncationLength;
    }

    @Override
    public void accept(byte[] array, int offset, int length) {
        if (!hasValues) {
            min = Arrays.copyOfRange(array, offset, offset + length);
            max = min.clone();
            hasValues = true;
            return;
        }
        if (BinaryComparator.compare(array, offset, offset + length, min, order) < 0) {
            min = Arrays.copyOfRange(array, offset, offset + length);
        }
        if (BinaryComparator.compare(array, offset, offset + length, max, order) > 0) {
            max = Arrays.copyOfRange(array, offset, offset + length);
        }
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
