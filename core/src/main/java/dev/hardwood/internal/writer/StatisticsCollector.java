/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import dev.hardwood.metadata.Statistics;

/// Accumulates a column chunk's `min` / `max` / `null_count` over its shredded leaf slots as
/// they are encoded, producing the [Statistics] written into the chunk metadata so that
/// produced files support reader-side predicate pushdown.
///
/// The `min` / `max` bounds span only present values and are compared with signed `INT32`
/// ordering, matching the column's type-defined `ColumnOrder`, so the written bounds are
/// pruning-correct. The null count is every not-present slot — a null leaf, a null or empty
/// list, or a null struct ancestor — mirroring how the reader counts nulls; a fully null
/// column therefore carries a null count but no bounds. Statistics are written with the
/// preferred `min_value` / `max_value` fields, so the bounds are never flagged as the
/// deprecated `min` / `max`.
final class StatisticsCollector {

    private int min = Integer.MAX_VALUE;
    private int max = Integer.MIN_VALUE;
    private long nullCount;
    private boolean hasValues;

    /// Records one shredded leaf slot: a present value extends the `min` / `max` bounds, an
    /// absent slot increments the null count. `value` is ignored when `present` is false.
    void accept(boolean present, int value) {
        if (present) {
            if (value < min) {
                min = value;
            }
            if (value > max) {
                max = value;
            }
            hasValues = true;
        }
        else {
            nullCount++;
        }
    }

    /// Builds the chunk statistics: the `INT32` bounds encoded as 4-byte little-endian `PLAIN`
    /// values — absent when the chunk holds no present value — alongside the null count.
    Statistics toStatistics() {
        byte[] minValue = hasValues ? encodeInt(min) : null;
        byte[] maxValue = hasValues ? encodeInt(max) : null;
        return new Statistics(minValue, maxValue, nullCount, null, false);
    }

    private static byte[] encodeInt(int value) {
        return ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array();
    }
}
