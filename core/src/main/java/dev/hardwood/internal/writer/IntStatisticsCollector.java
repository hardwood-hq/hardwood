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

/// Accumulates an `INT32` column chunk's `min` / `max` / `null_count` over its shredded leaf
/// slots as they are encoded, producing the [Statistics] written into the chunk metadata so
/// that produced files support reader-side predicate pushdown.
///
/// The `min` / `max` bounds span only present values and are compared with signed `INT32`
/// ordering, matching the column's type-defined `ColumnOrder`, so the written bounds are
/// pruning-correct. The null count is every not-present slot; a fully null column therefore
/// carries a null count but no bounds. Statistics are written with the preferred `min_value` /
/// `max_value` fields, and the fixed-width bounds are always exact.
final class IntStatisticsCollector {

    private int min = Integer.MAX_VALUE;
    private int max = Integer.MIN_VALUE;
    private long nullCount;
    private boolean hasValues;

    void accept(int value) {
        if (value < min) {
            min = value;
        }
        if (value > max) {
            max = value;
        }
        hasValues = true;
    }

    void acceptNull() {
        nullCount++;
    }

    Statistics toStatistics() {
        byte[] minValue = hasValues ? encode(min) : null;
        byte[] maxValue = hasValues ? encode(max) : null;
        return new Statistics(minValue, maxValue, nullCount, null, false);
    }

    private static byte[] encode(int value) {
        return ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array();
    }
}
