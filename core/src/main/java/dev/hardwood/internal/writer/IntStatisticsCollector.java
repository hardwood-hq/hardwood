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

/// Accumulates an `INT32` column page's or chunk's `min` / `max` / `null_count`, producing the
/// [Statistics] written into the chunk metadata and the page bounds written into the
/// `ColumnIndex`, so that produced files support reader-side predicate pushdown.
///
/// The `min` / `max` bounds span only present values and are compared in the column's
/// type-defined order, so the written bounds are pruning-correct: signed for an unannotated
/// `INT32` and for the signed annotations, unsigned for `UINT_8` / `UINT_16` / `UINT_32`. The
/// two orders differ only in where the sign bit sorts, so flipping it turns the unsigned
/// comparison into the signed one the accumulator already performs — `bias` is that flip, zero
/// for the signed order.
///
/// The null count is every not-present slot; a fully null column therefore carries a null count
/// but no bounds. Statistics are written with the preferred `min_value` / `max_value` fields,
/// and the fixed-width bounds are always exact.
final class IntStatisticsCollector extends StatisticsCollector<IntStatisticsCollector> {

    private final int bias;
    private int min = Integer.MAX_VALUE;
    private int max = Integer.MIN_VALUE;
    private boolean hasValues;

    /// @param unsigned whether the column's order is unsigned
    IntStatisticsCollector(boolean unsigned) {
        this.bias = unsigned ? Integer.MIN_VALUE : 0;
    }

    void accept(int value) {
        int key = value ^ bias;
        if (key < min) {
            min = key;
        }
        if (key > max) {
            max = key;
        }
        hasValues = true;
    }

    @Override
    void mergeValues(IntStatisticsCollector page) {
        if (page.hasValues) {
            min = Math.min(min, page.min);
            max = Math.max(max, page.max);
            hasValues = true;
        }
    }

    @Override
    boolean hasValues() {
        return hasValues;
    }

    @Override
    Statistics toStatistics() {
        byte[] minValue = hasValues ? indexMin() : null;
        byte[] maxValue = hasValues ? indexMax() : null;
        return new Statistics(minValue, maxValue, nullCount, null, false);
    }

    @Override
    byte[] indexMin() {
        return encode(min ^ bias);
    }

    @Override
    byte[] indexMax() {
        return encode(max ^ bias);
    }

    @Override
    int compareBounds(byte[] left, byte[] right) {
        return Integer.compare(decode(left) ^ bias, decode(right) ^ bias);
    }

    private static byte[] encode(int value) {
        return ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array();
    }

    private static int decode(byte[] bytes) {
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
    }
}
