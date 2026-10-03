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

/// Accumulates an `INT64` column chunk's `min` / `max` / `null_count`, compared in the column's
/// type-defined order — signed, or unsigned for `UINT_64` — and encoded as 8-byte little-endian
/// bounds. The `INT64` counterpart of [IntStatisticsCollector], including its sign-bit flip.
final class LongStatisticsCollector extends StatisticsCollector<LongStatisticsCollector> {

    private final long bias;
    private long min = Long.MAX_VALUE;
    private long max = Long.MIN_VALUE;
    private boolean hasValues;

    /// @param unsigned whether the column's order is unsigned
    LongStatisticsCollector(boolean unsigned) {
        this.bias = unsigned ? Long.MIN_VALUE : 0L;
    }

    void accept(long value) {
        long key = value ^ bias;
        if (key < min) {
            min = key;
        }
        if (key > max) {
            max = key;
        }
        hasValues = true;
    }

    @Override
    void mergeValues(LongStatisticsCollector page) {
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
        return Long.compare(decode(left) ^ bias, decode(right) ^ bias);
    }

    private static byte[] encode(long value) {
        return ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array();
    }

    private static long decode(byte[] bytes) {
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }
}
