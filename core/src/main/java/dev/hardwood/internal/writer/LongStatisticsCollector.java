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

/// Accumulates an `INT64` column chunk's `min` / `max` / `null_count`, compared with signed
/// `INT64` ordering and encoded as 8-byte little-endian bounds. The `INT64` counterpart of
/// [IntStatisticsCollector].
final class LongStatisticsCollector {

    private long min = Long.MAX_VALUE;
    private long max = Long.MIN_VALUE;
    private long nullCount;
    private boolean hasValues;

    void accept(long value) {
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

    private static byte[] encode(long value) {
        return ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array();
    }
}
