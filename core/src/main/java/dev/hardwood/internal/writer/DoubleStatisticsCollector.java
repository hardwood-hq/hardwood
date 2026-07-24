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

/// Accumulates a `DOUBLE` column chunk's `min` / `max` / `null_count` in IEEE-754 order, with
/// the same `NaN`-exclusion and signed-zero normalization as [FloatStatisticsCollector].
final class DoubleStatisticsCollector {

    private double min;
    private double max;
    private long nullCount;
    private boolean hasValues;

    void accept(double value) {
        if (Double.isNaN(value)) {
            return; // NaN never participates in min/max
        }
        if (!hasValues) {
            min = value;
            max = value;
            hasValues = true;
            return;
        }
        if (Double.compare(value, min) < 0) {
            min = value;
        }
        if (Double.compare(value, max) > 0) {
            max = value;
        }
    }

    void acceptNull() {
        nullCount++;
    }

    Statistics toStatistics() {
        byte[] minValue = null;
        byte[] maxValue = null;
        if (hasValues) {
            minValue = encode(min == 0.0 ? -0.0 : min);
            maxValue = encode(max == 0.0 ? 0.0 : max);
        }
        return new Statistics(minValue, maxValue, nullCount, null, false);
    }

    private static byte[] encode(double value) {
        return ByteBuffer.allocate(Double.BYTES).order(ByteOrder.LITTLE_ENDIAN).putDouble(value).array();
    }
}
