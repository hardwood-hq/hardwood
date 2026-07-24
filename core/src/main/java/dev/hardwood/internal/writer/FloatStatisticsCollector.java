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

/// Accumulates a `FLOAT` column chunk's `min` / `max` / `null_count` in IEEE-754 order.
///
/// Two ordering rules the format mandates for floating point: `NaN` values never extend the
/// bounds (a chunk of only `NaN` carries a null count but no bounds), and a zero bound is
/// sign-normalized so a reader's `[min, max]` test is correct for either signed zero — the
/// `min` of a zero is written as `-0.0`, the `max` of a zero as `+0.0`. `-0.0` sorts below
/// `+0.0` via [Float#compare] while the bounds are accumulated.
final class FloatStatisticsCollector {

    private float min;
    private float max;
    private long nullCount;
    private boolean hasValues;

    void accept(float value) {
        if (Float.isNaN(value)) {
            return; // NaN never participates in min/max
        }
        if (!hasValues) {
            min = value;
            max = value;
            hasValues = true;
            return;
        }
        if (Float.compare(value, min) < 0) {
            min = value;
        }
        if (Float.compare(value, max) > 0) {
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
            minValue = encode(min == 0.0f ? -0.0f : min);
            maxValue = encode(max == 0.0f ? 0.0f : max);
        }
        return new Statistics(minValue, maxValue, nullCount, null, false);
    }

    private static byte[] encode(float value) {
        return ByteBuffer.allocate(Float.BYTES).order(ByteOrder.LITTLE_ENDIAN).putFloat(value).array();
    }
}
