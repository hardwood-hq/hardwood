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

/// Accumulates a `FLOAT` column chunk's `min` / `max` / `null_count` / `nan_count` in IEEE-754
/// order.
///
/// Two ordering rules the format mandates for floating point: `NaN` values never extend the
/// bounds (a chunk of only `NaN` carries a null count and a NaN count but no bounds), and a zero
/// bound is sign-normalized so a reader's `[min, max]` test is correct for either signed zero —
/// the `min` of a zero is written as `-0.0`, the `max` of a zero as `+0.0`. `-0.0` sorts below
/// `+0.0` via [Float#compare] while the bounds are accumulated.
///
/// The NaN count is recorded for every chunk, zero included: the format requires it of a
/// floating-point column under the type-defined order, and a recorded `0` is the only thing that
/// proves a chunk holds no `NaN` — a count left absent tells a reader nothing, since the bounds
/// say nothing about `NaN` either way.
final class FloatStatisticsCollector extends StatisticsCollector<FloatStatisticsCollector> {

    private float min;
    private float max;
    private long nanCount;
    private boolean hasValues;

    /// Extends the statistics by `value`, occurring `times` times, which only the `NaN` count
    /// depends on.
    void accept(float value, int times) {
        if (Float.isNaN(value)) {
            nanCount += times;
            return;
        }
        extend(value, value);
    }

    void accept(float value) {
        if (Float.isNaN(value)) {
            nanCount++;
            return; // NaN never participates in min/max
        }
        extend(value, value);
    }

    private void extend(float low, float high) {
        if (!hasValues) {
            min = low;
            max = high;
            hasValues = true;
            return;
        }
        if (Float.compare(low, min) < 0) {
            min = low;
        }
        if (Float.compare(high, max) > 0) {
            max = high;
        }
    }

    @Override
    void mergeValues(FloatStatisticsCollector page) {
        nanCount += page.nanCount;
        if (page.hasValues) {
            extend(page.min, page.max);
        }
    }

    @Override
    boolean hasValues() {
        return hasValues;
    }

    @Override
    long nanCount() {
        return nanCount;
    }

    @Override
    Statistics toStatistics() {
        byte[] minValue = hasValues ? indexMin() : null;
        byte[] maxValue = hasValues ? indexMax() : null;
        return new Statistics(minValue, maxValue, nullCount, null, false, true, true, nanCount);
    }

    @Override
    byte[] indexMin() {
        return encode(min == 0.0f ? -0.0f : min);
    }

    @Override
    byte[] indexMax() {
        return encode(max == 0.0f ? 0.0f : max);
    }

    @Override
    int compareBounds(byte[] left, byte[] right) {
        return Float.compare(decode(left), decode(right));
    }

    private static byte[] encode(float value) {
        return ByteBuffer.allocate(Float.BYTES).order(ByteOrder.LITTLE_ENDIAN).putFloat(value).array();
    }

    private static float decode(byte[] bytes) {
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getFloat();
    }
}
