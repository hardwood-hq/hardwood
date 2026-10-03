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

/// Accumulates a `DOUBLE` column chunk's `min` / `max` / `null_count` / `nan_count` in IEEE-754
/// order, with the same `NaN`-exclusion, `NaN`-counting and signed-zero normalization as
/// [FloatStatisticsCollector].
final class DoubleStatisticsCollector extends StatisticsCollector<DoubleStatisticsCollector> {

    private double min;
    private double max;
    private long nanCount;
    private boolean hasValues;

    /// Extends the statistics by `value`, occurring `times` times, which only the `NaN` count
    /// depends on.
    void accept(double value, int times) {
        if (Double.isNaN(value)) {
            nanCount += times;
            return;
        }
        extend(value, value);
    }

    void accept(double value) {
        if (Double.isNaN(value)) {
            nanCount++;
            return; // NaN never participates in min/max
        }
        extend(value, value);
    }

    private void extend(double low, double high) {
        if (!hasValues) {
            min = low;
            max = high;
            hasValues = true;
            return;
        }
        if (Double.compare(low, min) < 0) {
            min = low;
        }
        if (Double.compare(high, max) > 0) {
            max = high;
        }
    }

    @Override
    void mergeValues(DoubleStatisticsCollector page) {
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
        return encode(min == 0.0 ? -0.0 : min);
    }

    @Override
    byte[] indexMax() {
        return encode(max == 0.0 ? 0.0 : max);
    }

    @Override
    int compareBounds(byte[] left, byte[] right) {
        return Double.compare(decode(left), decode(right));
    }

    private static byte[] encode(double value) {
        return ByteBuffer.allocate(Double.BYTES).order(ByteOrder.LITTLE_ENDIAN).putDouble(value).array();
    }

    private static double decode(byte[] bytes) {
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getDouble();
    }
}
