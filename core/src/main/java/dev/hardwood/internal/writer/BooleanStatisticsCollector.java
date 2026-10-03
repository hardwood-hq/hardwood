/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import dev.hardwood.metadata.Statistics;

/// Accumulates a `BOOLEAN` column chunk's `min` / `max` / `null_count` with `false < true`
/// ordering; each bound is a single byte (`0` / `1`).
final class BooleanStatisticsCollector extends StatisticsCollector<BooleanStatisticsCollector> {

    private static final byte FALSE = 0;
    private static final byte TRUE = 1;

    private boolean sawFalse;
    private boolean sawTrue;

    void accept(boolean value) {
        if (value) {
            sawTrue = true;
        }
        else {
            sawFalse = true;
        }
    }

    @Override
    void mergeValues(BooleanStatisticsCollector page) {
        sawFalse |= page.sawFalse;
        sawTrue |= page.sawTrue;
    }

    @Override
    boolean hasValues() {
        return sawFalse || sawTrue;
    }

    @Override
    Statistics toStatistics() {
        byte[] minValue = hasValues() ? indexMin() : null;
        byte[] maxValue = hasValues() ? indexMax() : null;
        return new Statistics(minValue, maxValue, nullCount, null, false);
    }

    @Override
    byte[] indexMin() {
        return new byte[] { sawFalse ? FALSE : TRUE };
    }

    @Override
    byte[] indexMax() {
        return new byte[] { sawTrue ? TRUE : FALSE };
    }

    @Override
    int compareBounds(byte[] left, byte[] right) {
        return Byte.compare(left[0], right[0]);
    }
}
