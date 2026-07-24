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
final class BooleanStatisticsCollector {

    private boolean sawFalse;
    private boolean sawTrue;
    private long nullCount;

    void accept(boolean value) {
        if (value) {
            sawTrue = true;
        }
        else {
            sawFalse = true;
        }
    }

    void acceptNull() {
        nullCount++;
    }

    Statistics toStatistics() {
        byte[] minValue = null;
        byte[] maxValue = null;
        if (sawFalse || sawTrue) {
            minValue = new byte[] { sawFalse ? (byte) 0 : (byte) 1 };
            maxValue = new byte[] { sawTrue ? (byte) 1 : (byte) 0 };
        }
        return new Statistics(minValue, maxValue, nullCount, null, false);
    }
}
