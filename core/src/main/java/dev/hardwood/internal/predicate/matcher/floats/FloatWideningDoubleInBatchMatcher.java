/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate.matcher.floats;

import dev.hardwood.internal.predicate.FloatBatchMatcher;
import dev.hardwood.internal.reader.BatchExchange;

/// IN-list matcher for `float` columns against double probe values. Reads `float[]`
/// storage, widens each element to `double`, and compares with [Double#compare] to implement
/// the total order, equating all NaNs and distinguishing `-0.0` from `+0.0`.
public final class FloatWideningDoubleInBatchMatcher implements FloatBatchMatcher {

    private final double[] values;

    public FloatWideningDoubleInBatchMatcher(double[] values) {
        this.values = values;
    }

    @Override
    public void test(BatchExchange.Batch batch, long[] outWords) {
        float[] vals = (float[]) batch.values;
        int n = batch.recordCount;
        int fullWords = n >>> 6;
        int tail = n & 63;

        // Build the predicate bitmap ignoring nulls; nulls are masked out in the
        // word-wise pass below. The comparison is branchless, but C2 does not
        // vectorize it — packing a vector compare into bitmap bits has no
        // autovectorization idiom, so this compiles to a scalar cmp/setcc/shl/or
        // chain unrolled 4x. The tail is split off to keep the hot loop's trip
        // count constant at 64.
        for (int w = 0; w < fullWords; w++) {
            int base = w << 6;
            long word = 0L;
            for (int b = 0; b < 64; b++) {
                double v = vals[base + b];
                long hit = 0L;
                for (double member : values) {
                    if (Double.compare(v, member) == 0) {
                        hit = 1L;
                        break;
                    }
                }
                word |= hit << b;
            }
            outWords[w] = word;
        }
        if (tail != 0) {
            int base = fullWords << 6;
            long word = 0L;
            for (int b = 0; b < tail; b++) {
                double v = vals[base + b];
                long hit = 0L;
                for (double member : values) {
                    if (Double.compare(v, member) == 0) {
                        hit = 1L;
                        break;
                    }
                }
                word |= hit << b;
            }
            outWords[fullWords] = word;
        }

        long[] validity = batch.validity;
        if (validity != null) {
            int activeWords = (n + 63) >>> 6;
            for (int w = 0; w < activeWords; w++) {
                outWords[w] &= validity[w];
            }
        }
    }
}
