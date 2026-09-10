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

public final class FloatGtBatchMatcher implements FloatBatchMatcher {

    private final float literal;

    public FloatGtBatchMatcher(float literal) {
        this.literal = literal;
    }

    @Override
    public void test(BatchExchange.Batch batch, long[] outWords) {
        float[] vals = (float[]) batch.values;
        int n = batch.recordCount;
        float lit = literal;
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
                word |= ((Float.compare(vals[base + b], lit) > 0) ? 1L : 0L) << b;
            }
            outWords[w] = word;
        }
        if (tail != 0) {
            int base = fullWords << 6;
            long word = 0L;
            for (int b = 0; b < tail; b++) {
                word |= ((Float.compare(vals[base + b], lit) > 0) ? 1L : 0L) << b;
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
