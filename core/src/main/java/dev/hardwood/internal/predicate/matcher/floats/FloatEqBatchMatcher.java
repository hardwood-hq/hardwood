/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate.matcher.floats;

import java.util.BitSet;

import dev.hardwood.internal.predicate.FloatBatchMatcher;
import dev.hardwood.internal.reader.BatchExchange;

public final class FloatEqBatchMatcher implements FloatBatchMatcher {

    private final int columnIndex;
    private final float literal;

    public FloatEqBatchMatcher(int columnIndex, float literal) {
        this.columnIndex = columnIndex;
        this.literal = literal;
    }

    @Override
    public int columnIndex() {
        return columnIndex;
    }

    @Override
    public void test(BatchExchange.Batch batch, long[] outWords) {
        float[] vals = (float[]) batch.values;
        int n = batch.recordCount;
        float lit = literal;
        int fullWords = n >>> 6;
        int tail = n & 63;

        // Build the predicate bitmap ignoring nulls. The inner loop is fixed at 64
        // iterations and uses a branchless `(cond ? 1 : 0) << b` pack so HotSpot
        // fully unrolls it and auto-vectorizes the comparison. The tail is split
        // off to keep the hot loop's trip count constant at 64.
        for (int w = 0; w < fullWords; w++) {
            int base = w << 6;
            long word = 0L;
            for (int b = 0; b < 64; b++) {
                word |= ((Float.compare(vals[base + b], lit) == 0) ? 1L : 0L) << b;
            }
            outWords[w] = word;
        }
        if (tail != 0) {
            int base = fullWords << 6;
            long word = 0L;
            for (int b = 0; b < tail; b++) {
                word |= ((Float.compare(vals[base + b], lit) == 0) ? 1L : 0L) << b;
            }
            outWords[fullWords] = word;
        }

        // Clear bits at null positions. Bits past `n` are intentionally left stale —
        // the consumer (FlatRowReader#intersectMatches) only touches the words
        // covering `[0, n)`, so the trailing zero-fill would be dead work.
        BitSet nulls = batch.nulls;
        if (nulls != null) {
            for (int i = nulls.nextSetBit(0); i >= 0 && i < n; i = nulls.nextSetBit(i + 1)) {
                outWords[i >>> 6] &= ~(1L << (i & 63));
            }
        }
    }
}
