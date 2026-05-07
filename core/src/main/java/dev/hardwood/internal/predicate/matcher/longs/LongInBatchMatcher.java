/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate.matcher.longs;

import java.util.BitSet;

import dev.hardwood.internal.predicate.LongBatchMatcher;
import dev.hardwood.internal.reader.BatchExchange;

/// IN-list matcher for `long` columns. Linear scan over the value list — IN lists
/// in practice have only a handful of entries.
public final class LongInBatchMatcher implements LongBatchMatcher {

    private final int columnIndex;
    private final long[] values;

    public LongInBatchMatcher(int columnIndex, long[] values) {
        this.columnIndex = columnIndex;
        this.values = values;
    }

    @Override
    public int columnIndex() {
        return columnIndex;
    }

    @Override
    public void test(BatchExchange.Batch batch, long[] outWords) {
        long[] vals = (long[]) batch.values;
        int n = batch.recordCount;
        int fullWords = n >>> 6;
        int tail = n & 63;

        // Build the predicate bitmap ignoring nulls. The inner loop is fixed at 64
        // iterations and uses a branchless `(cond ? 1 : 0) << b` pack so HotSpot
        // can fully unroll it. The tail is split off to keep the hot loop's trip
        // count constant at 64.
        for (int w = 0; w < fullWords; w++) {
            int base = w << 6;
            long word = 0L;
            for (int b = 0; b < 64; b++) {
                long v = vals[base + b];
                long hit = 0L;
                for (long member : values) {
                    if (member == v) { hit = 1L; break; }
                }
                word |= hit << b;
            }
            outWords[w] = word;
        }
        if (tail != 0) {
            int base = fullWords << 6;
            long word = 0L;
            for (int b = 0; b < tail; b++) {
                long v = vals[base + b];
                long hit = 0L;
                for (long member : values) {
                    if (member == v) { hit = 1L; break; }
                }
                word |= hit << b;
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
