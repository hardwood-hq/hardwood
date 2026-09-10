/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate.matcher.longs;

import dev.hardwood.internal.predicate.LongBatchMatcher;
import dev.hardwood.internal.reader.BatchExchange;

/// [LongGtBatchMatcher] for a column annotated `INT(64, isSigned = false)`, whose values order by
/// unsigned magnitude. Both sides are biased by `Long.MIN_VALUE`, which moves the sign bit
/// out of the way and leaves the order within each half untouched; the bias is a constant XOR,
/// so the loop vectorizes as the signed one does.
public final class UnsignedLongGtBatchMatcher implements LongBatchMatcher {

    private static final long BIAS = Long.MIN_VALUE;

    private final long biasedLiteral;

    public UnsignedLongGtBatchMatcher(long literal) {
        this.biasedLiteral = literal ^ BIAS;
    }

    @Override
    public void test(BatchExchange.Batch batch, long[] outWords) {
        long[] vals = (long[]) batch.values;
        int n = batch.recordCount;
        long lit = biasedLiteral;
        int fullWords = n >>> 6;
        int tail = n & 63;

        for (int w = 0; w < fullWords; w++) {
            int base = w << 6;
            long word = 0L;
            for (int b = 0; b < 64; b++) {
                word |= (((vals[base + b] ^ BIAS) > lit) ? 1L : 0L) << b;
            }
            outWords[w] = word;
        }
        if (tail != 0) {
            int base = fullWords << 6;
            long word = 0L;
            for (int b = 0; b < tail; b++) {
                word |= (((vals[base + b] ^ BIAS) > lit) ? 1L : 0L) << b;
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
