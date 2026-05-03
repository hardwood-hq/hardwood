/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate.matcher.ints;

import java.util.BitSet;

import dev.hardwood.internal.predicate.IntBatchMatcher;
import dev.hardwood.internal.reader.BatchExchange;

public final class IntGtEqBatchMatcher implements IntBatchMatcher {

    private final int columnIndex;
    private final int literal;

    public IntGtEqBatchMatcher(int columnIndex, int literal) {
        this.columnIndex = columnIndex;
        this.literal = literal;
    }

    @Override
    public int columnIndex() {
        return columnIndex;
    }

    @Override
    public void test(BatchExchange.Batch batch, long[] outWords) {
        int[] vals = (int[]) batch.values;
        BitSet nulls = batch.nulls;
        int n = batch.recordCount;
        int lit = literal;
        int fullWords = n >>> 6;
        int tail = n & 63;

        if (nulls == null) {
            for (int w = 0; w < fullWords; w++) {
                int base = w << 6;
                long word = 0L;
                for (int b = 0; b < 64; b++) {
                    word |= ((vals[base + b] >= lit) ? 1L : 0L) << b;
                }
                outWords[w] = word;
            }
            if (tail != 0) {
                int base = fullWords << 6;
                long word = 0L;
                for (int b = 0; b < tail; b++) {
                    word |= ((vals[base + b] >= lit) ? 1L : 0L) << b;
                }
                outWords[fullWords] = word;
            }
        }
        else {
            for (int w = 0; w < fullWords; w++) {
                int base = w << 6;
                long word = 0L;
                for (int b = 0; b < 64; b++) {
                    int i = base + b;
                    word |= ((!nulls.get(i) && vals[i] >= lit) ? 1L : 0L) << b;
                }
                outWords[w] = word;
            }
            if (tail != 0) {
                int base = fullWords << 6;
                long word = 0L;
                for (int b = 0; b < tail; b++) {
                    int i = base + b;
                    word |= ((!nulls.get(i) && vals[i] >= lit) ? 1L : 0L) << b;
                }
                outWords[fullWords] = word;
            }
        }

        int firstTrailing = (tail == 0) ? fullWords : fullWords + 1;
        for (int w = firstTrailing; w < outWords.length; w++) {
            outWords[w] = 0L;
        }
    }
}
