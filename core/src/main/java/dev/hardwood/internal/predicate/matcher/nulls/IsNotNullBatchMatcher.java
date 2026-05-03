/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate.matcher.nulls;

import java.util.BitSet;

import dev.hardwood.internal.predicate.NullBatchMatcher;
import dev.hardwood.internal.reader.BatchExchange;

/// IS NOT NULL: bit `i` is set iff row `i` is not null. Bulk-inverts the BitSet's
/// underlying long-words and masks off bits past `recordCount`.
public final class IsNotNullBatchMatcher implements NullBatchMatcher {

    private final int columnIndex;

    public IsNotNullBatchMatcher(int columnIndex) {
        this.columnIndex = columnIndex;
    }

    @Override
    public int columnIndex() {
        return columnIndex;
    }

    @Override
    public void test(BatchExchange.Batch batch, long[] outWords) {
        BitSet nulls = batch.nulls;
        int n = batch.recordCount;
        int wordsForN = (n + 63) >>> 6;
        int fullWords = n >>> 6;
        int tail = n & 63;

        if (nulls == null) {
            // Every row is non-null — set all bits up to recordCount.
            for (int w = 0; w < fullWords; w++) {
                outWords[w] = -1L;
            }
            if (tail != 0) {
                outWords[fullWords] = (1L << tail) - 1L;
            }
            for (int w = wordsForN; w < outWords.length; w++) {
                outWords[w] = 0L;
            }
            return;
        }
        long[] nullBits = nulls.toLongArray();
        int copy = Math.min(nullBits.length, wordsForN);
        for (int w = 0; w < copy; w++) {
            outWords[w] = ~nullBits[w];
        }
        for (int w = copy; w < wordsForN; w++) {
            outWords[w] = -1L;
        }
        if (tail != 0 && wordsForN > 0) {
            outWords[wordsForN - 1] &= (1L << tail) - 1L;
        }
        for (int w = wordsForN; w < outWords.length; w++) {
            outWords[w] = 0L;
        }
    }
}
