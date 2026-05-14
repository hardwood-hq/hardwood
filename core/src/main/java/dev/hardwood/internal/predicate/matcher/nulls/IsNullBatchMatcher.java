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

/// IS NULL: bit `i` is set iff row `i` is null. Bulk-inverts the validity
/// bitmap (set bit = present) across the live range.
public final class IsNullBatchMatcher implements NullBatchMatcher {

    @Override
    public void test(BatchExchange.Batch batch, long[] outWords) {
        BitSet validity = batch.validity;
        int n = batch.recordCount;
        int wordsForN = (n + 63) >>> 6;

        if (validity == null) {
            // Every row is present — no nulls.
            for (int w = 0; w < wordsForN; w++) {
                outWords[w] = 0L;
            }
            return;
        }
        long[] validityBits = validity.toLongArray();
        int copy = Math.min(validityBits.length, wordsForN);
        for (int w = 0; w < copy; w++) {
            outWords[w] = ~validityBits[w];
        }
        // Words past validity's backing array hold no set bits — every leaf at
        // those positions is absent, so the IS NULL predicate matches.
        for (int w = copy; w < wordsForN; w++) {
            outWords[w] = -1L;
        }
        // Bits past `n` (in the last live word and in stale trailing words) are
        // intentionally left as-is — the consumer (FlatRowReader#intersectMatches)
        // only touches the words covering `[0, n)`, so masking would be dead work.
    }
}
