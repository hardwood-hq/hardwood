/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate.matcher.nulls;

import dev.hardwood.internal.predicate.NullBatchMatcher;
import dev.hardwood.internal.reader.BatchExchange;

/// IS NOT NULL: bit `i` is set iff row `i` is not null. Bulk-copies the
/// validity bitmap (set bit = present) across the live range.
public final class IsNotNullBatchMatcher implements NullBatchMatcher {

    @Override
    public void test(BatchExchange.Batch batch, long[] outWords) {
        long[] validity = batch.validity;
        int wordsForN = (batch.recordCount + 63) >>> 6;

        if (validity == null) {
            // Every row is present — set every bit in the live range.
            for (int w = 0; w < wordsForN; w++) {
                outWords[w] = -1L;
            }
            return;
        }
        System.arraycopy(validity, 0, outWords, 0, wordsForN);
        // Bits past `n` in the last live word are intentionally left as-is —
        // the consumer (FlatRowReader#intersectMatches) only touches the words
        // covering `[0, n)`, so masking would be dead work.
    }
}
