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

/// No row: every bit of the live range is cleared. The answer to a comparison whose literal lies
/// past every value the column can hold, in the direction the operator admits.
public final class NoRowBatchMatcher implements NullBatchMatcher {

    @Override
    public void test(BatchExchange.Batch batch, long[] outWords) {
        int wordsForN = (batch.recordCount + 63) >>> 6;
        for (int w = 0; w < wordsForN; w++) {
            outWords[w] = 0L;
        }
    }
}
