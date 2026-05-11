/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import dev.hardwood.internal.reader.BatchExchange;

/// Composes two [ColumnBatchMatcher]s evaluated against the same column and
/// word-AND merges their bit masks. Used when an eligible `And` has multiple
/// leaves on a single column (e.g. `id >= x AND id <= y`).
///
/// Both children must be valid matchers for the column's batch — the composite
/// is type-agnostic and delegates the `batch.values` cast to each child. The
/// scratch buffer is allocated lazily on the first [#test] call and reused
/// across batches; the matcher is intended to be called sequentially by a
/// single drain thread.
public final class AndBatchMatcher implements ColumnBatchMatcher {

    private final ColumnBatchMatcher first;
    private final ColumnBatchMatcher second;
    private long[] scratch;

    public AndBatchMatcher(ColumnBatchMatcher first, ColumnBatchMatcher second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public void test(BatchExchange.Batch batch, long[] outWords) {
        if (scratch == null || scratch.length < outWords.length) {
            scratch = new long[outWords.length];
        }
        first.test(batch, outWords);
        second.test(batch, scratch);
        int activeWords = (batch.recordCount + 63) >>> 6;
        for (int i = 0; i < activeWords; i++) {
            outWords[i] &= scratch[i];
        }
    }
}
