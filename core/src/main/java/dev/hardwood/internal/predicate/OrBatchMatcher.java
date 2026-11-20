/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import dev.hardwood.internal.reader.batch.FlatBatch;

/// Composes two [ColumnBatchMatcher]s evaluated against the same column and
/// word-OR merges their bit masks. Used when an eligible subtree has multiple
/// leaves on a single column joined by `Or` (e.g. `id < -5 OR id > 5`).
///
/// Under the "definitely matches, nulls excluded" semantics of
/// [ColumnBatchMatcher], a bitwise OR across leaves is equivalent to SQL
/// three-valued disjunction: a row survives iff at least one leaf definitely
/// matches it.
///
/// Both children must be valid matchers for the column's batch — the composite
/// is type-agnostic and delegates the `batch.values` cast to each child. The
/// second child's mask goes to the batch's scratch bitmap at this composite's
/// [#slot()], so the composite holds no state of its own and the tasks completing
/// a column's batches call it at once.
public final class OrBatchMatcher implements ColumnBatchMatcher {

    private final ColumnBatchMatcher first;
    private final ColumnBatchMatcher second;
    private final int slot;

    /// @param slot the batch scratch bitmap this composite uses, distinct from every other
    ///        composite in the column's fragment ([BatchFilterCompiler] numbers them)
    public OrBatchMatcher(ColumnBatchMatcher first, ColumnBatchMatcher second, int slot) {
        this.first = first;
        this.second = second;
        this.slot = slot;
    }

    @Override
    public void test(FlatBatch batch, long[] outWords) {
        long[] scratch = batch.matchScratch(slot, outWords.length);
        first.test(batch, outWords);
        second.test(batch, scratch);
        int activeWords = (batch.recordCount + 63) >>> 6;
        for (int i = 0; i < activeWords; i++) {
            outWords[i] |= scratch[i];
        }
    }

    ColumnBatchMatcher first() {
        return first;
    }

    ColumnBatchMatcher second() {
        return second;
    }

    int slot() {
        return slot;
    }
}
