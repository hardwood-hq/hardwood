/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import dev.hardwood.internal.reader.batch.FlatBatch;

/// Per-column predicate fragment evaluated against a just-filled [FlatBatch], by the
/// job that completes a flat column's batch.
///
/// Implementations write a per-batch matches mask into the caller-provided
/// `outWords` `long[]`. A bit at index `i` is set iff row `i` definitely matches.
/// NULL rows are always excluded ("definitely matches" semantics) so word-wise
/// AND across columns is equivalent to SQL three-valued conjunction.
///
/// The matcher overwrites `outWords` in full — the caller does not need to
/// clear it beforehand. `outWords.length` must be at least
/// `(batch.recordCount + 63) >>> 6`.
///
/// The jobs completing a column's batches call its matcher at once, so a matcher keeps no
/// mutable state of its own: what it reuses between calls lives in the batch
/// ([FlatBatch#matchScratch(int, int)]) or in the batch's dictionary.
public sealed interface ColumnBatchMatcher
        permits LongBatchMatcher, DoubleBatchMatcher, IntBatchMatcher, FloatBatchMatcher,
        BooleanBatchMatcher, BinaryBatchMatcher, NullBatchMatcher, AndBatchMatcher, OrBatchMatcher {

    void test(FlatBatch batch, long[] outWords);
}
