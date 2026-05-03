/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import dev.hardwood.internal.reader.BatchExchange;

/// Per-column predicate fragment evaluated by [dev.hardwood.internal.reader.FlatColumnWorker]
/// drain threads against a just-filled [BatchExchange.Batch].
///
/// Implementations write a per-batch matches mask into the caller-provided
/// `outWords` `long[]`. A bit at index `i` is set iff row `i` definitely matches.
/// NULL rows are always excluded ("definitely matches" semantics) so word-wise
/// AND across columns is equivalent to SQL three-valued conjunction.
///
/// The matcher overwrites `outWords` in full — the caller does not need to
/// clear it beforehand. `outWords.length` must be at least
/// `(batch.recordCount + 63) >>> 6`.
public sealed interface BatchMatcher
        permits LongBatchMatcher, DoubleBatchMatcher, IntBatchMatcher, FloatBatchMatcher,
        BooleanBatchMatcher, NullBatchMatcher {

    void test(BatchExchange.Batch batch, long[] outWords);

    /// Projected column index this fragment evaluates against. The compiler
    /// installs the matcher on the [dev.hardwood.internal.reader.FlatColumnWorker]
    /// for this projected index.
    int columnIndex();
}