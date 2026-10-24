/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.row;

import dev.hardwood.internal.predicate.ColumnBatchMatcher;
import dev.hardwood.internal.predicate.CompiledBatchFilter;
import dev.hardwood.internal.predicate.MergePlan;
import dev.hardwood.internal.predicate.MergePlanEvaluator;
import dev.hardwood.internal.reader.batch.BatchExchange;

/// Combines the per-column [BatchExchange.Batch#matches] bitmaps a compiled
/// batch filter produces into the one per-row survivor bitmap its consumer
/// iterates.
///
/// Owns everything the merge needs — the plan, the evaluator and its scratch
/// pool, the per-column bitmap slots and the combined buffer — so a consumer
/// holds a single collaborator rather than five correlated fields.
///
/// Two axes, both decided once at construction:
///
/// - **Where the per-column bitmaps come from.** [#aliasing] serves a consumer
///   whose columns already ran the matchers into each batch's own
///   `matches` array as its cells completed: the merge reseats pointers and copies
///   nothing. [#owning] serves a consumer whose columns run no matchers — it runs
///   them itself, into buffers allocated here.
/// - **Plan shape.** A single-column plan has nothing to combine, so the merge
///   yields that one column's bitmap: no combined buffer, no evaluator, no
///   scratch pool. Any other plan goes to the evaluator, which populates the
///   combined buffer in place.
///
/// [#merge] runs once per batch, never per row.
public final class BatchMatchMerger {

    /// Projected index of the single column [#merge] yields, or `-1` when the
    /// plan is a compound and the bitmaps have to be combined. Resolved at
    /// construction so the per-batch path is an int test rather than a type test.
    private final int aliasedColumn;

    /// Per-column matchers to run over each batch, indexed by projected column
    /// index, or `null` when the consumer's columns already ran them.
    private final ColumnBatchMatcher[] matchers;
    /// Projected indices the plan reads. The only entries of [#merge]'s
    /// `batches` argument it dereferences, so a caller staging that array need
    /// keep only these current — and the only entries of [#perColumn] it
    /// touches, so the per-batch cost scales with the filter width rather than
    /// the projection width.
    private final int[] referencedColumns;
    /// Per-column bitmaps indexed by projected column index. Under [#owning]
    /// the referenced entries are buffers allocated here and overwritten by the
    /// matchers; under [#aliasing] they are reseated each batch to the batches'
    /// own arrays.
    private final long[][] perColumn;

    // Compound-plan state; all null on the single-column path.

    /// Plan describing how to combine the per-column bitmaps.
    private final MergePlan plan;
    /// Walks [#plan] each batch. Owns the depth-indexed scratch pool and is
    /// reused across batches, so the merge allocates nothing after warm-up.
    private final MergePlanEvaluator evaluator;
    /// Destination for the combined bitmap, owned and reused across batches.
    private final long[] combined;

    /// A merger for a consumer whose columns already wrote every batch's `matches` array, as
    /// [dev.hardwood.internal.reader.pipeline.ScheduledFlatColumn] does as each cell completes on
    /// the batch-matcher read path.
    ///
    /// @param plan how to combine the per-column bitmaps
    /// @param columnCount number of projected columns
    /// @param wordsLen words needed to cover the largest batch
    public static BatchMatchMerger aliasing(MergePlan plan, int columnCount, int wordsLen) {
        return new BatchMatchMerger(plan, null, columnCount, wordsLen);
    }

    /// A merger for a consumer whose columns run no matchers: [#merge] runs `filter`'s
    /// per-column matchers over the batches itself, into buffers allocated here.
    ///
    /// @param filter the compiled filter, supplying both the matchers and the plan
    /// @param columnCount number of projected columns
    /// @param wordsLen words needed to cover the largest batch
    public static BatchMatchMerger owning(CompiledBatchFilter filter, int columnCount, int wordsLen) {
        return new BatchMatchMerger(filter.mergePlan(), filter.columnMatchers(), columnCount, wordsLen);
    }

    private BatchMatchMerger(MergePlan plan, ColumnBatchMatcher[] matchers, int columnCount, int wordsLen) {
        this.matchers = matchers;
        this.referencedColumns = referencedColumns(plan, columnCount);
        this.perColumn = new long[columnCount][];
        if (matchers != null) {
            for (int c : referencedColumns) {
                perColumn[c] = new long[wordsLen];
            }
        }
        if (plan instanceof MergePlan.Column c) {
            this.aliasedColumn = c.projectedIndex();
            this.plan = null;
            this.evaluator = null;
            this.combined = null;
        }
        else {
            this.aliasedColumn = -1;
            this.plan = plan;
            this.evaluator = new MergePlanEvaluator(wordsLen);
            this.combined = new long[wordsLen];
        }
    }

    /// The projected column indices [#merge] reads from its `batches` argument.
    /// A caller that stages that array itself need only keep these entries
    /// current. Returns a copy; call it once and keep the result.
    public int[] referencedColumns() {
        return referencedColumns.clone();
    }

    /// Returns the survivor bitmap for the batch now loaded, set bit = the row
    /// matches. Valid until the next call; on the single-column
    /// [#aliasing] path it is the batch's own array, which stays valid until
    /// that batch is recycled.
    ///
    /// The combined buffer is written only over the words covering
    /// `[0, recordCount)` — bits past `recordCount` are never read by the
    /// consumer, so leaving them stale from an earlier, longer batch is safe
    /// and saves a trailing zero-fill per batch. The same holds of the
    /// per-column bitmaps, whether a column or [#bitmap] filled them.
    ///
    /// @param batches the batches being served, indexed by projected column
    ///         index; only [#referencedColumns] are dereferenced
    /// @param recordCount rows in the batch
    public long[] merge(BatchExchange.Batch[] batches, int recordCount) {
        if (aliasedColumn >= 0) {
            return bitmap(batches, aliasedColumn);
        }
        // Snapshot the bitmaps the plan reads for this batch and hand off to the
        // evaluator. Only the plan-referenced columns are touched (one pointer
        // each under aliasing); the indirection is negligible vs. the per-row
        // merge work.
        for (int i = 0; i < referencedColumns.length; i++) {
            int c = referencedColumns[i];
            perColumn[c] = bitmap(batches, c);
        }
        evaluator.eval(plan, combined, (recordCount + 63) >>> 6, perColumn);
        return combined;
    }

    /// The bitmap for projected column `c`: the batch's own array when the
    /// consumer's columns filled it, otherwise this merger's buffer, filled here.
    private long[] bitmap(BatchExchange.Batch[] batches, int c) {
        if (matchers == null) {
            return batches[c].matches;
        }
        long[] words = perColumn[c];
        matchers[c].test(batches[c], words);
        return words;
    }

    /// Collects the projected column indices referenced by `plan`, so [#merge]
    /// touches only those columns instead of all `columnCount` of them each batch.
    private static int[] referencedColumns(MergePlan plan, int columnCount) {
        boolean[] seen = new boolean[columnCount];
        markReferenced(plan, seen);
        int count = 0;
        for (int i = 0; i < columnCount; i++) {
            if (seen[i]) {
                count++;
            }
        }
        int[] result = new int[count];
        int idx = 0;
        for (int i = 0; i < columnCount; i++) {
            if (seen[i]) {
                result[idx++] = i;
            }
        }
        return result;
    }

    private static void markReferenced(MergePlan node, boolean[] seen) {
        switch (node) {
            case MergePlan.Column c -> seen[c.projectedIndex()] = true;
            case MergePlan.Compound compound -> {
                for (MergePlan child : compound.children()) {
                    markReferenced(child, seen);
                }
            }
        }
    }
}
