/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.batch;

import java.util.Arrays;

/// A flat column's batch: the column's values for one batch of the read's batch plan. The
/// column's cell fills it and its exchange publishes it to the consumer, which reads from it; a
/// recycling exchange reuses it.
///
/// `validity` carries set-bit-= -present semantics: a set bit at position
/// `i` means the leaf value at that position is present. Packed into
/// `long[]` of length `(recordCount + 63) >>> 6` (each word covers 64
/// consecutive rows, low bit = lowest row). `null` is the sparse
/// representation of "every leaf in this batch is present."
///
/// `values` is a typed primitive array (`int[]`, `long[]`, …) for
/// fixed-width physical types and a [BinaryBatchValues] for byte-array
/// types.
public final class FlatBatch {
    public Object values;
    public long[] validity;
    /// The definition level of each row, for a leaf whose levels tell a reader more than
    /// [#validity] does: whether a struct above it is null. `null` for any other column,
    /// which the fill then leaves at its validity alone. When non-null, sized to the
    /// batch capacity and overwritten on every fill.
    public int[] definitionLevels;
    public int recordCount;
    /// The rows [#values] was allocated for, at least [#recordCount]. A batch ended early,
    /// at a file or by a row cap, can hold far fewer rows than its arrays have room for, and
    /// a detaching exchange limits its ready queue by this so that the limit bounds memory.
    public int capacity;
    public String fileName;
    /// Per-batch matches mask, populated by
    /// [dev.hardwood.internal.predicate.ColumnBatchMatcher] on the task that completes the
    /// batch when batch-matcher filtering is enabled. `null` means "no fragment for this
    /// column" — [dev.hardwood.internal.reader.row.BatchMatchMerger] never reads those columns,
    /// since it visits exactly the ones its plan references. When non-null, sized to
    /// `(batchCapacity + 63) >>> 6` and overwritten on every batch-matcher test call.
    public long[] matches;

    /// Whether statistics proved every record of this batch matches the filter
    /// predicate, making [#matches] all-ones without per-record evaluation.
    /// Only set on the batch-matcher filter path; `false` is always safe.
    public boolean filterAlwaysMatches;

    /// The bitmaps a compound filter fragment keeps a child's mask in while it combines it with
    /// another's, by slot; see [#matchScratch(int, int)].
    private long[][] matchScratch;

    /// Scratch bitmap `slot` of this batch, at least `words` long. A compound matcher of the
    /// column's filter fragment ([dev.hardwood.internal.predicate.AndBatchMatcher],
    /// [dev.hardwood.internal.predicate.OrBatchMatcher]) keeps its second child's mask here.
    /// The batch owns it, and one task completes a batch at a time, so the tasks completing a
    /// column's batches call the fragment at once without sharing a bitmap. Allocated on first
    /// use and kept while the batch object is recycled.
    public long[] matchScratch(int slot, int words) {
        long[][] scratch = matchScratch;
        if (scratch == null || scratch.length <= slot) {
            scratch = scratch == null ? new long[slot + 1][] : Arrays.copyOf(scratch, slot + 1);
            matchScratch = scratch;
        }
        long[] bitmap = scratch[slot];
        if (bitmap == null || bitmap.length < words) {
            bitmap = new long[words];
            scratch[slot] = bitmap;
        }
        return bitmap;
    }
}
