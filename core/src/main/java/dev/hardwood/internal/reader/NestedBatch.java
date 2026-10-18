/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

/// Batch holder for nested schemas, filled by [ScheduledNestedColumn].
///
/// This is a separate class from [BatchExchange.Batch] (not a subclass) so
/// that the JIT's class hierarchy analysis keeps `Batch` as a leaf type,
/// preserving field-access optimizations on the flat hot path.
///
/// A nested batch is filled as its cell completes ([NestedFill]) and published through a
/// `BatchExchange<NestedBatch>`.
///
/// **Polarity:** validity bitmaps carry set-bit-= -present semantics. A
/// `null` reference is the sparse representation of "every item at that
/// scope is present in this batch."
///
/// `multiLevelOffsets[k]` is **layer-indexed** — one slot per `STRUCT` /
/// `REPEATED` layer, with `STRUCT` slots holding `null` (no offsets) and
/// `REPEATED` slots holding sentinel-suffixed offsets: length
/// `count(k) + 1`, with the final entry equal to the count at the next
/// inner level (or [#valueCount] for the innermost).
public final class NestedBatch {
    // Raw arrays (written by the cell's fill, NestedFill)
    public Object values;
    public int recordCount;
    public int valueCount;
    public int[] definitionLevels;
    public int[] repetitionLevels;
    public int[] recordOffsets;

    // File name of the originating file (set by the cell's fill before publish)
    public String fileName;

    /// Whether statistics proved every record of this batch matches the filter
    /// predicate, so a filtering reader can return the batch without evaluating it.
    /// Only set when a filter is installed; `false` is always safe.
    public boolean filterAlwaysMatches;

    /// Elements per row when this batch was assembled via the fixed-size-list
    /// fast path (every record a present list of exactly this many elements),
    /// or `0` for a regular batch. When set, `definitionLevels` /
    /// `repetitionLevels` are `null` and the record/layer offsets are implicit
    /// multiples of this value.
    public int fixedListK;

    /// True when every leaf in this batch is present (no null or empty parents) —
    /// set by the fill when every contributing page was measured all-present, so
    /// the index computation can skip the `O(valueCount)` definition-level scan that would
    /// otherwise re-derive the same fact.
    public boolean allPresent;

    // Pre-computed index. Validity bit set iff present. Null means "all items
    // at that layer are present in this batch." The cell's fill computes both before
    // publish, except on the exact-filtered ColumnReader path: there it leaves
    // both null, and the SelectionEngine derives elementValidity for its
    // predicate columns.
    public long[] elementValidity;
    public int[][] multiLevelOffsets;

    /// Real-items view, computed by the cell's fill on the [dev.hardwood.reader.ColumnReader]
    /// (real-items) path so the serial consumer reads it without a level scan.
    /// `null` on the all-items path and on batches derived by consumer-side record
    /// selection (for which the consumer builds a view lazily from the sliced levels).
    public NestedLevelComputer.RealView realView;

    /// Real-items-only leaf values, set with [#realView]: gathered by the cell's fill from
    /// [#values] when the batch has phantom positions, so the compaction runs off the serial
    /// consumer, and [#values] itself when no gather is needed (`realView.realToRawLeaf() ==
    /// null`). `null` wherever [#realView] is.
    public Object realValues;
}
