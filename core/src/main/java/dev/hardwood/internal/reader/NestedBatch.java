/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

/// Batch holder for nested schemas, used by [NestedColumnWorker].
///
/// This is a separate class from [BatchExchange.Batch] (not a subclass) so
/// that the JIT's class hierarchy analysis keeps `Batch` as a leaf type,
/// preserving field-access optimizations on the flat hot path.
///
/// Nested batches are allocated and managed by [NestedColumnWorker]'s drain
/// thread and published through a `BatchExchange<NestedBatch>`.
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
    // Raw arrays (filled by drain assembly)
    public Object values;
    public int recordCount;
    public int valueCount;
    public int[] definitionLevels;
    public int[] repetitionLevels;
    public int[] recordOffsets;

    // File name of the originating file (set by drain before publish)
    public String fileName;

    /// Elements per row when this batch was assembled via the fixed-size-list
    /// fast path (every record a present list of exactly this many elements),
    /// or `0` for a regular batch. When set, `definitionLevels` /
    /// `repetitionLevels` are `null` and the record/layer offsets are implicit
    /// multiples of this value.
    public int fixedListK;

    // Pre-computed index (computed by drain before publish). Validity bit set
    // iff present. Null means "all items at that layer are present in this
    // batch."
    public long[] elementValidity;
    public int[][] multiLevelOffsets;

    /// Real-items view, computed by the drain on the [dev.hardwood.reader.ColumnReader]
    /// (real-items) path so the serial consumer reads it without a level scan.
    /// `null` on the all-items path and on batches derived by consumer-side record
    /// selection (for which the consumer builds a view lazily from the sliced levels).
    public NestedLevelComputer.RealView realView;

    /// Real-items-only leaf values, gathered by the drain from [#values] when the
    /// batch has phantom positions, so the compaction runs off the serial consumer.
    /// `null` when no gather is needed (`realView.realToRawLeaf() == null`: the raw
    /// [#values] already pass through) or on paths that compact lazily.
    public Object realValues;
}
