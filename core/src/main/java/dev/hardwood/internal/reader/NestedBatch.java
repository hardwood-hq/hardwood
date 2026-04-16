/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.BitSet;

/// Batch holder for nested schemas, used by [NestedColumnWorker].
///
/// This is a separate class from [BatchExchange.Batch] (not a subclass) so
/// that the JIT's class hierarchy analysis keeps `Batch` as a leaf type,
/// preserving field-access optimizations on the flat hot path.
///
/// Nested batches are allocated and managed by [NestedColumnWorker]'s drain
/// thread and published through a `BatchExchange<NestedBatch>`.
///
/// The drain computes index structures (element nulls, multi-level offsets,
/// level nulls) before publishing, so the consumer thread does not need to
/// perform any expensive index computation.
public final class NestedBatch {
    // Raw arrays (filled by drain assembly)
    public Object values;
    public int recordCount;
    public int valueCount;
    public int[] definitionLevels;
    public int[] repetitionLevels;
    public int[] recordOffsets;

    // Pre-computed index (computed by drain before publish)
    public BitSet elementNulls;
    public int[][] multiLevelOffsets;
    public BitSet[] levelNulls;
}
