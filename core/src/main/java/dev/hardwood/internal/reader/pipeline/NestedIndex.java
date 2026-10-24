/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.pipeline;

import dev.hardwood.internal.reader.batch.NestedBatch;
import dev.hardwood.internal.reader.batch.NestedIndexMode;
import dev.hardwood.internal.reader.batch.NestedLevelComputer;

/// Derives the structures a nested batch carries for its reader before it is published, off the
/// serial consumer: what each [NestedIndexMode] says the reader reads.
///
/// Set bits of a validity indicate **present** values; a `null` validity means every item at that
/// scope is present in the batch. `multiLevelOffsets` is indexed by **layer** (one slot per
/// `STRUCT` / `REPEATED` layer): `null` for `STRUCT` layers and sentinel-suffixed for `REPEATED`
/// layers.
final class NestedIndex {

    private NestedIndex() {
    }

    /// The real-items view of a batch on `REAL_VIEW`, built from its levels before its values are
    /// written, so that the fill writes each real leaf value straight to its real position.
    ///
    /// The consumer reads only the real-items view and values. An all-present batch has no phantom
    /// positions, so its offsets equal the all-items offsets: only those are built. A batch with
    /// phantom positions builds the full view, without the real-to-raw map, as its values are never
    /// written at their raw positions. Either way a structural read gets its offsets without a
    /// consumer-side scan, and a flat leaf read ignores them.
    ///
    /// @param definitionLevels the batch's definition levels for `[0, valueCount)`; `null` for a
    ///        fixed-width or an all-present batch
    /// @param repetitionLevels the batch's repetition levels likewise; `null` for a fixed-width
    ///        batch or a column without repetition
    static NestedLevelComputer.RealView realView(int[] definitionLevels, int[] repetitionLevels, int valueCount,
                                                 int recordCount, int fixedListK, boolean allPresent,
                                                 int maxDefinitionLevel, NestedLevelComputer.Layers layers) {
        if (fixedListK > 0) {
            return new NestedLevelComputer.RealView(
                    NestedLevelComputer.fixedListLayerOffsets(fixedListK, recordCount, layers),
                    new long[layers.count()][], null, valueCount, null);
        }
        if (allPresent || allDefsMax(definitionLevels, valueCount, maxDefinitionLevel)) {
            return new NestedLevelComputer.RealView(
                    NestedLevelComputer.computeLayerOffsets(repetitionLevels, valueCount, recordCount, layers),
                    new long[layers.count()][], null, valueCount, null);
        }
        return NestedLevelComputer.computeRealView(definitionLevels, repetitionLevels, valueCount, recordCount,
                maxDefinitionLevel, layers, false);
    }

    /// Computes the batch's all-items index for `mode` from its levels. A batch on `REAL_VIEW`
    /// carries its [#realView] instead, and computes nothing here.
    ///
    /// @param definitionLevels the batch's definition levels for `[0, valueCount)`; `null` for a
    ///        fixed-width batch
    /// @param repetitionLevels the batch's repetition levels likewise
    static void compute(NestedBatch batch, int[] definitionLevels, int[] repetitionLevels, int maxDefinitionLevel,
                        NestedLevelComputer.Layers layers, NestedIndexMode mode) {
        if (mode == NestedIndexMode.REAL_VIEW) {
            batch.elementValidity = null;
            batch.multiLevelOffsets = null;
            return;
        }
        batch.realView = null;
        batch.realValues = null;

        if (mode == NestedIndexMode.REAL_VIEW_KEEP_LEVELS) {
            // Exact-filtered ColumnReader: record selection compacts the batch from the raw levels
            // before the consumer reads it, and the consumer builds the view lazily; the
            // SelectionEngine derives the element validity of the predicate columns itself.
            batch.elementValidity = null;
            batch.multiLevelOffsets = null;
            return;
        }

        if (batch.fixedListK > 0) {
            // All elements present, boundaries at multiples of k: no validity and arithmetic
            // layer offsets, skipping the level scans.
            batch.elementValidity = null;
            batch.multiLevelOffsets = NestedLevelComputer.fixedListLayerOffsets(
                    batch.fixedListK, batch.recordCount, layers);
            return;
        }

        int valueCount = batch.valueCount;
        batch.elementValidity = NestedLevelComputer.computeElementValidity(
                definitionLevels, valueCount, maxDefinitionLevel);
        batch.multiLevelOffsets = layers != null && layers.count() > 0 && valueCount > 0
                ? NestedLevelComputer.computeLayerOffsets(repetitionLevels, valueCount, batch.recordCount, layers)
                : null;
    }

    /// Whether every leaf of the batch is present (definition level at max): no null or empty
    /// parents, so a flat consumer needs neither compaction nor per-layer offsets.
    private static boolean allDefsMax(int[] definitionLevels, int n, int maxDefinitionLevel) {
        if (definitionLevels == null) {
            return true;
        }
        for (int i = 0; i < n; i++) {
            if (definitionLevels[i] != maxDefinitionLevel) {
                return false;
            }
        }
        return true;
    }
}
