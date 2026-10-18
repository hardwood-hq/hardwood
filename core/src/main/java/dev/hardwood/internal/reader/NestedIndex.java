/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

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

    /// Computes the batch's index for `mode` from its levels.
    ///
    /// @param definitionLevels the batch's definition levels for `[0, valueCount)`, which the
    ///        batch itself carries unless `mode` is `REAL_VIEW`; `null` for a fixed-width batch,
    ///        and on `REAL_VIEW` for an all-present batch
    /// @param repetitionLevels the batch's repetition levels likewise; on `REAL_VIEW` `null` also
    ///        for a column without repetition
    static void compute(NestedBatch batch, int[] definitionLevels, int[] repetitionLevels, int maxDefinitionLevel,
                        NestedLevelComputer.Layers layers, NestedIndexMode mode) {
        if (mode == NestedIndexMode.REAL_VIEW) {
            // Unfiltered ColumnReader: the consumer reads only the real-items view and values. An
            // all-present batch has no phantom positions, so its offsets equal the all-items
            // offsets: only those are built, and the dense values pass through. A batch with
            // phantom positions builds the full view and gathers the real leaf values. Either way
            // a structural read gets its offsets without a consumer-side scan, and a flat leaf
            // read ignores them.
            boolean allPresent = batch.fixedListK > 0 || batch.allPresent
                    || allDefsMax(definitionLevels, batch.valueCount, maxDefinitionLevel);
            if (allPresent) {
                batch.realValues = batch.values;
                batch.realView = allPresentView(batch, repetitionLevels, layers);
            }
            else {
                NestedLevelComputer.RealView view = NestedLevelComputer.computeRealView(definitionLevels,
                        repetitionLevels, batch.valueCount, batch.recordCount, maxDefinitionLevel, layers);
                batch.realView = view;
                int[] map = view.realToRawLeaf();
                batch.realValues = map != null ? LeafCompaction.compact(batch.values, map) : batch.values;
            }
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

    /// The real-items view of an all-present batch: only the layer offsets, which without phantom
    /// positions equal the all-items offsets. Every validity is `null` (all present) and
    /// `realToRawLeaf` is `null` (the dense values are the real leaf values).
    private static NestedLevelComputer.RealView allPresentView(NestedBatch batch, int[] repetitionLevels,
                                                               NestedLevelComputer.Layers layers) {
        int[][] offsets = batch.fixedListK > 0
                ? NestedLevelComputer.fixedListLayerOffsets(batch.fixedListK, batch.recordCount, layers)
                : NestedLevelComputer.computeLayerOffsets(repetitionLevels, batch.valueCount, batch.recordCount,
                        layers);
        return new NestedLevelComputer.RealView(offsets, new long[layers.count()][], null, batch.valueCount, null);
    }
}
