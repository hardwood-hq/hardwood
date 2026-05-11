/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.LayerKind;
import dev.hardwood.schema.SchemaNode;

/// Computes multi-level offsets and null bitmaps from Parquet repetition/definition levels.
///
/// These algorithms are used by both [dev.hardwood.reader.ColumnReader] for columnar
/// access and by [NestedBatchIndex] for flyweight row-level access.
public final class NestedLevelComputer {

    private NestedLevelComputer() {
    }

    /// Compute leaf-level validity bitmap with **set bit = present** polarity.
    ///
    /// Returns `null` when every leaf in the batch is present
    /// (sparse-all-present representation, mirroring the omitted-validity
    /// convention used at the API surface).
    public static BitSet computeElementValidity(int[] defLevels, int valueCount, int maxDefLevel) {
        if (defLevels == null || maxDefLevel == 0) {
            return null;
        }
        BitSet validity = null;
        for (int i = 0; i < valueCount; i++) {
            if (defLevels[i] < maxDefLevel) {
                if (validity == null) {
                    validity = new BitSet(valueCount);
                    validity.set(0, i);
                }
            }
            else if (validity != null) {
                validity.set(i);
            }
        }
        return validity;
    }

    /// Compute multi-level offsets with a trailing sentinel.
    ///
    /// Each `int[]` has length `count + 1`, where `offsets[count]` equals
    /// the count at the next inner rep level (or `valueCount` for the
    /// innermost). The output array is indexed by Parquet repetition level
    /// (one slot per `REPEATED` layer); [#computeLayerOffsets] wraps this
    /// routine to produce the layer-indexed shape consumed by
    /// [ColumnReader].
    public static int[][] computeMultiLevelOffsetsWithSentinel(int[] repLevels, int valueCount,
                                                                int recordCount, int maxRepLevel) {
        if (maxRepLevel == 0) {
            return new int[0][];
        }

        int[] itemCounts = new int[maxRepLevel];
        if (maxRepLevel == 1) {
            itemCounts[0] = recordCount;
        }
        else {
            for (int i = 0; i < valueCount; i++) {
                int rep = repLevels[i];
                for (int k = rep; k < maxRepLevel; k++) {
                    itemCounts[k]++;
                }
            }
        }

        int[][] offsets = new int[maxRepLevel][];
        for (int k = 0; k < maxRepLevel; k++) {
            offsets[k] = new int[itemCounts[k] + 1];
        }

        int[] itemIndices = new int[maxRepLevel];

        if (maxRepLevel == 1) {
            int recordIdx = 0;
            for (int i = 0; i < valueCount; i++) {
                if (repLevels[i] == 0) {
                    if (recordIdx < recordCount) {
                        offsets[0][recordIdx] = i;
                    }
                    recordIdx++;
                }
            }
        }
        else {
            for (int i = 0; i < valueCount; i++) {
                int rep = repLevels[i];
                for (int k = rep; k < maxRepLevel; k++) {
                    int idx = itemIndices[k];
                    if (k == maxRepLevel - 1) {
                        offsets[k][idx] = i;
                    }
                    else {
                        offsets[k][idx] = itemIndices[k + 1];
                    }
                    itemIndices[k]++;
                }
            }
        }

        // Trailing sentinels — for the innermost level it is `valueCount`,
        // for outer levels it is the count at the next inner level.
        for (int k = 0; k < maxRepLevel; k++) {
            if (k == maxRepLevel - 1) {
                offsets[k][itemCounts[k]] = valueCount;
            }
            else {
                offsets[k][itemCounts[k]] = itemCounts[k + 1];
            }
        }

        return offsets;
    }

    // ==================== Layer Model ====================

    /// Layer descriptor for a column's schema chain. The arrays are positional:
    /// `kinds[k]` and `defThresholds[k]` together describe layer `k`. Layers
    /// are numbered outermost-to-innermost.
    ///
    /// `itemDefThresholds` and `itemRepThresholds` are derived from `kinds`
    /// and `defThresholds` and cached on the descriptor:
    ///
    /// - `itemRepThresholds[k]` = number of `REPEATED` layers strictly
    ///   before `k`. A raw position with `rep[i] <= itemRepThresholds[k]`
    ///   represents a candidate item-start at layer `k`.
    /// - `itemDefThresholds[k]` = the minimum def-level at which a real item
    ///   slot exists at layer `k`. The recurrence: `itemDefThresholds[0] = 0`;
    ///   for `k > 0`, `itemDefThresholds[k] = itemDefThresholds[k-1]` if
    ///   `kinds[k-1] == STRUCT`, or `defThresholds[k-1] + 1` if
    ///   `kinds[k-1] == REPEATED` (the inner `repeated group` scaffolding
    ///   bumps def by one to reach the next layer's items).
    /// - `itemDefThresholds[count]` and `itemRepThresholds[count]` extend the
    ///   recurrence one step past the innermost layer to describe leaf-item
    ///   advances; `itemDefThresholds[count] == 0` for chains with no
    ///   `REPEATED` layer means "every position is a real leaf slot," i.e.
    ///   the leaf array is sized to `recordCount`.
    public record Layers(LayerKind[] kinds, int[] defThresholds,
                         int[] itemDefThresholds, int[] itemRepThresholds) {
        public int count() {
            return kinds.length;
        }

        public static Layers of(LayerKind[] kinds, int[] defThresholds) {
            int n = kinds.length;
            int[] itemDef = new int[n + 1];
            int[] itemRep = new int[n + 1];
            int repSeen = 0;
            for (int k = 0; k < n; k++) {
                itemRep[k] = repSeen;
                if (k == 0) {
                    itemDef[0] = 0;
                }
                else {
                    LayerKind parentKind = kinds[k - 1];
                    int parentDef = defThresholds[k - 1];
                    itemDef[k] = parentKind == LayerKind.STRUCT
                            ? itemDef[k - 1] : parentDef + 1;
                }
                if (kinds[k] == LayerKind.REPEATED) {
                    repSeen++;
                }
            }
            // Leaf entry (one step past the innermost layer)
            itemRep[n] = repSeen;
            if (n == 0) {
                itemDef[0] = 0;
            }
            else {
                LayerKind innermost = kinds[n - 1];
                int innermostDef = defThresholds[n - 1];
                itemDef[n] = innermost == LayerKind.STRUCT
                        ? itemDef[n - 1] : innermostDef + 1;
            }
            return new Layers(kinds, defThresholds, itemDef, itemRep);
        }
    }

    /// Compute per-layer offsets (one slot per layer). `STRUCT` layers
    /// have `null` entries (they carry no offsets); `REPEATED` layers carry
    /// the same sentinel-suffixed offsets [#computeMultiLevelOffsetsWithSentinel]
    /// produces, just remapped from rep-level indexing onto the layer
    /// position. The offsets retain raw-counting semantics — phantom
    /// positions for null/empty lists still occupy a slot in the
    /// next-layer index. [#computeRealView] performs the real-items-only
    /// compaction consumed at the [ColumnReader] surface.
    public static int[][] computeLayerOffsets(int[] repLevels, int valueCount,
                                              int recordCount, Layers layers) {
        int layerCount = layers.count();
        int[][] result = new int[layerCount][];
        if (layerCount == 0) {
            return result;
        }

        int repCount = 0;
        for (int k = 0; k < layerCount; k++) {
            if (layers.kinds()[k] == LayerKind.REPEATED) {
                repCount++;
            }
        }
        if (repCount == 0) {
            return result;
        }

        int[][] perRep = computeMultiLevelOffsetsWithSentinel(
                repLevels, valueCount, recordCount, repCount);

        int repIdx = 0;
        for (int k = 0; k < layerCount; k++) {
            if (layers.kinds()[k] == LayerKind.REPEATED) {
                result[k] = perRep[repIdx++];
            }
        }
        return result;
    }

    /// Compute the layer descriptor for the given leaf column by walking the
    /// schema chain from `root` to the leaf at `columnIndex`.
    ///
    /// Each `OPTIONAL` non-LIST/non-MAP group along the path contributes a
    /// [LayerKind#STRUCT] layer; each `LIST`/`MAP`-annotated group contributes
    /// a [LayerKind#REPEATED] layer. The synthetic inner `repeated group` of a
    /// LIST/MAP is folded into its parent's REPEATED layer and contributes no
    /// layer of its own. `REQUIRED` groups contribute no layer.
    ///
    /// The threshold for each layer is the def-level at-or-above which the
    /// node at that layer is **present**. Validity bit is then
    /// `defLevels[i] >= threshold`.
    public static Layers computeLayers(SchemaNode.GroupNode root, int columnIndex) {
        List<LayerKind> kinds = new ArrayList<>();
        List<Integer> thresholds = new ArrayList<>();
        walkLayersToLeaf(root, columnIndex, kinds, thresholds, false);
        LayerKind[] kindArr = kinds.toArray(new LayerKind[0]);
        int[] threshArr = new int[thresholds.size()];
        for (int i = 0; i < threshArr.length; i++) {
            threshArr[i] = thresholds.get(i);
        }
        return Layers.of(kindArr, threshArr);
    }

    private static boolean walkLayersToLeaf(SchemaNode node, int columnIndex,
                                            List<LayerKind> kinds, List<Integer> thresholds,
                                            boolean parentWasListMap) {
        return switch (node) {
            case SchemaNode.PrimitiveNode prim -> prim.columnIndex() == columnIndex;
            case SchemaNode.GroupNode group -> {
                boolean isListOrMap = group.isList() || group.isMap();
                LayerKind addedKind = null;

                if (isListOrMap) {
                    // A LIST/MAP-annotated node always contributes its own
                    // `REPEATED` layer, even when its parent was also a
                    // LIST/MAP. In the legacy 2-level encoding
                    // `group A (LIST) { repeated group B (LIST) { ... } }`,
                    // `B` is the element type (a nested list), not a
                    // synthetic wrapper of `A` — checking the annotation
                    // first is what distinguishes "element-is-itself-a-list"
                    // from "synthetic single-field element wrapper".
                    addedKind = LayerKind.REPEATED;
                    kinds.add(addedKind);
                    thresholds.add(group.maxDefinitionLevel());
                }
                else if (parentWasListMap) {
                    // Inside the LIST/MAP scaffolding — synthetic
                    // `repeated group` wrapper. Don't contribute; the outer
                    // LIST/MAP already added its REPEATED layer.
                }
                else if (group.repetitionType() == RepetitionType.OPTIONAL) {
                    addedKind = LayerKind.STRUCT;
                    kinds.add(addedKind);
                    thresholds.add(group.maxDefinitionLevel());
                }
                else if (group.repetitionType() == RepetitionType.REPEATED) {
                    // Top-level repeated outside of a LIST/MAP annotation (legacy /
                    // unannotated repeated field). Treat as REPEATED layer.
                    addedKind = LayerKind.REPEATED;
                    kinds.add(addedKind);
                    thresholds.add(group.maxDefinitionLevel());
                }
                // REQUIRED: no layer

                boolean found = false;
                for (SchemaNode child : group.children()) {
                    if (walkLayersToLeaf(child, columnIndex, kinds, thresholds, isListOrMap)) {
                        found = true;
                        break;
                    }
                }

                if (!found && addedKind != null) {
                    kinds.remove(kinds.size() - 1);
                    thresholds.remove(thresholds.size() - 1);
                }

                yield found;
            }
        };
    }

    /// Real-items-only view of a raw-counting batch.
    ///
    /// `layerOffsets[k]` is sentinel-suffixed and `null` for `STRUCT` layers.
    /// `layerValidity[k]` carries set-bit-= -present polarity over the layer's
    /// real-items index; `null` means every item at that layer is present in
    /// this batch.
    ///
    /// `leafValidity` is over `valueCount` real leaf positions; `null` means
    /// every real leaf is present.
    ///
    /// `realToRawLeaf` maps each real-leaf index to the raw position in the
    /// batch's value array (so `compactInts(rawValues, realToRawLeaf)` etc.
    /// produces real-items-only typed arrays). `null` means the leaf is in
    /// 1-to-1 correspondence with the raw value stream — i.e. the chain has
    /// no `REPEATED` layers — and the raw values array can pass through
    /// without compaction.
    public record RealView(int[][] layerOffsets, BitSet[] layerValidity,
                           BitSet leafValidity, int valueCount,
                           int[] realToRawLeaf) {}

    /// Builds the real-items-only view from raw def/rep arrays. Phantom
    /// positions (those representing null/empty parents at any `REPEATED`
    /// layer) are skipped from layer offsets and from the leaf-items axis;
    /// at `STRUCT` layers slots stay 1-to-1 with the parent (the design's
    /// "STRUCT does not expand or contract the item stream" rule).
    public static RealView computeRealView(int[] defLevels, int[] repLevels,
                                           int rawValueCount, int recordCount,
                                           int maxDefLevel, Layers layers) {
        int layerCount = layers.count();
        int[] itemDef = layers.itemDefThresholds();
        int[] itemRep = layers.itemRepThresholds();

        int[][] offsets = new int[layerCount][];
        for (int k = 0; k < layerCount; k++) {
            if (layers.kinds()[k] == LayerKind.REPEATED) {
                offsets[k] = new int[rawValueCount + 1];
            }
        }
        BitSet[] layerValidity = new BitSet[layerCount];
        boolean[] layerHasAbsent = new boolean[layerCount];
        int[] realCount = new int[layerCount + 1];

        boolean leafCompacts = layerCount > 0 && itemDef[layerCount] > 0;
        int[] realToRaw = leafCompacts ? new int[rawValueCount] : null;

        BitSet leafValidity = null;
        boolean leafAnyAbsent = false;

        for (int i = 0; i < rawValueCount; i++) {
            int rep = repLevels != null ? repLevels[i] : 0;
            int def = defLevels != null ? defLevels[i] : maxDefLevel;

            for (int k = 0; k < layerCount; k++) {
                if (rep <= itemRep[k] && def >= itemDef[k]) {
                    int slot = realCount[k];
                    if (layers.kinds()[k] == LayerKind.REPEATED) {
                        offsets[k][slot] = realCount[k + 1];
                    }
                    boolean present = def >= layers.defThresholds()[k];
                    if (!present) {
                        if (layerValidity[k] == null) {
                            layerValidity[k] = new BitSet();
                            layerValidity[k].set(0, slot);
                        }
                        layerHasAbsent[k] = true;
                    }
                    else if (layerValidity[k] != null) {
                        layerValidity[k].set(slot);
                    }
                    realCount[k]++;
                }
            }

            // Leaf advance: a real leaf slot exists at this position iff every
            // enclosing layer's content is reached. The rule unifies the
            // STRUCT-only chain (always advance — leaf is sized to recordCount)
            // and the REPEATED-bearing chain (advance only at content-reached
            // positions — leaf is compacted).
            if (rep <= itemRep[layerCount] && def >= itemDef[layerCount]) {
                int slot = realCount[layerCount];
                if (realToRaw != null) {
                    realToRaw[slot] = i;
                }
                boolean leafPresent = def >= maxDefLevel;
                if (!leafPresent) {
                    if (leafValidity == null) {
                        leafValidity = new BitSet();
                        leafValidity.set(0, slot);
                    }
                    leafAnyAbsent = true;
                }
                else if (leafValidity != null) {
                    leafValidity.set(slot);
                }
                realCount[layerCount]++;
            }
        }

        for (int k = 0; k < layerCount; k++) {
            if (layers.kinds()[k] == LayerKind.REPEATED) {
                int n = realCount[k];
                int[] trimmed = Arrays.copyOf(offsets[k], n + 1);
                trimmed[n] = realCount[k + 1];
                offsets[k] = trimmed;
            }
            if (!layerHasAbsent[k]) {
                layerValidity[k] = null;
            }
        }

        if (!leafAnyAbsent) {
            leafValidity = null;
        }
        int realLeafCount = realCount[layerCount];
        int[] trimmedRealToRaw = null;
        if (realToRaw != null) {
            trimmedRealToRaw = realLeafCount == realToRaw.length
                    ? realToRaw : Arrays.copyOf(realToRaw, realLeafCount);
        }
        return new RealView(offsets, layerValidity, leafValidity, realLeafCount,
                trimmedRealToRaw);
    }
}
