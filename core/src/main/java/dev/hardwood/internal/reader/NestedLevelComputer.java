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
import java.util.List;

import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.LayerKind;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;

/// Computes multi-level offsets and null bitmaps from Parquet repetition/definition levels.
///
/// These algorithms are used by both [dev.hardwood.reader.ColumnReader] for columnar
/// access and by [NestedBatchIndex] for flyweight row-level access.
public final class NestedLevelComputer {

    /// The layers of a column with no `STRUCT` or `REPEATED` layer, shared by every such column.
    private static final Layers NO_LAYERS = Layers.of(new LayerKind[0], new int[0]);

    private NestedLevelComputer() {
    }

    /// Compute leaf-level validity bitmap with **set bit = present** polarity.
    ///
    /// Returns `null` when every leaf in the batch is present
    /// (sparse-all-present representation, mirroring the omitted-validity
    /// convention used at the API surface). When non-null, the array has
    /// length `(valueCount + 63) >>> 6` words.
    public static long[] computeElementValidity(int[] defLevels, int valueCount, int maxDefLevel) {
        if (defLevels == null || maxDefLevel == 0) {
            return null;
        }
        long[] validity = null;
        for (int i = 0; i < valueCount; i++) {
            if (defLevels[i] < maxDefLevel) {
                if (validity == null) {
                    validity = new long[(valueCount + 63) >>> 6];
                    BitmapWords.setRange(validity, 0, i);
                }
            }
            else if (validity != null) {
                validity[i >>> 6] |= 1L << i;
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

    /// Sentinel-suffixed record offsets for a fixed-width fixed-size-list batch, where
    /// record `r` spans values `[r * k, (r + 1) * k)`: the array
    /// `[0, k, 2k, ..., recordCount * k]`. Used by the fixed-size-list fast path
    /// in place of a repetition-level scan.
    public static int[] fixedListOffsets(int k, int recordCount) {
        int[] offsets = new int[recordCount + 1];
        for (int i = 0; i <= recordCount; i++) {
            offsets[i] = Math.multiplyExact(i, k);
        }
        return offsets;
    }

    /// Layer-indexed offsets for a fixed-width fixed-size-list batch: each `REPEATED`
    /// layer gets the arithmetic sentinel-suffixed offsets `[0, k, 2k, ...,
    /// recordCount * k]` from [#fixedListOffsets]; `STRUCT` layers stay `null`.
    /// Returns `null` when there are no layers. The arithmetic replacement for
    /// [#computeLayerOffsets] on the fast path, where record boundaries are
    /// implicit multiples of `k` rather than recovered from a repetition scan.
    public static int[][] fixedListLayerOffsets(int k, int recordCount, Layers layers) {
        if (layers == null || layers.count() == 0) {
            return null;
        }
        int layerCount = layers.count();
        int[][] result = new int[layerCount][];
        for (int i = 0; i < layerCount; i++) {
            if (layers.kinds()[i] == LayerKind.REPEATED) {
                result[i] = fixedListOffsets(k, recordCount);
            }
        }
        return result;
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

    /// Compute the layer descriptor of every leaf column of `schema` in one walk of the
    /// schema tree, indexed by column index.
    ///
    /// Each `OPTIONAL` non-LIST/non-MAP group along a leaf's path contributes a
    /// [LayerKind#STRUCT] layer; each `LIST`/`MAP`-annotated group contributes
    /// a [LayerKind#REPEATED] layer. The synthetic inner `repeated group` of a
    /// LIST/MAP is folded into its parent's REPEATED layer and contributes no
    /// layer of its own. `REQUIRED` groups contribute no layer.
    ///
    /// The threshold for each layer is the def-level at-or-above which the
    /// node at that layer is **present**. Validity bit is then
    /// `defLevels[i] >= threshold`.
    public static Layers[] computeLayers(FileSchema schema) {
        Layers[] result = new Layers[schema.getColumnCount()];
        walkLayers(schema.getRootNode(), new ArrayList<>(), new ArrayList<>(), false, result);
        return result;
    }

    private static void walkLayers(SchemaNode node, List<LayerKind> kinds, List<Integer> thresholds,
                                   boolean parentWasListMap, Layers[] result) {
        switch (node) {
            case SchemaNode.PrimitiveNode prim -> {
                // A repeated primitive that is not the suppressed element of a
                // LIST/MAP wrapper contributes its own `REPEATED` layer: the bare
                // unannotated repeated field, including the inner repeated element
                // of a legacy 2-level list-of-lists. When it IS the wrapper's
                // element (`parentWasListMap`), the enclosing LIST/MAP already
                // accounts for that repetition level. The threshold is the
                // container's def level (one below the leaf's), so the leaf items
                // appear at `threshold + 1 == prim.maxDefinitionLevel()`.
                boolean ownLayer = !parentWasListMap && prim.repetitionType() == RepetitionType.REPEATED;
                if (ownLayer) {
                    kinds.add(LayerKind.REPEATED);
                    thresholds.add(prim.maxDefinitionLevel() - 1);
                }
                result[prim.columnIndex()] = toLayers(kinds, thresholds);
                if (ownLayer) {
                    removeLast(kinds, thresholds, 1);
                }
            }
            case SchemaNode.GroupNode group -> {
                boolean isListOrMap = group.isList() || group.isMap();
                int added = 0;

                if (!parentWasListMap && group.repetitionType() == RepetitionType.REPEATED) {
                    // Repeated group outside LIST/MAP scaffolding, at any depth: a list
                    // of its own elements. As for the repeated primitive, the threshold
                    // is the container's def level, one below the group's; the group's
                    // own level marks an element. A group that also reads as a LIST/MAP
                    // (a legacy MAP inferred from a sole MAP_KEY_VALUE child) is that
                    // list's element and adds its own layer below.
                    kinds.add(LayerKind.REPEATED);
                    thresholds.add(group.maxDefinitionLevel() - 1);
                    added++;
                }
                if (isListOrMap) {
                    // A LIST/MAP-annotated node always contributes its own
                    // `REPEATED` layer, even when its parent was also a
                    // LIST/MAP. In the legacy 2-level encoding
                    // `group A (LIST) { repeated group B (LIST) { ... } }`,
                    // `B` is the element type (a nested list), not a
                    // synthetic wrapper of `A` — checking the annotation
                    // first is what distinguishes "element-is-itself-a-list"
                    // from "synthetic single-field element wrapper".
                    kinds.add(LayerKind.REPEATED);
                    thresholds.add(group.maxDefinitionLevel());
                    added++;
                }
                else if (!parentWasListMap && group.repetitionType() == RepetitionType.OPTIONAL) {
                    kinds.add(LayerKind.STRUCT);
                    thresholds.add(group.maxDefinitionLevel());
                    added++;
                }
                // REQUIRED, and the synthetic `repeated group` inside LIST/MAP
                // scaffolding: no layer; the outer LIST/MAP already added its own.

                for (SchemaNode child : group.children()) {
                    walkLayers(child, kinds, thresholds, isListOrMap, result);
                }
                removeLast(kinds, thresholds, added);
            }
        }
    }

    private static Layers toLayers(List<LayerKind> kinds, List<Integer> thresholds) {
        if (kinds.isEmpty()) {
            return NO_LAYERS;
        }
        int[] threshArr = new int[thresholds.size()];
        for (int i = 0; i < threshArr.length; i++) {
            threshArr[i] = thresholds.get(i);
        }
        return Layers.of(kinds.toArray(new LayerKind[0]), threshArr);
    }

    private static void removeLast(List<LayerKind> kinds, List<Integer> thresholds, int count) {
        for (int k = 0; k < count; k++) {
            kinds.removeLast();
            thresholds.removeLast();
        }
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
    /// 1-to-1 correspondence with the raw value stream — either the chain has
    /// no `REPEATED` layers, or this batch has no phantom (null/empty parent)
    /// positions, so the map would be the identity — and the raw values array
    /// can pass through without compaction.
    public record RealView(int[][] layerOffsets, long[][] layerValidity,
                           long[] leafValidity, int valueCount,
                           int[] realToRawLeaf) {}

    /// Builds the real-items-only view from raw def/rep arrays. Phantom
    /// positions (those representing null/empty parents at any `REPEATED`
    /// layer) are skipped from layer offsets and from the leaf-items axis;
    /// at `STRUCT` layers slots stay 1-to-1 with the parent (the design's
    /// "STRUCT does not expand or contract the item stream" rule).
    ///
    /// A position is an item of layer `k` when `rep <= itemRepThresholds[k]` and
    /// `def >= itemDefThresholds[k]`. Each layer, and the leaf, takes a counting pass, which sizes
    /// its arrays exactly and finds whether any of its items is absent, then a filling pass that
    /// stores unconditionally at the next free slot and advances it by the item predicate, so a
    /// position that is no item writes a slot the next item overwrites. No pass branches on the
    /// levels.
    public static RealView computeRealView(int[] defLevels, int[] repLevels,
                                           int rawValueCount, int recordCount,
                                           int maxDefLevel, Layers layers) {
        return repLevels == null
                ? repetitionFreeView(defLevels, rawValueCount, maxDefLevel, layers)
                : repeatedView(defLevels, repLevels, rawValueCount, maxDefLevel, layers);
    }

    /// The view of a column with repetition levels, built layer by layer as
    /// [#computeRealView] describes.
    private static RealView repeatedView(int[] defLevels, int[] reps, int rawValueCount, int maxDefLevel,
                                         Layers layers) {
        int layerCount = layers.count();
        int[] itemDef = layers.itemDefThresholds();
        int[] itemRep = layers.itemRepThresholds();
        int[] defThresholds = layers.defThresholds();
        // Only a batch derived by record selection whose levels are all at the maximum comes
        // without definition levels next to repetition levels.
        int[] defs = defLevels != null ? defLevels : constantLevels(rawValueCount, maxDefLevel);

        int[][] offsets = new int[layerCount][];
        long[][] layerValidity = new long[layerCount][];
        for (int k = 0; k < layerCount; k++) {
            if (layers.kinds()[k] == LayerKind.REPEATED) {
                offsets[k] = repeatedLayer(defs, reps, rawValueCount, itemRep[k], itemDef[k],
                        itemRep[k + 1], itemDef[k + 1], defThresholds[k], layerValidity, k);
            }
            else {
                layerValidity[k] = structLayerValidity(defs, reps, rawValueCount, itemRep[k], itemDef[k],
                        defThresholds[k]);
            }
        }

        int leafRep = itemRep[layerCount];
        int leafDef = itemDef[layerCount];
        int leaves = 0;
        int absentLeaves = 0;
        for (int i = 0; i < rawValueCount; i++) {
            int def = defs[i];
            int item = reached(reps[i], leafRep, def, leafDef);
            leaves += item;
            absentLeaves += item & (atLeast(def, maxDefLevel) ^ 1);
        }
        // `leaves == rawValueCount` means every raw position is a real leaf: no phantom
        // (null/empty parent) positions, and the map would be the identity. It stays null, the
        // pass-through signal a REPEATED-free chain uses, so the consumer reads the batch's
        // (already per-batch fresh) value array directly instead of gathering a copy.
        int[] realToRaw = leaves != rawValueCount ? new int[leaves] : null;
        long[] leafValidity = absentLeaves != 0 ? new long[(leaves + 63) >>> 6] : null;
        if (realToRaw != null || leafValidity != null) {
            leafPass(defs, reps, leaves, leafRep, leafDef, maxDefLevel, realToRaw, leafValidity);
        }
        return new RealView(offsets, layerValidity, leafValidity, leaves, realToRaw);
    }

    /// The view of a column without repetition levels, whose layers are all `STRUCT` layers: every
    /// position is an item of every layer and a leaf, so there are no offsets and no real-to-raw
    /// map, and each validity marks the positions whose definition level reaches the layer's
    /// threshold. `null` definition levels are all at the maximum.
    private static RealView repetitionFreeView(int[] defs, int n, int maxDefLevel, Layers layers) {
        int layerCount = layers.count();
        int[] defThresholds = layers.defThresholds();
        long[][] layerValidity = new long[layerCount][];
        for (int k = 0; k < layerCount; k++) {
            if (layers.kinds()[k] != LayerKind.STRUCT) {
                throw new IllegalStateException("Layer " + k + " is " + layers.kinds()[k]
                        + " but the batch has no repetition levels");
            }
            layerValidity[k] = defs != null ? reaching(defs, n, defThresholds[k]) : null;
        }
        long[] leafValidity = defs != null ? reaching(defs, n, maxDefLevel) : null;
        return new RealView(new int[layerCount][], layerValidity, leafValidity, n, null);
    }

    /// The validity over all `n` positions marking those whose definition level is at least
    /// `threshold`, `null` when every position reaches it.
    private static long[] reaching(int[] defs, int n, int threshold) {
        int absent = 0;
        for (int i = 0; i < n; i++) {
            absent += atLeast(defs[i], threshold) ^ 1;
        }
        if (absent == 0) {
            return null;
        }
        long[] validity = new long[(n + 63) >>> 6];
        for (int w = 0; w < validity.length; w++) {
            int base = w << 6;
            int end = Math.min(64, n - base);
            long word = 0;
            for (int b = 0; b < end; b++) {
                word |= (long) atLeast(defs[base + b], threshold) << b;
            }
            validity[w] = word;
        }
        return validity;
    }

    /// Fills the real-to-raw map of the `leaves` leaves and their validity, either of which may be
    /// `null`. The pass ends at the last leaf, so every store is in bounds of arrays sized exactly.
    private static void leafPass(int[] defs, int[] reps, int leaves, int leafRep, int leafDef, int maxDefLevel,
                                 int[] realToRaw, long[] validity) {
        int slot = 0;
        long word = 0;
        for (int i = 0; slot < leaves; i++) {
            int def = defs[i];
            int item = reached(reps[i], leafRep, def, leafDef);
            if (realToRaw != null) {
                realToRaw[slot] = i;
            }
            if (validity != null) {
                word |= (long) (item & atLeast(def, maxDefLevel)) << slot;
                if ((((slot + item) & 63) | (item ^ 1)) == 0) {
                    validity[slot >>> 6] = word;
                    word = 0;
                }
            }
            slot += item;
        }
        if (validity != null && (slot & 63) != 0) {
            validity[slot >>> 6] = word;
        }
    }

    /// The sentinel-suffixed offsets of `REPEATED` layer `k` over its items, each item's offset
    /// being the number of items of the next layer (or leaf) before it; stores the layer's
    /// validity into `layerValidity[k]`, `null` when every item is present.
    private static int[] repeatedLayer(int[] defs, int[] reps, int n, int itemRep, int itemDef,
                                       int nextItemRep, int nextItemDef, int defThreshold,
                                       long[][] layerValidity, int k) {
        // A counting pass sizes the offsets exactly and finds whether any item is absent; it
        // carries no state but its sums, so the compiler can vectorize it.
        int items = 0;
        int absentItems = 0;
        for (int i = 0; i < n; i++) {
            int def = defs[i];
            int item = reached(reps[i], itemRep, def, itemDef);
            items += item;
            absentItems += item & (atLeast(def, defThreshold) ^ 1);
        }
        int[] offsets = new int[items + 1];
        int slot = 0;
        int next = 0;
        if (absentItems == 0) {
            for (int i = 0; i < n; i++) {
                int rep = reps[i];
                int def = defs[i];
                int item = reached(rep, itemRep, def, itemDef);
                offsets[slot] = next;
                slot += item;
                next += reached(rep, nextItemRep, def, nextItemDef);
            }
            layerValidity[k] = null;
            offsets[items] = next;
            return offsets;
        }
        long[] validity = new long[(items + 63) >>> 6];
        long word = 0;
        for (int i = 0; i < n; i++) {
            int rep = reps[i];
            int def = defs[i];
            int item = reached(rep, itemRep, def, itemDef);
            offsets[slot] = next;
            word |= (long) (item & atLeast(def, defThreshold)) << slot;
            int nextSlot = slot + item;
            if (((nextSlot & 63) | (item ^ 1)) == 0) {
                validity[slot >>> 6] = word;
                word = 0;
            }
            slot = nextSlot;
            next += reached(rep, nextItemRep, def, nextItemDef);
        }
        if ((slot & 63) != 0) {
            validity[slot >>> 6] = word;
        }
        layerValidity[k] = validity;
        offsets[items] = next;
        return offsets;
    }

    /// The validity of a `STRUCT` layer over its items, `null` when every item is present. A
    /// counting pass, which the compiler can vectorize, finds whether any item is absent and how
    /// many items there are; most batches of most columns have no absent item.
    private static long[] structLayerValidity(int[] defs, int[] reps, int n, int itemRep, int itemDef,
                                              int defThreshold) {
        int items = 0;
        int absentItems = 0;
        for (int i = 0; i < n; i++) {
            int def = defs[i];
            int item = reached(reps[i], itemRep, def, itemDef);
            items += item;
            absentItems += item & (atLeast(def, defThreshold) ^ 1);
        }
        if (absentItems == 0) {
            return null;
        }
        long[] validity = new long[(items + 63) >>> 6];
        int slot = 0;
        long word = 0;
        for (int i = 0; slot < items; i++) {
            int def = defs[i];
            int item = reached(reps[i], itemRep, def, itemDef);
            word |= (long) (item & atLeast(def, defThreshold)) << slot;
            if ((((slot + item) & 63) | (item ^ 1)) == 0) {
                validity[slot >>> 6] = word;
                word = 0;
            }
            slot += item;
        }
        if ((slot & 63) != 0) {
            validity[slot >>> 6] = word;
        }
        return validity;
    }

    /// `1` when a position with levels `rep` and `def` is an item of the layer with thresholds
    /// `itemRep` and `itemDef` (`rep <= itemRep` and `def >= itemDef`), else `0`: computed from sign
    /// bits rather than compared, so no loop over the levels branches on them. Levels are small and
    /// non-negative, so neither difference overflows.
    private static int reached(int rep, int itemRep, int def, int itemDef) {
        return ((itemRep - rep) | (def - itemDef)) >>> 31 ^ 1;
    }

    /// `1` when `def >= threshold`, else `0`, from the sign bit as [#reached] is.
    private static int atLeast(int def, int threshold) {
        return (def - threshold) >>> 31 ^ 1;
    }

    private static int[] constantLevels(int n, int level) {
        int[] levels = new int[n];
        if (level != 0) {
            Arrays.fill(levels, level);
        }
        return levels;
    }
}
