/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.Arrays;

import dev.hardwood.reader.LayerKind;

/// The reference [NestedLevelComputer#computeRealView] is checked against: builds the real-items
/// view in one pass over the levels, visiting every layer at every position.
final class ReferenceRealView {

    private ReferenceRealView() {
    }

    static NestedLevelComputer.RealView computeRealView(int[] defLevels, int[] repLevels,
                                           int rawValueCount, int recordCount,
                                           int maxDefLevel, NestedLevelComputer.Layers layers) {
        int layerCount = layers.count();
        int[] itemDef = layers.itemDefThresholds();
        int[] itemRep = layers.itemRepThresholds();

        int[][] offsets = new int[layerCount][];
        for (int k = 0; k < layerCount; k++) {
            if (layers.kinds()[k] == LayerKind.REPEATED) {
                offsets[k] = new int[rawValueCount + 1];
            }
        }
        long[][] layerValidity = new long[layerCount][];
        boolean[] layerHasAbsent = new boolean[layerCount];
        int[] realCount = new int[layerCount + 1];

        boolean leafCompacts = layerCount > 0 && itemDef[layerCount] > 0;
        int[] realToRaw = leafCompacts ? new int[rawValueCount] : null;

        long[] leafValidity = null;
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
                            layerValidity[k] = new long[(rawValueCount + 63) >>> 6];
                            BitmapWords.setRange(layerValidity[k], 0, slot);
                        }
                        layerHasAbsent[k] = true;
                    }
                    else if (layerValidity[k] != null) {
                        layerValidity[k][slot >>> 6] |= 1L << slot;
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
                        leafValidity = new long[(rawValueCount + 63) >>> 6];
                        BitmapWords.setRange(leafValidity, 0, slot);
                    }
                    leafAnyAbsent = true;
                }
                else if (leafValidity != null) {
                    leafValidity[slot >>> 6] |= 1L << slot;
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
            else if (layerValidity[k] != null) {
                int n = realCount[k];
                int wordsNeeded = (n + 63) >>> 6;
                if (wordsNeeded < layerValidity[k].length) {
                    layerValidity[k] = Arrays.copyOf(layerValidity[k], wordsNeeded);
                }
            }
        }

        if (!leafAnyAbsent) {
            leafValidity = null;
        }
        else if (leafValidity != null) {
            int wordsNeeded = (realCount[layerCount] + 63) >>> 6;
            if (wordsNeeded < leafValidity.length) {
                leafValidity = Arrays.copyOf(leafValidity, wordsNeeded);
            }
        }
        int realLeafCount = realCount[layerCount];
        int[] trimmedRealToRaw = null;
        // `realToRaw` is sized to `rawValueCount`, so `realLeafCount == rawValueCount`
        // means every raw position is a real leaf: no phantom (null/empty parent)
        // positions, and the map is the identity `realToRaw[i] == i`. Leave it null —
        // the identical pass-through signal a REPEATED-free chain uses — so the
        // consumer reads the batch's (already per-batch fresh) value array directly
        // instead of allocating and gathering a compacted copy.
        if (realToRaw != null && realLeafCount != realToRaw.length) {
            trimmedRealToRaw = Arrays.copyOf(realToRaw, realLeafCount);
        }
        return new NestedLevelComputer.RealView(offsets, layerValidity, leafValidity, realLeafCount,
                trimmedRealToRaw);
    }
}
