/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.BitSet;

/**
 * Computes multi-level offsets and null bitmaps from Parquet repetition/definition levels.
 * <p>
 * These algorithms are used by both {@link dev.hardwood.reader.ColumnReader} for columnar
 * access and by {@link NestedBatchIndex} for flyweight row-level access.
 * </p>
 */
public final class NestedLevelComputer {

    private NestedLevelComputer() {
    }

    /**
     * Compute multi-level offset arrays from repetition levels.
     * <p>
     * For maxRepLevel=1 (simple list): one offset array mapping records to value positions.
     * For maxRepLevel=N (nested list): N offset arrays, chained.
     * Level k boundary: positions where repLevel[i] &lt;= k.
     */
    public static int[][] computeMultiLevelOffsets(int[] repLevels, int valueCount,
                                                   int recordCount, int maxRepLevel) {
        if (maxRepLevel == 1) {
            int[] offsets = new int[recordCount];
            int recordIdx = 0;
            for (int i = 0; i < valueCount; i++) {
                if (repLevels[i] == 0) {
                    if (recordIdx < recordCount) {
                        offsets[recordIdx] = i;
                    }
                    recordIdx++;
                }
            }
            return new int[][] { offsets };
        }

        // General case: multi-level offsets
        int[] itemCounts = new int[maxRepLevel];
        for (int i = 0; i < valueCount; i++) {
            int rep = repLevels[i];
            for (int k = rep; k < maxRepLevel; k++) {
                itemCounts[k]++;
            }
        }

        int[][] offsets = new int[maxRepLevel][];
        for (int k = 0; k < maxRepLevel; k++) {
            offsets[k] = new int[itemCounts[k]];
        }

        int[] itemIndices = new int[maxRepLevel];

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

        return offsets;
    }

    /**
     * Compute per-level null bitmaps from definition and repetition levels.
     *
     * @return array of BitSets, one per nesting level; null entries mean all-non-null at that level
     */
    public static BitSet[] computeLevelNulls(int[] defLevels, int[] repLevels,
                                             int valueCount, int maxDefLevel, int maxRepLevel) {
        BitSet[] levelNulls = new BitSet[maxRepLevel];

        for (int k = 0; k < maxRepLevel; k++) {
            int defThreshold = maxDefLevel - maxRepLevel + k + 1;
            BitSet nullBits = null;

            int itemIdx = 0;
            for (int i = 0; i < valueCount; i++) {
                if (repLevels[i] <= k) {
                    if (defLevels[i] < defThreshold) {
                        if (nullBits == null) {
                            nullBits = new BitSet();
                        }
                        nullBits.set(itemIdx);
                    }
                    itemIdx++;
                }
            }

            levelNulls[k] = nullBits;
        }

        return levelNulls;
    }

    /**
     * Compute leaf-level null bitmap.
     *
     * @return BitSet where set bits indicate null values, or null if all elements are required
     */
    public static BitSet computeElementNulls(int[] defLevels, int valueCount, int maxDefLevel) {
        if (defLevels == null || maxDefLevel == 0) {
            return null;
        }
        BitSet nulls = null;
        for (int i = 0; i < valueCount; i++) {
            if (defLevels[i] < maxDefLevel) {
                if (nulls == null) {
                    nulls = new BitSet(valueCount);
                }
                nulls.set(i);
            }
        }
        return nulls;
    }
}
