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
 * Wraps a {@link NestedColumnData} with pre-computed index data.
 * <p>
 * Index computation (multi-level offsets, null bitmaps) is fused into the
 * column read future so it runs in parallel with other columns rather than
 * sequentially on the consumer thread.
 * </p>
 */
public record IndexedNestedColumnData(
        NestedColumnData data,
        int[] recordOffsets,
        int[][] multiLevelOffsets,
        BitSet[] levelNulls,
        BitSet elementNulls
) {

    /**
     * Compute index data for a nested column and wrap it.
     *
     * @param data                the nested column data
     * @param levelNullThresholds per-level definition level thresholds from
     *                            {@link NestedLevelComputer#computeLevelNullThresholds}
     */
    public static IndexedNestedColumnData compute(NestedColumnData data, int[] levelNullThresholds) {
        int maxRepLevel = data.column().maxRepetitionLevel();
        int maxDefLevel = data.maxDefinitionLevel();
        int[] repLevels = data.repetitionLevels();
        int[] defLevels = data.definitionLevels();
        int valueCount = data.valueCount();
        int recordCount = data.recordCount();

        int[] recordOffsets = data.recordOffsets();
        BitSet elementNulls = NestedLevelComputer.computeElementNulls(defLevels, valueCount, maxDefLevel);

        int[][] multiLevelOffsets = null;
        BitSet[] levelNulls = null;
        if (maxRepLevel > 0 && repLevels != null && valueCount > 0) {
            multiLevelOffsets = NestedLevelComputer.computeMultiLevelOffsets(
                    repLevels, valueCount, recordCount, maxRepLevel);
            levelNulls = NestedLevelComputer.computeLevelNulls(
                    defLevels, repLevels, valueCount, maxRepLevel, levelNullThresholds);
        }

        return new IndexedNestedColumnData(data, recordOffsets, multiLevelOffsets, levelNulls, elementNulls);
    }
}
