/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.BitSet;

import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.ProjectedSchema;

/**
 * Pre-computed batch-level index for all projected columns.
 * <p>
 * Computed once per {@code setBatchData()} call. Holds multi-level offset arrays
 * and null bitmaps that enable flyweight cursors to navigate directly over
 * {@link NestedColumnData} column arrays without per-row tree assembly.
 * </p>
 */
final class NestedBatchIndex {

    final NestedColumnData[] columns;
    final int[][] offsets;         // [projectedCol] -> record-level offsets (from NestedColumnData.recordOffsets)
    final int[][][] multiOffsets;  // [projectedCol] -> int[][] multi-level offsets (for repeated cols)
    final BitSet[][] levelNulls;   // [projectedCol][level] -> null bitmap
    final BitSet[] elementNulls;   // [projectedCol] -> leaf null bitmap
    final FileSchema schema;
    final ProjectedSchema projectedSchema;
    final TopLevelFieldMap fieldMap;

    private NestedBatchIndex(NestedColumnData[] columns, int[][] offsets,
                             int[][][] multiOffsets, BitSet[][] levelNulls,
                             BitSet[] elementNulls, FileSchema schema,
                             ProjectedSchema projectedSchema, TopLevelFieldMap fieldMap) {
        this.columns = columns;
        this.offsets = offsets;
        this.multiOffsets = multiOffsets;
        this.levelNulls = levelNulls;
        this.elementNulls = elementNulls;
        this.schema = schema;
        this.projectedSchema = projectedSchema;
        this.fieldMap = fieldMap;
    }

    /**
     * Build the batch index for the given columns.
     */
    static NestedBatchIndex build(NestedColumnData[] columns, FileSchema schema,
                                  ProjectedSchema projectedSchema, TopLevelFieldMap fieldMap) {
        int colCount = columns.length;
        int[][] offsets = new int[colCount][];
        int[][][] multiOffsets = new int[colCount][][];
        BitSet[][] levelNulls = new BitSet[colCount][];
        BitSet[] elementNulls = new BitSet[colCount];

        for (int col = 0; col < colCount; col++) {
            NestedColumnData data = columns[col];
            int maxRepLevel = data.column().maxRepetitionLevel();
            int maxDefLevel = data.maxDefinitionLevel();
            int[] repLevels = data.repetitionLevels();
            int[] defLevels = data.definitionLevels();
            int valueCount = data.valueCount();
            int recordCount = data.recordCount();

            // Store record offsets for direct access
            offsets[col] = data.recordOffsets();

            // Compute element nulls for all columns
            elementNulls[col] = NestedLevelComputer.computeElementNulls(defLevels, valueCount, maxDefLevel);

            // For repeated columns, compute multi-level offsets and level nulls
            if (maxRepLevel > 0 && repLevels != null && valueCount > 0) {
                multiOffsets[col] = NestedLevelComputer.computeMultiLevelOffsets(
                        repLevels, valueCount, recordCount, maxRepLevel);
                levelNulls[col] = NestedLevelComputer.computeLevelNulls(
                        defLevels, repLevels, valueCount, maxDefLevel, maxRepLevel);
            }
        }

        return new NestedBatchIndex(columns, offsets, multiOffsets, levelNulls,
                elementNulls, schema, projectedSchema, fieldMap);
    }

    /**
     * Get the value index for a non-repeated column at the given record.
     * For columns with maxRepLevel==0, the record offset is the value index.
     */
    int getValueIndex(int projectedCol, int recordIndex) {
        int[] recordOffsets = offsets[projectedCol];
        return recordOffsets != null ? recordOffsets[recordIndex] : recordIndex;
    }

    /**
     * Get the start value index for a repeated column's list at the given record.
     */
    int getListStart(int projectedCol, int recordIndex) {
        int[][]  ml = multiOffsets[projectedCol];
        if (ml == null) {
            // Fallback to record offsets
            int[] recordOffsets = offsets[projectedCol];
            return recordOffsets != null ? recordOffsets[recordIndex] : recordIndex;
        }
        return ml[0][recordIndex];
    }

    /**
     * Get the end index (exclusive) for a repeated column's list at the given record.
     * For maxRepLevel==1, returns a value index. For maxRepLevel>1, returns a level-1 item index.
     */
    int getListEnd(int projectedCol, int recordIndex) {
        int[][] ml = multiOffsets[projectedCol];
        if (ml == null) {
            int[] recordOffsets = offsets[projectedCol];
            if (recordOffsets == null) {
                return recordIndex + 1;
            }
            int recordCount = columns[projectedCol].recordCount();
            return (recordIndex + 1 < recordCount)
                    ? recordOffsets[recordIndex + 1]
                    : columns[projectedCol].valueCount();
        }
        if (recordIndex + 1 < ml[0].length) {
            return ml[0][recordIndex + 1];
        }
        // Last record: end is total items at the next level
        if (ml.length > 1) {
            return ml[1].length;
        }
        return columns[projectedCol].valueCount();
    }

    /**
     * Get the start index at a given multi-level offset level.
     * Level 1 maps level-1 items to values (or to level-2 items for deeper nesting).
     */
    int getLevelStart(int projectedCol, int level, int itemIndex) {
        int[][] ml = multiOffsets[projectedCol];
        return ml[level][itemIndex];
    }

    /**
     * Get the end index (exclusive) at a given multi-level offset level.
     */
    int getLevelEnd(int projectedCol, int level, int itemIndex) {
        int[][] ml = multiOffsets[projectedCol];
        if (itemIndex + 1 < ml[level].length) {
            return ml[level][itemIndex + 1];
        }
        // Last item at this level
        if (level + 1 < ml.length) {
            return ml[level + 1].length;
        }
        return columns[projectedCol].valueCount();
    }

    /**
     * Check if a value at the given position is null at the leaf level.
     */
    boolean isElementNull(int projectedCol, int valueIndex) {
        BitSet nulls = elementNulls[projectedCol];
        return nulls != null && nulls.get(valueIndex);
    }
}
