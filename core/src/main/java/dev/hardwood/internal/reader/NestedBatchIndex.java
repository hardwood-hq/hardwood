/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.charset.StandardCharsets;
import java.util.BitSet;

import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.ProjectedSchema;

/// Pre-computed batch-level index for all projected columns.
///
/// Computed once per `setBatchData()` call. Holds multi-level offset arrays,
/// null bitmaps, and raw value arrays that enable flyweight cursors to
/// navigate directly over column data without per-row tree assembly.
final class NestedBatchIndex {

    final Object[] valueArrays;    // [projectedCol] -> typed value array (int[], long[], etc.)
    final int[][] defLevels;       // [projectedCol] -> definition levels
    final ColumnSchema[] columnSchemas; // [projectedCol] -> column schema
    final int[] valueCounts;       // [projectedCol] -> number of values
    final int[] recordCounts;      // [projectedCol] -> number of records
    final int[][] offsets;         // [projectedCol] -> record-level offsets
    final int[][][] multiOffsets;  // [projectedCol] -> int[][] multi-level offsets (for repeated cols)
    final BitSet[][] levelNulls;   // [projectedCol][level] -> null bitmap
    final BitSet[] elementNulls;   // [projectedCol] -> leaf null bitmap
    final ProjectedSchema projectedSchema;

    private NestedBatchIndex(Object[] valueArrays, int[][] defLevels,
                             ColumnSchema[] columnSchemas, int[] valueCounts,
                             int[] recordCounts, int[][] offsets,
                             int[][][] multiOffsets, BitSet[][] levelNulls,
                             BitSet[] elementNulls, ProjectedSchema projectedSchema) {
        this.valueArrays = valueArrays;
        this.defLevels = defLevels;
        this.columnSchemas = columnSchemas;
        this.valueCounts = valueCounts;
        this.recordCounts = recordCounts;
        this.offsets = offsets;
        this.multiOffsets = multiOffsets;
        this.levelNulls = levelNulls;
        this.elementNulls = elementNulls;
        this.projectedSchema = projectedSchema;
    }

    /// Build the batch index from [NestedBatch] objects whose index fields
    /// have been pre-computed by the drain thread.
    static NestedBatchIndex buildFromBatches(NestedBatch[] batches, ColumnSchema[] columnSchemas,
                                              FileSchema schema, ProjectedSchema projectedSchema,
                                              TopLevelFieldMap fieldMap) {
        int colCount = batches.length;
        Object[] valueArrays = new Object[colCount];
        int[][] defLevels = new int[colCount][];
        int[] valueCounts = new int[colCount];
        int[] recordCounts = new int[colCount];
        int[][] offsets = new int[colCount][];
        int[][][] multiOffsets = new int[colCount][][];
        BitSet[][] levelNulls = new BitSet[colCount][];
        BitSet[] elementNulls = new BitSet[colCount];

        for (int col = 0; col < colCount; col++) {
            NestedBatch batch = batches[col];
            valueArrays[col] = batch.values;
            defLevels[col] = batch.definitionLevels;
            valueCounts[col] = batch.valueCount;
            recordCounts[col] = batch.recordCount;
            offsets[col] = batch.recordOffsets;
            multiOffsets[col] = batch.multiLevelOffsets;
            levelNulls[col] = batch.levelNulls;
            elementNulls[col] = batch.elementNulls;
        }

        return new NestedBatchIndex(valueArrays, defLevels, columnSchemas,
                valueCounts, recordCounts, offsets, multiOffsets, levelNulls,
                elementNulls, projectedSchema);
    }

    // ==================== Value Access ====================

    /// Get the definition level at the given value index.
    int getDefLevel(int projectedCol, int valueIndex) {
        int[] dl = defLevels[projectedCol];
        return dl != null ? dl[valueIndex] : columnSchemas[projectedCol].maxDefinitionLevel();
    }

    /// Get the maximum repetition level for a column.
    int getMaxRepLevel(int projectedCol) {
        return columnSchemas[projectedCol].maxRepetitionLevel();
    }

    /// Get the boxed value at the given index (for generic access paths).
    Object getValue(int projectedCol, int valueIndex) {
        Object arr = valueArrays[projectedCol];
        return switch (arr) {
            case int[] a -> a[valueIndex];
            case long[] a -> a[valueIndex];
            case float[] a -> a[valueIndex];
            case double[] a -> a[valueIndex];
            case boolean[] a -> a[valueIndex];
            case byte[][] a -> a[valueIndex];
            default -> throw new IllegalStateException("Unexpected array type: " + arr.getClass());
        };
    }

    /// Get a string value at the given index.
    String getString(int projectedCol, int valueIndex) {
        byte[] raw = ((byte[][]) valueArrays[projectedCol])[valueIndex];
        return new String(raw, StandardCharsets.UTF_8);
    }

    // ==================== Index Navigation ====================

    /// Get the value index for a non-repeated column at the given record.
    int getValueIndex(int projectedCol, int recordIndex) {
        int[] recordOffsets = offsets[projectedCol];
        return recordOffsets != null ? recordOffsets[recordIndex] : recordIndex;
    }

    /// Get the start value index for a repeated column's list at the given record.
    int getListStart(int projectedCol, int recordIndex) {
        int[][] ml = multiOffsets[projectedCol];
        if (ml == null) {
            int[] recordOffsets = offsets[projectedCol];
            return recordOffsets != null ? recordOffsets[recordIndex] : recordIndex;
        }
        return ml[0][recordIndex];
    }

    /// Get the end index (exclusive) for a repeated column's list at the given record.
    int getListEnd(int projectedCol, int recordIndex) {
        int[][] ml = multiOffsets[projectedCol];
        if (ml == null) {
            int[] recordOffsets = offsets[projectedCol];
            if (recordOffsets == null) {
                return recordIndex + 1;
            }
            return (recordIndex + 1 < recordCounts[projectedCol])
                    ? recordOffsets[recordIndex + 1]
                    : valueCounts[projectedCol];
        }
        if (recordIndex + 1 < ml[0].length) {
            return ml[0][recordIndex + 1];
        }
        if (ml.length > 1) {
            return ml[1].length;
        }
        return valueCounts[projectedCol];
    }

    /// Get the start index at a given multi-level offset level.
    int getLevelStart(int projectedCol, int level, int itemIndex) {
        return multiOffsets[projectedCol][level][itemIndex];
    }

    /// Get the end index (exclusive) at a given multi-level offset level.
    int getLevelEnd(int projectedCol, int level, int itemIndex) {
        int[][] ml = multiOffsets[projectedCol];
        if (itemIndex + 1 < ml[level].length) {
            return ml[level][itemIndex + 1];
        }
        if (level + 1 < ml.length) {
            return ml[level + 1].length;
        }
        return valueCounts[projectedCol];
    }

    /// Check if a value at the given position is null at the leaf level.
    boolean isElementNull(int projectedCol, int valueIndex) {
        BitSet nulls = elementNulls[projectedCol];
        return nulls != null && nulls.get(valueIndex);
    }
}
