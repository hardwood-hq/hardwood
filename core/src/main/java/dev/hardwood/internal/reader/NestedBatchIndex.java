/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

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
    /// `[projectedCol] -> int[repCount][]`, **rep-level-indexed** offsets
    /// compacted from the layer-indexed [NestedBatch#multiLevelOffsets]
    /// produced by the worker. `STRUCT`-layer slots (`null`) are dropped so
    /// the remaining `REPEATED`-layer offsets are addressable by the
    /// 0-indexed rep level used by internal consumers
    /// ([PqListImpl] / [PqMapImpl] / [PqStructImpl]). Each per-rep-level
    /// `int[]` is sentinel-suffixed (length `count + 1`).
    final int[][][] multiOffsets;
    final BitSet[] elementValidity; // [projectedCol] -> leaf validity bitmap (set bit = present)
    final ProjectedSchema projectedSchema;

    private NestedBatchIndex(Object[] valueArrays, int[][] defLevels,
                             ColumnSchema[] columnSchemas, int[] valueCounts,
                             int[] recordCounts, int[][] offsets,
                             int[][][] multiOffsets,
                             BitSet[] elementValidity, ProjectedSchema projectedSchema) {
        this.valueArrays = valueArrays;
        this.defLevels = defLevels;
        this.columnSchemas = columnSchemas;
        this.valueCounts = valueCounts;
        this.recordCounts = recordCounts;
        this.offsets = offsets;
        this.multiOffsets = multiOffsets;
        this.elementValidity = elementValidity;
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
        BitSet[] elementValidity = new BitSet[colCount];

        for (int col = 0; col < colCount; col++) {
            NestedBatch batch = batches[col];
            valueArrays[col] = batch.values;
            defLevels[col] = batch.definitionLevels;
            valueCounts[col] = batch.valueCount;
            recordCounts[col] = batch.recordCount;
            offsets[col] = batch.recordOffsets;
            multiOffsets[col] = compactToRepLevelOffsets(batch.multiLevelOffsets);
            elementValidity[col] = batch.elementValidity;
        }

        return new NestedBatchIndex(valueArrays, defLevels, columnSchemas,
                valueCounts, recordCounts, offsets, multiOffsets,
                elementValidity, projectedSchema);
    }

    /// Compact a layer-indexed offsets array (length `layerCount`, with
    /// `null` at `STRUCT`-layer positions) into a rep-level-indexed array
    /// (length `repCount`) by dropping the `null` slots while preserving
    /// order. `null` input passes through unchanged.
    private static int[][] compactToRepLevelOffsets(int[][] layerIndexed) {
        if (layerIndexed == null) {
            return null;
        }
        int repCount = 0;
        for (int[] o : layerIndexed) {
            if (o != null) {
                repCount++;
            }
        }
        if (repCount == layerIndexed.length) {
            return layerIndexed;
        }
        int[][] compact = new int[repCount][];
        int j = 0;
        for (int[] o : layerIndexed) {
            if (o != null) {
                compact[j++] = o;
            }
        }
        return compact;
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
    /// For byte-array physical types this materialises a fresh `byte[]`
    /// copy out of [BinaryBatchValues].
    Object getValue(int projectedCol, int valueIndex) {
        Object arr = valueArrays[projectedCol];
        return switch (arr) {
            case int[] a -> a[valueIndex];
            case long[] a -> a[valueIndex];
            case float[] a -> a[valueIndex];
            case double[] a -> a[valueIndex];
            case boolean[] a -> a[valueIndex];
            case BinaryBatchValues bbv -> bbv.byteArrayAt(valueIndex);
            default -> throw new IllegalStateException("Unexpected array type: " + arr.getClass());
        };
    }

    /// Get a fresh byte[] copy of value `valueIndex` for a varlength column.
    byte[] getBinary(int projectedCol, int valueIndex) {
        return ((BinaryBatchValues) valueArrays[projectedCol]).byteArrayAt(valueIndex);
    }

    /// Get a UTF-8 decoded string for value `valueIndex` of a varlength column.
    String getString(int projectedCol, int valueIndex) {
        return ((BinaryBatchValues) valueArrays[projectedCol]).stringAt(valueIndex);
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

    /// Get the end index (exclusive) for a repeated column's list at the
    /// given record. With sentinel-suffixed `multiOffsets[k]` (length
    /// `count + 1`) the last record's end is just `ml[0][recordIndex + 1]`.
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
        return ml[0][recordIndex + 1];
    }

    /// Get the start index at a given multi-level offset level.
    int getLevelStart(int projectedCol, int level, int itemIndex) {
        return multiOffsets[projectedCol][level][itemIndex];
    }

    /// Get the end index (exclusive) at a given multi-level offset level.
    /// `multiOffsets[level]` is sentinel-suffixed (length `count + 1`), so
    /// the next slot is always available.
    int getLevelEnd(int projectedCol, int level, int itemIndex) {
        return multiOffsets[projectedCol][level][itemIndex + 1];
    }

    /// Check if a value at the given position is null at the leaf level.
    /// Validity polarity is **set bit = present**, so a null leaf is
    /// indicated by a clear bit (or a `null` validity reference means every
    /// leaf in the batch is present).
    boolean isElementNull(int projectedCol, int valueIndex) {
        BitSet validity = elementValidity[projectedCol];
        return validity != null && !validity.get(valueIndex);
    }
}
