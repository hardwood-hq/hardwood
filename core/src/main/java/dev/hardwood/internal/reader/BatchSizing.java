/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.List;

import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.schema.ColumnSchema;

/// Computes optimal batch sizes for column data assembly.
public final class BatchSizing {

    /// Hard upper bound on the batch size returned by
    /// [#computeOptimalBatchSize(ProjectedSchema)]. Other components that
    /// pre-size structures around the worst-case batch size (e.g. the
    /// `ALL_PRESENT` sentinel in `FlatRowReader`) read this constant.
    public static final int MAX_BATCH = 524288;

    private BatchSizing() {}

    /// Computes a batch size that keeps all column arrays for one batch within the L2 cache.
    ///
    /// Equivalent to [#computeOptimalBatchSize(ProjectedSchema, double[])] with no
    /// fan-out information — every column is assumed to hold one value per row.
    public static int computeOptimalBatchSize(ProjectedSchema projectedSchema) {
        return computeOptimalBatchSize(projectedSchema, null);
    }

    /// Computes a batch size that keeps all column arrays for one batch within the L2 cache.
    ///
    /// Each batch allocates one value array per projected column, sized to the batch's
    /// **value** count. For a flat column that equals the row count, but for a repeated
    /// (list) column each row expands to several leaf values, so the value array is
    /// `valuesPerRow` times larger than the row count. `valuesPerRow[i]` is the average
    /// leaf values per top-level row for projected column `i` (its list fan-out); a `null`
    /// array, a short array, or a non-positive entry falls back to `1.0` (one value per
    /// row). Sizing by rows alone — ignoring fan-out — makes a wide list column's value
    /// array many times larger than the intended budget, so the batch no longer fits in
    /// cache and both assembly and the consumer run memory-bound.
    ///
    /// The batch is sized so the total value memory stays under the target (6 MB), clamped
    /// to at least one row and at most [#MAX_BATCH]. No larger row floor is applied: because
    /// the value count per batch is `rows * fan-out ~= budget / valueBytes`, the work per
    /// batch is roughly constant regardless of fan-out, so a high-fan-out column's small row
    /// count still carries a full batch of values to amortise per-batch overhead.
    ///
    /// For example, 3 projected DOUBLE columns (8 bytes each, one value per row = 24
    /// bytes/row) yields `6 MB / 24 = 262 144` rows; a single `LIST<float32>` of 768-wide
    /// vectors (`768 * 4 = 3072` bytes/row) yields `6 MB / 3072 = 2048` rows.
    public static int computeOptimalBatchSize(ProjectedSchema projectedSchema, double[] valuesPerRow) {
        // Target 6 MB of value memory per batch (fits comfortably in L2 cache).
        long targetBytes = 6L * 1024 * 1024;
        int maxBatch = MAX_BATCH;

        double bytesPerRow = 0;
        for (int i = 0; i < projectedSchema.getProjectedColumnCount(); i++) {
            ColumnSchema column = projectedSchema.getProjectedColumn(i);
            double fanout = valuesPerRow != null && i < valuesPerRow.length && valuesPerRow[i] > 0
                    ? valuesPerRow[i]
                    : 1.0;
            bytesPerRow += fanout * (columnByteWidth(column) + levelBytesPerValue(column));
        }

        if (bytesPerRow <= 0) {
            bytesPerRow = 8;
        }

        return (int) Math.min(maxBatch, Math.max(1, (long) (targetBytes / bytesPerRow)));
    }

    /// Computes each projected column's average list fan-out — leaf values per
    /// top-level row — from row-group metadata, for
    /// [#computeOptimalBatchSize(ProjectedSchema, double[])]. A column's fan-out is
    /// its total leaf value count across `rowGroups` divided by the total row count.
    /// Returns `null` when there are no rows (the caller then assumes one value per
    /// row). A flat column's fan-out is `1`; a `LIST<float32>` of 768-wide vectors is
    /// `768`.
    public static double[] valuesPerRow(ProjectedSchema projectedSchema, List<RowGroup> rowGroups) {
        long totalRows = 0;
        for (RowGroup rowGroup : rowGroups) {
            totalRows += rowGroup.numRows();
        }
        if (totalRows == 0) {
            return null;
        }
        int columnCount = projectedSchema.getProjectedColumnCount();
        double[] valuesPerRow = new double[columnCount];
        for (int i = 0; i < columnCount; i++) {
            FieldPath path = projectedSchema.getProjectedColumn(i).fieldPath();
            long values = 0;
            for (RowGroup rowGroup : rowGroups) {
                for (ColumnChunk chunk : rowGroup.columns()) {
                    if (chunk.metaData().pathInSchema().equals(path)) {
                        values += chunk.metaData().numValues();
                        break;
                    }
                }
            }
            valuesPerRow[i] = (double) values / totalRows;
        }
        return valuesPerRow;
    }

    /// Bytes of repetition/definition level storage a repeated column's nested
    /// worker holds per leaf value — one `int` definition level plus one `int`
    /// repetition level. A repeated column drains through [NestedColumnWorker],
    /// which accumulates both `int[]` level arrays alongside the value array, so
    /// they count toward the batch's cache footprint. Non-repeated columns carry no
    /// per-value level arrays (nullability rides a packed validity bitmap), so they
    /// contribute `0`.
    private static int levelBytesPerValue(ColumnSchema column) {
        return column.maxRepetitionLevel() > 0 ? 2 * Integer.BYTES : 0;
    }

    /// Returns the estimated byte width of a single value for the given column's physical type.
    /// Variable-length types use a 16-byte estimate (pointer + average payload).
    private static int columnByteWidth(ColumnSchema col) {
        return switch (col.type()) {
            case INT32, FLOAT -> 4;
            case INT64, DOUBLE -> 8;
            case BOOLEAN -> 1;
            case INT96 -> 12;
            // Rough estimate; UTF8/JSON columns cost a little more per row — they
            // also carry a lazily-allocated per-value dictionary-index array for
            // interned-String reuse — but the byte-array estimate is intentionally
            // approximate.
            case BYTE_ARRAY -> 16;
            case FIXED_LEN_BYTE_ARRAY -> col.typeLength() != null ? col.typeLength() : 16;
        };
    }
}
