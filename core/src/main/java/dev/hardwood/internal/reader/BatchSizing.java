/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.List;

import dev.hardwood.internal.conversion.FixedWidths;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.schema.ColumnSchema;

/// Computes optimal batch sizes for column data assembly.
public final class BatchSizing {

    /// Hard upper bound on the batch size returned by
    /// [#computeOptimalBatchSize(ProjectedSchema, double[], long)], and on the values per batch
    /// of a binary column. Other components that pre-size structures around the worst-case
    /// batch size (e.g. the `ALL_PRESENT` sentinel in `FlatRowReader`) read this constant.
    ///
    /// `ColumnReader.getStrings()` and `getBinaries()` build one reference array per batch,
    /// holding a binary column's values. G1 allocates an object larger than half a heap region
    /// as a humongous object, in whole old-generation regions, and allocating one per batch
    /// makes garbage collection take most of the process CPU. This is the longest reference
    /// array that stays an ordinary object at G1's smallest region (1 MB) with compressed
    /// references: half a region less a 24-byte allowance for the array header (16 bytes by
    /// default, 12 with compact object headers), over 4 bytes per reference.
    /// Batches from about 128K rows up read equally fast, so the bound costs no throughput.
    public static final int MAX_BATCH = (512 * 1024 - 24) / 4;

    /// Passed as `availableRows` where the rows a read can produce are not known ahead of it,
    /// as for a multi-file read whose later files have not been planned. The batch then follows
    /// the byte budget alone.
    public static final long ROWS_UNKNOWN = -1;

    private BatchSizing() {}

    /// Computes a batch size that keeps all column arrays for one batch within a fixed byte budget.
    ///
    /// Each batch allocates one value array per projected column, sized to the batch's
    /// **value** count. For a flat column that equals the row count, but for a repeated
    /// (list) column each row expands to several leaf values, so the value array is
    /// `valuesPerRow` times larger than the row count. `valuesPerRow[i]` is the average
    /// leaf values per top-level row for projected column `i` (its list fan-out); a `null`
    /// array, a short array, or a non-positive entry falls back to `1.0` (one value per
    /// row). Sizing by rows alone — ignoring fan-out — makes a wide list column's value
    /// array many times larger than the intended budget, and both assembly and the consumer
    /// run memory-bound.
    ///
    /// The batch is sized so the total value memory stays under the target (6 MB), clamped
    /// to at least one row and at most [#MAX_BATCH]. No larger row floor is applied: because
    /// the value count per batch is `rows * fan-out ~= budget / valueBytes`, the work per
    /// batch is roughly constant regardless of fan-out, so a high-fan-out column's small row
    /// count still carries a full batch of values to amortise per-batch overhead.
    ///
    /// For example, 10 projected DOUBLE columns (8 bytes each, one value per row = 80
    /// bytes/row) yield `6 MB / 80 = 78 643` rows; a single `LIST<float32>` of 768-wide
    /// vectors (`768 * 4 = 3072` bytes/row) yields `6 MB / 3072 = 2048` rows. A projection
    /// narrower than 48 bytes/row budgets above [#MAX_BATCH] and is clamped to it.
    private static int budgetedBatchSize(ProjectedSchema projectedSchema, double[] valuesPerRow) {
        // Target 6 MB of value memory per batch, enough rows to amortise the per-batch overhead.
        long targetBytes = 6L * 1024 * 1024;
        int maxBatch = MAX_BATCH;

        double bytesPerRow = 0;
        for (int i = 0; i < projectedSchema.getProjectedColumnCount(); i++) {
            ColumnSchema column = projectedSchema.getProjectedColumn(i);
            bytesPerRow += fanout(valuesPerRow, i) * (columnByteWidth(column) + levelBytesPerValue(column));
        }

        if (bytesPerRow <= 0) {
            bytesPerRow = 8;
        }

        return (int) Math.min(maxBatch, Math.max(1, (long) (targetBytes / bytesPerRow)));
    }

    /// Returns the most rows whose values fit in [#MAX_BATCH] for every projected binary column,
    /// at least 1, or [#MAX_BATCH] where no column is binary. A primitive column's values are
    /// bounded by the byte budget alone, so a wide list of numbers keeps its full batch.
    private static int binaryValueRows(ProjectedSchema projectedSchema, double[] valuesPerRow) {
        long rows = MAX_BATCH;
        for (int i = 0; i < projectedSchema.getProjectedColumnCount(); i++) {
            if (isBinary(projectedSchema.getProjectedColumn(i))) {
                rows = Math.min(rows, (long) (MAX_BATCH / fanout(valuesPerRow, i)));
            }
        }
        return (int) Math.max(1, rows);
    }

    /// Whether `ColumnReader.getBinaries()` reads the column, building a reference array of its
    /// values.
    private static boolean isBinary(ColumnSchema column) {
        return switch (column.type()) {
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 -> true;
            case BOOLEAN, INT32, INT64, FLOAT, DOUBLE -> false;
        };
    }

    /// Returns projected column `i`'s list fan-out, falling back to `1.0` for a `null` or short
    /// array or a non-positive entry.
    private static double fanout(double[] valuesPerRow, int i) {
        return valuesPerRow != null && i < valuesPerRow.length && valuesPerRow[i] > 0
                ? valuesPerRow[i]
                : 1.0;
    }

    /// Computes a batch size that keeps all column arrays for one batch within a fixed byte
    /// budget and a binary column's values per batch within [#MAX_BATCH], capped at
    /// `availableRows`, the rows the read can produce, or [#ROWS_UNKNOWN] where that
    /// is not known.
    ///
    /// The byte budget sizes a batch from the projected columns' widths alone, so a read
    /// shorter than one batch would carry arrays for rows that cannot arrive: a single `INT64`
    /// column over a 600-row file budgets [#MAX_BATCH], a 1 MB array per batch to hold 600
    /// values. The cap binds only in that case. Where the read is longer than a batch the
    /// budgeted size is returned unchanged, so batches stay as full as they were.
    ///
    /// `availableRows` must be an upper bound on the read. Row-group pruning, a row limit and a
    /// skip only ever remove rows, so a file's own row count bounds any read of it.
    ///
    /// @param projectedSchema the columns the read decodes
    /// @param valuesPerRow each projected column's list fan-out, or `null` for one value per row
    /// @param availableRows an upper bound on the rows the read produces, or [#ROWS_UNKNOWN]
    /// @return the batch size in rows, at least 1
    /// @throws IllegalArgumentException if `availableRows` is negative and not [#ROWS_UNKNOWN]
    public static int computeOptimalBatchSize(ProjectedSchema projectedSchema, double[] valuesPerRow,
            long availableRows) {
        int budgeted = Math.min(budgetedBatchSize(projectedSchema, valuesPerRow),
                binaryValueRows(projectedSchema, valuesPerRow));
        if (availableRows == ROWS_UNKNOWN) {
            return budgeted;
        }
        if (availableRows < 0) {
            throw new IllegalArgumentException("availableRows must not be negative: " + availableRows);
        }
        return (int) Math.max(1, Math.min(budgeted, availableRows));
    }

    /// Returns the total row count of `rowGroups`, an upper bound on any read over them.
    public static long totalRows(List<RowGroup> rowGroups) {
        long rows = 0;
        for (RowGroup rowGroup : rowGroups) {
            rows = Math.addExact(rows, rowGroup.numRows());
        }
        return rows;
    }

    /// Computes each projected column's average list fan-out — leaf values per
    /// top-level row — from row-group metadata, for
    /// [#computeOptimalBatchSize(ProjectedSchema, double[], long)]. A column's fan-out is
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
    /// they count toward the batch's byte budget. Non-repeated columns carry no
    /// per-value level arrays (nullability rides a packed validity bitmap), so they
    /// contribute `0`.
    private static int levelBytesPerValue(ColumnSchema column) {
        return column.maxRepetitionLevel() > 0 ? 2 * Integer.BYTES : 0;
    }

    /// Returns the estimated byte width of a single value for the given column's physical type.
    /// Variable-length types use a 16-byte estimate (pointer + average payload).
    private static int columnByteWidth(ColumnSchema col) {
        return switch (col.type()) {
            case INT32, FLOAT, INT64, DOUBLE, INT96 -> FixedWidths.of(col.type());
            // One `boolean` per value in the decoded batch.
            case BOOLEAN -> 1;
            // Rough estimate; UTF8/ENUM/JSON columns cost a little more per row — they
            // also carry a lazily-allocated per-value dictionary-index array for
            // interned-String reuse — but the byte-array estimate is intentionally
            // approximate.
            case BYTE_ARRAY -> 16;
            // The width is present and positive: RowGroupIterator#initialize validates every
            // projected column before a batch size is ever computed for it.
            case FIXED_LEN_BYTE_ARRAY -> col.typeLength();
        };
    }
}
