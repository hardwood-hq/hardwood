/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import dev.hardwood.internal.writer.IntArrayColumnSource;
import dev.hardwood.internal.writer.IntColumnSource;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

/// One aligned slice of a file's columns. Every column in a batch must have the same
/// number of values, which is the batch's row count.
///
/// A batch is not constructed directly: [ParquetFileWriter#writeBatch] creates it, bound
/// to the schema, hands it to a filler that populates the columns, then submits it — so
/// there is no separate build or submit step to forget. Columns are addressed by index or
/// by name, and the schema binding lets every identifier be validated as it is added — an
/// unknown name, an out-of-range index, a non-`INT32` column, or setting the same column
/// twice (whether by index or name) all fail eagerly rather than at write time.
///
/// ```java
/// writer.writeBatch(b -> b
///         .ints(0, idColumn)
///         .ints("value", valueColumn));
/// ```
public final class ColumnBatch {

    private final FileSchema schema;
    private final IntColumnSource[] sources;
    private int rowCount = -1;
    private boolean consumed;

    ColumnBatch(FileSchema schema) {
        this.schema = schema;
        this.sources = new IntColumnSource[schema.getColumnCount()];
    }

    /// Adds the values for a `REQUIRED INT32` column, addressed by index.
    ///
    /// The array is referenced, not copied, so it must not be mutated until the batch
    /// has been written.
    ///
    /// @param columnIndex zero-based leaf-column index
    /// @param values the column's values for this batch
    /// @return this batch, for chaining
    /// @throws IllegalArgumentException if the index is out of range, the column is
    ///         already set, or the length does not match the other columns in this batch
    public ColumnBatch ints(int columnIndex, int[] values) {
        if (columnIndex < 0 || columnIndex >= sources.length) {
            throw new IllegalArgumentException(
                    "Column index " + columnIndex + " is out of range [0, " + sources.length + ")");
        }
        set(columnIndex, values);
        return this;
    }

    /// Adds the values for a `REQUIRED INT32` column, addressed by name.
    ///
    /// The array is referenced, not copied, so it must not be mutated until the batch
    /// has been written.
    ///
    /// @param columnName the column's name
    /// @param values the column's values for this batch
    /// @return this batch, for chaining
    /// @throws IllegalArgumentException if no column has that name, the column is already
    ///         set (by name or index), or the length does not match the other columns
    public ColumnBatch ints(String columnName, int[] values) {
        // getColumn(String) throws on an unknown name, so the identifier is validated here.
        set(schema.getColumn(columnName).columnIndex(), values);
        return this;
    }

    private void set(int columnIndex, int[] values) {
        if (consumed) {
            throw new IllegalStateException("Batch has already been written and cannot be modified");
        }
        if (values == null) {
            throw new IllegalArgumentException("values must not be null for column " + describe(columnIndex));
        }
        ColumnSchema column = schema.getColumn(columnIndex);
        if (column.type() != PhysicalType.INT32) {
            throw new IllegalArgumentException("Column " + describe(columnIndex) + " is " + column.type()
                    + ", not INT32");
        }
        if (sources[columnIndex] != null) {
            throw new IllegalArgumentException("Column " + describe(columnIndex) + " is already set in this batch");
        }
        if (rowCount < 0) {
            rowCount = values.length;
        }
        else if (rowCount != values.length) {
            throw new IllegalArgumentException("Ragged batch: column " + describe(columnIndex) + " has "
                    + values.length + " values but the batch row count is " + rowCount);
        }
        sources[columnIndex] = new IntArrayColumnSource(values);
    }

    private String describe(int columnIndex) {
        return columnIndex + " (" + schema.getColumn(columnIndex).name() + ")";
    }

    /// Marks the batch written, so a filler that stashed a reference and mutates it after
    /// `writeBatch` returns fails loudly instead of silently doing nothing.
    void markConsumed() {
        consumed = true;
    }

    /// The number of rows in this batch (zero if no columns were added).
    int rowCount() {
        return rowCount < 0 ? 0 : rowCount;
    }

    /// The value sources in column order, after checking every column was set.
    IntColumnSource[] completedSources() {
        for (int c = 0; c < sources.length; c++) {
            if (sources[c] == null) {
                throw new IllegalArgumentException("Batch is missing column " + describe(c));
            }
        }
        return sources;
    }
}
