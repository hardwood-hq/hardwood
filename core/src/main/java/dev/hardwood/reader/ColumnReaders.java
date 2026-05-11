/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.util.LinkedHashMap;
import java.util.Map;

import dev.hardwood.internal.reader.HardwoodContextImpl;
import dev.hardwood.internal.reader.RowGroupIterator;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.ProjectedSchema;

/// Holds multiple [ColumnReader] instances backed by a shared
/// [RowGroupIterator] for batch-oriented projection reads. Works for both
/// single- and multi-file [ParquetFileReader] inputs; the iterator
/// transparently handles cross-file prefetching when more than one file is
/// involved.
///
/// Use [#nextBatch()] to advance every underlying reader in lockstep — this is
/// the structurally-safe path for multi-column consumption: a single call drives
/// every reader, returns false when any is exhausted, and validates that the
/// readers report matching record counts.
///
/// ```java
/// try (ParquetFileReader parquet = ParquetFileReader.openAll(files);
///      ColumnReaders columns = parquet.buildColumnReaders(
///              ColumnProjection.columns("passenger_count", "trip_distance", "fare_amount"))
///              .build()) {
///
///     while (columns.nextBatch()) {
///         int count = columns.getRecordCount();
///         double[] v0 = columns.getColumnReader(0).getDoubles();
///         double[] v1 = columns.getColumnReader(1).getDoubles();
///         double[] v2 = columns.getColumnReader(2).getDoubles();
///         // ...
///     }
/// }
/// ```
public class ColumnReaders implements AutoCloseable {

    private final Map<String, ColumnReader> readersByName;
    private final ColumnReader[] readersByIndex;
    private int recordCount;
    private boolean batchAvailable;

    ColumnReaders(HardwoodContextImpl context,
                  RowGroupIterator rowGroupIterator,
                  FileSchema schema,
                  ProjectedSchema projectedSchema) {
        int projectedColumnCount = projectedSchema.getProjectedColumnCount();
        this.readersByName = new LinkedHashMap<>(projectedColumnCount);
        this.readersByIndex = new ColumnReader[projectedColumnCount];

        for (int i = 0; i < projectedColumnCount; i++) {
            int originalIndex = projectedSchema.toOriginalIndex(i);
            ColumnSchema columnSchema = schema.getColumn(originalIndex);

            ColumnReader reader = ColumnReader.createFromIterator(
                    columnSchema, schema, rowGroupIterator, context, i, null);

            readersByName.put(columnSchema.fieldPath().toString(), reader);
            readersByIndex[i] = reader;
        }
    }

    /// Get the number of projected columns.
    public int getColumnCount() {
        return readersByIndex.length;
    }

    /// Get the ColumnReader for a named column.
    /// For nested columns, use the dot-separated field path (e.g. `"address.zip"`).
    ///
    /// @param columnName the column name or dot-separated field path (must have been requested in the projection)
    /// @return the ColumnReader for the column
    /// @throws IllegalArgumentException if the column was not requested
    public ColumnReader getColumnReader(String columnName) {
        ColumnReader reader = readersByName.get(columnName);
        if (reader == null) {
            throw new IllegalArgumentException("Column '" + columnName + "' was not requested");
        }
        return reader;
    }

    /// Get the ColumnReader by index within the requested columns.
    ///
    /// @param index index within the requested column names (0-based)
    /// @return the ColumnReader at the given index
    public ColumnReader getColumnReader(int index) {
        return readersByIndex[index];
    }

    /// Advance every underlying [ColumnReader] to its next batch in lockstep.
    ///
    /// All readers share the same [RowGroupIterator], so they always publish batches at
    /// the same row boundaries. This method drives every reader once and returns:
    ///
    /// - `true` when every reader produced a new batch — callers can then read values
    ///   via the per-column accessors. The aligned record count is exposed through
    ///   [#getRecordCount()].
    /// - `false` when any reader is exhausted — partial advancement is impossible
    ///   because all readers consume from the shared iterator, so once one is done they
    ///   all are.
    ///
    /// As a defensive guard, a mismatch between the readers' published record counts
    /// throws [IllegalStateException]. Under correct internal behavior this can't
    /// happen — the guard exists to detect future regressions in the per-column drain
    /// workers, not to be triggered in production.
    ///
    /// Single-column consumers, or consumers that need fine-grained control over the
    /// per-reader cadence, can still call [ColumnReader#nextBatch()] directly on the
    /// readers returned by [#getColumnReader(int)] / [#getColumnReader(String)].
    ///
    /// @return true if a new aligned batch is available across all readers, false if exhausted
    /// @throws IllegalStateException if the readers report mismatched record counts
    public boolean nextBatch() {
        if (readersByIndex.length == 0) {
            batchAvailable = false;
            recordCount = 0;
            return false;
        }
        boolean firstAdvanced = readersByIndex[0].nextBatch();
        if (!firstAdvanced) {
            batchAvailable = false;
            recordCount = 0;
            // Drain any remaining readers so the shared iterator finalizes cleanly.
            for (int i = 1; i < readersByIndex.length; i++) {
                readersByIndex[i].nextBatch();
            }
            return false;
        }
        int firstCount = readersByIndex[0].getRecordCount();
        for (int i = 1; i < readersByIndex.length; i++) {
            if (!readersByIndex[i].nextBatch()) {
                throw new IllegalStateException(
                        "ColumnReader '" + readersByIndex[i].getColumnSchema().name()
                                + "' exhausted before peer column '"
                                + readersByIndex[0].getColumnSchema().name()
                                + "' — readers from the same projection should advance"
                                + " in lockstep");
            }
            int count = readersByIndex[i].getRecordCount();
            if (count != firstCount) {
                throw new IllegalStateException(
                        "ColumnReader batch sizes diverged: column '"
                                + readersByIndex[0].getColumnSchema().name() + "' has "
                                + firstCount + " records, column '"
                                + readersByIndex[i].getColumnSchema().name() + "' has "
                                + count);
            }
        }
        recordCount = firstCount;
        batchAvailable = true;
        return true;
    }

    /// Number of records in the most recently published batch.
    ///
    /// Equal to every underlying reader's [ColumnReader#getRecordCount()] — alignment is
    /// validated by [#nextBatch()].
    ///
    /// @throws IllegalStateException if no batch is currently available — call
    ///         [#nextBatch()] first
    public int getRecordCount() {
        if (!batchAvailable) {
            throw new IllegalStateException(
                    "No batch available — call nextBatch() first, and check that it returned true");
        }
        return recordCount;
    }

    @Override
    public void close() {
        for (ColumnReader reader : readersByIndex) {
            reader.close();
        }
    }
}
