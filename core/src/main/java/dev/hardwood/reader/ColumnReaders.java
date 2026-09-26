/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

/// Holds multiple [ColumnReader] instances backed by one shared decode
/// pipeline for batch-oriented projection reads. Works for both
/// single- and multi-file [ParquetFileReader] inputs; the pipeline
/// transparently handles cross-file prefetching when more than one file is
/// involved.
///
/// [#nextBatch()] advances every underlying reader in lockstep: a single call drives
/// every reader, returns false when the readers are exhausted, and validates that the
/// readers report matching record counts. The readers show the group's current batch;
/// they do not advance or close on their own.
///
/// ```java
/// try (ParquetFileReader parquet = ParquetFileReader.openAll(files);
///      ColumnReaders columns = parquet.buildColumnReaders(
///              ColumnProjection.columns("passenger_count", "trip_distance", "fare_amount"))
///              .build()) {
///
///     while (columns.nextBatch()) {
///         int count = columns.getRecordCount();
///         long[] v0 = columns.getColumnReader(0).getLongs();
///         double[] v1 = columns.getColumnReader(1).getDoubles();
///         double[] v2 = columns.getColumnReader(2).getDoubles();
///         // ...
///     }
/// }
/// ```
public class ColumnReaders implements Closeable {

    /// The pipeline every reader of this group is a view of.
    private final ColumnScan scan;
    /// One reader per column, by field path.
    private final Map<String, ColumnReader> readersByName;
    /// One entry per position, in the order the projection requests the columns. A column
    /// requested more than once has the same reader at each of its positions.
    private final ColumnReader[] readersByIndex;

    /// A group of views over the payload columns of `scan`, the columns of `payload`.
    ColumnReaders(ColumnScan scan, FileSchema schema, ProjectedSchema payload) {
        int positionCount = payload.requestedColumnCount();
        this.scan = scan;
        this.readersByName = new LinkedHashMap<>(positionCount);
        this.readersByIndex = new ColumnReader[positionCount];
        for (int position = 0; position < positionCount; position++) {
            int projectedIndex = payload.requestedColumn(position);
            ColumnSchema columnSchema = schema.getColumn(payload.toOriginalIndex(projectedIndex));
            readersByIndex[position] = readersByName.computeIfAbsent(columnSchema.fieldPath().toString(),
                    path -> new ColumnReader(scan, projectedIndex, schema, columnSchema, true));
        }
    }

    /// Get the number of requested columns, counting a column once for every name in the
    /// projection that selects it.
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
    /// Indices follow the order the projection names its columns in, one index for every leaf
    /// column a name selects. A name that selects several columns, such as a group or
    /// [ColumnProjection#all()], contributes them in schema order, so a column listed after a
    /// group in `columns(...)` sits after all of that group's leaf columns and moves when the
    /// group gains or loses a field. A column that several names select appears at each of
    /// their positions, with the same reader at each.
    ///
    /// @param index index among the requested leaf columns (0-based)
    /// @return the ColumnReader at the given index
    public ColumnReader getColumnReader(int index) {
        return readersByIndex[index];
    }

    /// Advance every underlying [ColumnReader] to its next batch in lockstep. This is the only
    /// way to advance the group: [ColumnReader#nextBatch()] on one of its readers throws
    /// [IllegalStateException].
    ///
    /// All readers share one pipeline, so they always publish batches at the same row
    /// boundaries. This method advances that pipeline once and returns:
    ///
    /// - `true` when a new batch is available — callers can then read values via the
    ///   per-column accessors. The aligned record count is exposed through
    ///   [#getRecordCount()].
    /// - `false` when the readers are exhausted — partial advancement is impossible
    ///   because all readers consume from the shared pipeline, so once one is done they
    ///   all are.
    ///
    /// As a defensive guard, a mismatch between the columns' decoded record counts
    /// throws [IllegalStateException]. Under correct internal behavior this can't
    /// happen — the guard exists to detect future regressions in the per-column drain
    /// workers, not to be triggered in production.
    ///
    /// @return true if a new aligned batch is available across all readers, false if exhausted
    /// @throws IOException if the bytes could not be read
    /// @throws dev.hardwood.reader.ParquetReadException if the file's bytes are not what a
    ///         Parquet file can say: a footer or a page index that will not parse, a
    ///         dictionary page the metadata places outside its column chunk, a page whose
    ///         checksum fails, values that do not decode under the encoding declared for
    ///         them. In a multi-file read this covers a later file that is not Parquet at
    ///         all, or whose schema cannot be reconciled with the first file's
    /// @throws IllegalStateException if the readers report mismatched record counts, or the
    ///         group was closed
    public boolean nextBatch() throws IOException {
        boolean advanced = scan.advance();
        for (ColumnReader reader : readersByName.values()) {
            reader.adoptCurrentStep();
        }
        return advanced;
    }

    /// Number of records in the group's current batch.
    ///
    /// Equal to every underlying reader's [ColumnReader#getRecordCount()] — alignment is
    /// validated when the group advances.
    ///
    /// @throws IllegalStateException if no batch is currently available — call
    ///         [#nextBatch()] first
    public int getRecordCount() {
        if (!scan.hasBatch()) {
            throw new IllegalStateException(
                    "No batch available — call nextBatch() first, and check that it returned true");
        }
        return scan.recordCount();
    }

    /// Releases the resources held by every reader of this group, after which [#nextBatch()]
    /// throws [IllegalStateException]. Idempotent.
    @Override
    public void close() throws IOException {
        scan.close();
    }

}
