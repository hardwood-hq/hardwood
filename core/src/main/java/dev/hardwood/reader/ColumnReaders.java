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
/// Use [#nextBatch()] to advance every underlying reader in lockstep — this is
/// the structurally-safe path for multi-column consumption: a single call drives
/// every reader, returns false when the readers are exhausted, and validates that the
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
    /// The first position's reader of each column.
    private final Map<String, ColumnReader> readersByName;
    /// One reader per position, in the order the projection requests the columns. A column
    /// requested more than once has a reader of its own at each position, all views of the one
    /// payload cursor, so that each tracks the batch it last took up.
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
            ColumnReader reader = new ColumnReader(scan, projectedIndex, schema, columnSchema);
            readersByName.putIfAbsent(columnSchema.fieldPath().toString(), reader);
            readersByIndex[position] = reader;
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
    /// their positions, with a reader of its own at each; [#getColumnReader(String)] returns
    /// the first.
    ///
    /// @param index index among the requested leaf columns (0-based)
    /// @return the ColumnReader at the given index
    public ColumnReader getColumnReader(int index) {
        return readersByIndex[index];
    }

    /// Advance every underlying [ColumnReader] to its next batch in lockstep.
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
    /// Calling [ColumnReader#nextBatch()] on each reader in turn instead moves the group
    /// once as well: the first reader called advances it, and the others take up the same
    /// batch.
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
        for (ColumnReader reader : readersByIndex) {
            reader.adoptCurrentStep();
        }
        return advanced;
    }

    /// Number of records in the group's current batch, whether [#nextBatch()] or
    /// [ColumnReader#nextBatch()] on one of its readers advanced the group to it.
    ///
    /// Equal to every underlying reader's [ColumnReader#getRecordCount()] once that reader
    /// has taken up the batch — alignment is validated when the group advances.
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

    /// Releases the resources held by every reader of this group. Idempotent.
    @Override
    public void close() throws IOException {
        scan.close();
    }

}
