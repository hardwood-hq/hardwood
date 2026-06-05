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

import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.reader.HardwoodContextImpl;
import dev.hardwood.internal.reader.RowGroupIterator;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

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
    /// Non-null on the exact-filtering path (#624): drives all readers in
    /// lockstep and compacts each to the matching records per batch. `null` on
    /// the plain projection path.
    private final FilterCoordinator coordinator;
    private int recordCount;
    private boolean batchAvailable;

    ColumnReaders(HardwoodContextImpl context,
                  RowGroupIterator rowGroupIterator,
                  FileSchema schema,
                  ProjectedSchema projectedSchema,
                  int batchSize) {
        int projectedColumnCount = projectedSchema.getProjectedColumnCount();
        this.readersByName = new LinkedHashMap<>(projectedColumnCount);
        this.readersByIndex = new ColumnReader[projectedColumnCount];
        this.coordinator = null;

        for (int i = 0; i < projectedColumnCount; i++) {
            int originalIndex = projectedSchema.toOriginalIndex(i);
            ColumnSchema columnSchema = schema.getColumn(originalIndex);

            ColumnReader reader = ColumnReader.createFromIterator(
                    columnSchema, schema, rowGroupIterator, context, i, null, batchSize);

            readersByName.put(columnSchema.fieldPath().toString(), reader);
            readersByIndex[i] = reader;
        }
    }

    private ColumnReaders(Map<String, ColumnReader> readersByName,
                          ColumnReader[] readersByIndex,
                          FilterCoordinator coordinator) {
        this.readersByName = readersByName;
        this.readersByIndex = readersByIndex;
        this.coordinator = coordinator;
    }

    /// Builds a filtered [ColumnReaders] that returns only the records matching
    /// `resolved` (#624). Every column of `augProjected` (payload columns plus
    /// the predicate columns) is decoded through one shared iterator; the
    /// exposed readers are those of `payloadProjection`, compacted to the
    /// matching records each batch. Predicate columns not in `payloadProjection`
    /// are decoded to evaluate the predicate but are not exposed.
    static ColumnReaders filtered(HardwoodContextImpl context,
                                  RowGroupIterator rowGroupIterator,
                                  FileSchema schema,
                                  ProjectedSchema augProjected,
                                  ProjectedSchema payloadProjected,
                                  ResolvedPredicate resolved,
                                  int batchSize) {
        int augCount = augProjected.getProjectedColumnCount();
        ColumnReader[] allReaders = new ColumnReader[augCount];
        Map<String, ColumnReader> byPath = new LinkedHashMap<>(augCount);
        for (int i = 0; i < augCount; i++) {
            ColumnSchema columnSchema = schema.getColumn(augProjected.toOriginalIndex(i));
            ColumnReader reader = ColumnReader.createFromIterator(
                    columnSchema, schema, rowGroupIterator, context, i, null, batchSize);
            allReaders[i] = reader;
            byPath.put(columnSchema.fieldPath().toString(), reader);
        }

        int payloadCount = payloadProjected.getProjectedColumnCount();
        Map<String, ColumnReader> readersByName = new LinkedHashMap<>(payloadCount);
        ColumnReader[] payloadReaders = new ColumnReader[payloadCount];
        for (int p = 0; p < payloadCount; p++) {
            ColumnSchema columnSchema = schema.getColumn(payloadProjected.toOriginalIndex(p));
            ColumnReader reader = byPath.get(columnSchema.fieldPath().toString());
            payloadReaders[p] = reader;
            readersByName.put(columnSchema.fieldPath().toString(), reader);
        }

        SelectionEngine engine = SelectionEngine.create(schema, augProjected, resolved, allReaders, batchSize);
        FilterCoordinator coordinator = new FilterCoordinator(allReaders, payloadReaders, engine);
        for (ColumnReader reader : allReaders) {
            reader.setCoordinator(coordinator);
        }
        return new ColumnReaders(readersByName, payloadReaders, coordinator);
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
        if (coordinator != null) {
            boolean advanced = coordinator.advance();
            for (ColumnReader reader : readersByIndex) {
                reader.syncGeneration();
            }
            recordCount = coordinator.recordCount();
            batchAvailable = advanced;
            return advanced;
        }
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
        if (coordinator != null) {
            coordinator.close();
            return;
        }
        for (ColumnReader reader : readersByIndex) {
            reader.close();
        }
    }
}
