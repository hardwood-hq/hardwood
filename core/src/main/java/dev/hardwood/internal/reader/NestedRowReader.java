/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.UUID;

import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqDoubleList;
import dev.hardwood.row.PqIntList;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqLongList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.ProjectedSchema;

/// Row reader for nested schemas using the v3 pipeline.
///
/// Consumes [NestedBatch] objects with nested fields (definition levels,
/// repetition levels, record offsets) and delegates typed accessors to
/// [NestedBatchDataView]. Index structures are pre-computed by the
/// [NestedColumnWorker] drain thread before publishing.
public final class NestedRowReader implements RowReader {

    private final BatchExchange<NestedBatch>[] exchanges;
    private final NestedColumnWorker[] columnWorkers;
    private final int columnCount;

    private final FileSchema fileSchema;
    private final ProjectedSchema projectedSchema;
    private final NestedBatchDataView dataView;
    private final ColumnSchema[] columnSchemas;

    // Iteration state
    private NestedBatch[] previousBatches;
    private int rowIndex = -1;
    private int batchSize = 0;
    private boolean exhausted;

    // File name from the current batch — used for exception enrichment
    private String currentFileName;


    NestedRowReader(BatchExchange<NestedBatch>[] exchanges, NestedColumnWorker[] columnWorkers,
                    FileSchema fileSchema, ProjectedSchema projectedSchema) {
        this.exchanges = exchanges;
        this.columnWorkers = columnWorkers;
        this.columnCount = exchanges.length;
        this.fileSchema = fileSchema;
        this.projectedSchema = projectedSchema;
        this.dataView = new NestedBatchDataView(fileSchema, projectedSchema);
        this.previousBatches = new NestedBatch[columnCount];

        // Cache column schemas for batch wrapping
        this.columnSchemas = new ColumnSchema[columnCount];
        for (int i = 0; i < columnCount; i++) {
            int originalIndex = projectedSchema.toOriginalIndex(i);
            columnSchemas[i] = fileSchema.getColumn(originalIndex);
        }
    }

    /// Eagerly loads the first batch. Must be called after construction.
    void initialize() {
        if (!loadNextBatch()) {
            exhausted = true;
        }
    }

    // ==================== Factory ====================

    /// Creates a nested v3 pipeline and returns a [RowReader].
    ///
    /// Wires up `RowGroupIterator → PageSource → NestedColumnWorker → BatchExchange →
    /// NestedRowReader`, starts all column workers, and initializes the reader.
    /// When a filter is present, wraps in [FilteredRowReader] for record-level filtering.
    ///
    /// @param rowGroupIterator pre-configured iterator
    /// @param schema the file schema
    /// @param projectedSchema the projected column schema
    /// @param context the hardwood context
    /// @param filter resolved predicate, or `null` for no filtering
    /// @param maxRows maximum rows (0 = unlimited), enforced by [ColumnWorker] drain
    /// @return a [NestedRowReader] or [FilteredRowReader]
    public static RowReader create(RowGroupIterator rowGroupIterator,
                            FileSchema schema,
                            ProjectedSchema projectedSchema,
                            HardwoodContextImpl context,
                            ResolvedPredicate filter,
                            long maxRows) {
        int batchSize = BatchSizing.computeOptimalBatchSize(projectedSchema);
        int projectedColumnCount = projectedSchema.getProjectedColumnCount();
        NestedColumnWorker[] workers = new NestedColumnWorker[projectedColumnCount];
        @SuppressWarnings("unchecked")
        BatchExchange<NestedBatch>[] buffers = new BatchExchange[projectedColumnCount];

        for (int i = 0; i < projectedColumnCount; i++) {
            int originalIndex = projectedSchema.toOriginalIndex(i);
            ColumnSchema columnSchema = schema.getColumn(originalIndex);

            PageSource pageSource = new PageSource(rowGroupIterator, i);

            PhysicalType physType = columnSchema.type();
            BatchExchange<NestedBatch> buffer = BatchExchange.recycling(
                    columnSchema.name(), () -> {
                        NestedBatch b = new NestedBatch();
                        b.values = BatchExchange.allocateArray(physType, batchSize);
                        return b;
                    });
            int[] levelNullThresholds = columnSchema.maxRepetitionLevel() > 0
                    ? NestedLevelComputer.computeLevelNullThresholds(
                            schema.getRootNode(), columnSchema.columnIndex())
                    : null;
            NestedColumnWorker worker = new NestedColumnWorker(
                    pageSource, buffer, columnSchema, batchSize,
                    context.decompressorFactory(), context.executor(), maxRows,
                    levelNullThresholds);

            buffers[i] = buffer;
            workers[i] = worker;
            worker.start();
        }

        NestedRowReader reader = new NestedRowReader(buffers, workers, schema, projectedSchema);
        reader.initialize();
        if (filter != null) {
            return new FilteredRowReader(reader, filter, schema);
        }
        return reader;
    }

    // ==================== Iteration ====================

    @Override
    public boolean hasNext() {
        try {
            if (exhausted) {
                return false;
            }
            if (rowIndex + 1 < batchSize) {
                return true;
            }
            return loadNextBatch();
        }
        catch (RuntimeException e) {
            throw wrapException(e);
        }
    }

    @Override
    public void next() {
        rowIndex++;
        dataView.setRowIndex(rowIndex);
    }

    // ==================== Batch Loading ====================

    private boolean loadNextBatch() {
        // Poll columns sequentially with manual recycling and error checking.
        // Each poll is non-blocking when its exchange has a batch ready; the
        // pipeline runs ahead of the consumer in steady state, so the per-call
        // cost is dominated by the first non-blocking readyQueue.poll().
        NestedBatch[] batches = new NestedBatch[columnCount];
        for (int i = 0; i < columnCount; i++) {
            if (previousBatches[i] != null) {
                exchanges[i].freeQueue().offer(previousBatches[i]);
                previousBatches[i] = null;
            }
            NestedBatch batch;
            try {
                batch = exchanges[i].poll();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
            if (batch == null || batch.recordCount == 0) {
                for (int j = 0; j < columnCount; j++) {
                    exchanges[j].checkError();
                }
                if (i > 0) {
                    throw new IllegalStateException(
                            "Column count mismatch: column " + i + " produced no data"
                            + " while earlier columns had " + batches[0].recordCount + " records");
                }
                exhausted = true;
                return false;
            }
            batches[i] = batch;
            previousBatches[i] = batch;
        }

        batchSize = batches[0].recordCount;
        currentFileName = batches[0].fileName;

        // Index structures are pre-computed by the drain — just assemble the view
        dataView.setBatchData(batches, columnSchemas);
        rowIndex = -1;
        return true;
    }

    // ==================== Accessors (delegate to NestedBatchDataView) ====================

    @Override public boolean isNull(int i) { try { return dataView.isNull(i); } catch (RuntimeException e) { throw wrapException(e); } }
    @Override public boolean isNull(String name) { try { return dataView.isNull(name); } catch (RuntimeException e) { throw wrapException(e); } }

    @Override public int getInt(int i) { try { return dataView.getInt(i); } catch (RuntimeException e) { throw wrapException(e); } }
    @Override public int getInt(String name) { try { return dataView.getInt(name); } catch (RuntimeException e) { throw wrapException(e); } }
    @Override public long getLong(int i) { try { return dataView.getLong(i); } catch (RuntimeException e) { throw wrapException(e); } }
    @Override public long getLong(String name) { try { return dataView.getLong(name); } catch (RuntimeException e) { throw wrapException(e); } }
    @Override public float getFloat(int i) { try { return dataView.getFloat(i); } catch (RuntimeException e) { throw wrapException(e); } }
    @Override public float getFloat(String name) { try { return dataView.getFloat(name); } catch (RuntimeException e) { throw wrapException(e); } }
    @Override public double getDouble(int i) { try { return dataView.getDouble(i); } catch (RuntimeException e) { throw wrapException(e); } }
    @Override public double getDouble(String name) { try { return dataView.getDouble(name); } catch (RuntimeException e) { throw wrapException(e); } }
    @Override public boolean getBoolean(int i) { try { return dataView.getBoolean(i); } catch (RuntimeException e) { throw wrapException(e); } }
    @Override public boolean getBoolean(String name) { try { return dataView.getBoolean(name); } catch (RuntimeException e) { throw wrapException(e); } }

    @Override public String getString(int i) { try { return dataView.getString(i); } catch (RuntimeException e) { throw wrapException(e); } }
    @Override public String getString(String name) { try { return dataView.getString(name); } catch (RuntimeException e) { throw wrapException(e); } }
    @Override public byte[] getBinary(int i) { try { return dataView.getBinary(i); } catch (RuntimeException e) { throw wrapException(e); } }
    @Override public byte[] getBinary(String name) { try { return dataView.getBinary(name); } catch (RuntimeException e) { throw wrapException(e); } }
    @Override public LocalDate getDate(int i) { try { return dataView.getDate(i); } catch (RuntimeException e) { throw wrapException(e); } }
    @Override public LocalDate getDate(String name) { try { return dataView.getDate(name); } catch (RuntimeException e) { throw wrapException(e); } }
    @Override public LocalTime getTime(int i) { try { return dataView.getTime(i); } catch (RuntimeException e) { throw wrapException(e); } }
    @Override public LocalTime getTime(String name) { try { return dataView.getTime(name); } catch (RuntimeException e) { throw wrapException(e); } }
    @Override public Instant getTimestamp(int i) { try { return dataView.getTimestamp(i); } catch (RuntimeException e) { throw wrapException(e); } }
    @Override public Instant getTimestamp(String name) { try { return dataView.getTimestamp(name); } catch (RuntimeException e) { throw wrapException(e); } }
    @Override public BigDecimal getDecimal(int i) { try { return dataView.getDecimal(i); } catch (RuntimeException e) { throw wrapException(e); } }
    @Override public BigDecimal getDecimal(String name) { try { return dataView.getDecimal(name); } catch (RuntimeException e) { throw wrapException(e); } }
    @Override public UUID getUuid(int i) { try { return dataView.getUuid(i); } catch (RuntimeException e) { throw wrapException(e); } }
    @Override public UUID getUuid(String name) { try { return dataView.getUuid(name); } catch (RuntimeException e) { throw wrapException(e); } }

    @Override public Object getValue(int i) { try { return dataView.getValue(i); } catch (RuntimeException e) { throw wrapException(e); } }
    @Override public Object getValue(String name) { try { return dataView.getValue(name); } catch (RuntimeException e) { throw wrapException(e); } }

    @Override public PqStruct getStruct(String name) { try { return dataView.getStruct(name); } catch (RuntimeException e) { throw wrapException(e); } }
    @Override public PqStruct getStruct(int i) { try { return dataView.getStruct(i); } catch (RuntimeException e) { throw wrapException(e); } }
    @Override public PqIntList getListOfInts(String name) { try { return dataView.getListOfInts(name); } catch (RuntimeException e) { throw wrapException(e); } }
    @Override public PqIntList getListOfInts(int i) { try { return dataView.getListOfInts(i); } catch (RuntimeException e) { throw wrapException(e); } }
    @Override public PqLongList getListOfLongs(String name) { try { return dataView.getListOfLongs(name); } catch (RuntimeException e) { throw wrapException(e); } }
    @Override public PqLongList getListOfLongs(int i) { try { return dataView.getListOfLongs(i); } catch (RuntimeException e) { throw wrapException(e); } }
    @Override public PqDoubleList getListOfDoubles(String name) { try { return dataView.getListOfDoubles(name); } catch (RuntimeException e) { throw wrapException(e); } }
    @Override public PqDoubleList getListOfDoubles(int i) { try { return dataView.getListOfDoubles(i); } catch (RuntimeException e) { throw wrapException(e); } }
    @Override public PqList getList(String name) { try { return dataView.getList(name); } catch (RuntimeException e) { throw wrapException(e); } }
    @Override public PqList getList(int i) { try { return dataView.getList(i); } catch (RuntimeException e) { throw wrapException(e); } }
    @Override public PqMap getMap(String name) { try { return dataView.getMap(name); } catch (RuntimeException e) { throw wrapException(e); } }
    @Override public PqMap getMap(int i) { try { return dataView.getMap(i); } catch (RuntimeException e) { throw wrapException(e); } }

    // ==================== Metadata ====================

    @Override
    public int getFieldCount() {
        return dataView.getFieldCount();
    }

    @Override
    public String getFieldName(int index) {
        return dataView.getFieldName(index);
    }

    // ==================== Internal ====================

    private RuntimeException wrapException(RuntimeException e) {
        return ExceptionContext.addFileContext(currentFileName, e);
    }

    // ==================== Close ====================

    @Override
    public void close() {
        if (columnWorkers != null) {
            for (NestedColumnWorker worker : columnWorkers) {
                worker.close();
            }
        }
        for (int i = 0; i < columnCount; i++) {
            if (previousBatches[i] != null) {
                exchanges[i].freeQueue().offer(previousBatches[i]);
                previousBatches[i] = null;
            }
            NestedBatch leftover;
            while ((leftover = exchanges[i].readyQueue().poll()) != null) {
                exchanges[i].freeQueue().offer(leftover);
            }
        }
    }
}
