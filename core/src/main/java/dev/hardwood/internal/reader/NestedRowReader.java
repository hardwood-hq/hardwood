/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.NoSuchElementException;
import java.util.UUID;

import dev.hardwood.internal.predicate.RecordFilterCompiler;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.predicate.RowMatcher;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.internal.schema.ReadProjection;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqInterval;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
import dev.hardwood.row.PqVariant;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

/// Row reader for nested schemas using the v3 pipeline.
///
/// Consumes [NestedBatch] objects with nested fields (definition levels,
/// repetition levels, record offsets) and delegates typed accessors to
/// [NestedBatchDataView]. Index structures are pre-computed by the
/// [NestedColumnWorker] drain thread before publishing.
public final class NestedRowReader implements FileAwareRowReader {

    private final BatchExchange<NestedBatch>[] exchanges;
    private final NestedColumnWorker[] columnWorkers;
    private final int columnCount;

    private final FileSchema fileSchema;
    /// Backs the accessors. It spans the payload projection alone, so a predicate column
    /// outside the projection is not reachable through it at any depth.
    private final NestedBatchDataView dataView;
    /// The payload columns' schemas and current batches: the leading
    /// [ReadProjection#payloadColumnCount()] of the decoded ones.
    private final ColumnSchema[] payloadSchemas;
    private final NestedBatch[] payloadBatches;
    /// Per-file record-filter counts for JFR, or `null` when the read has no
    /// filter and nothing evaluates records.
    private final RecordFilterTally tally;

    /// The iterator this reader consumes. No column reader owns it — they all
    /// draw from the one iterator — so this reader releases it, which is also
    /// what stops the owning [dev.hardwood.reader.ParquetFileReader] tracking it.
    private final RowGroupIterator rowGroupIterator;

    // Iteration state
    private NestedBatch[] previousBatches;
    /// File name from the current batch — used for exception enrichment
    private String currentFileName;
    /// Whether statistics proved every row of the batch being served matches the filter,
    /// so [#recordMatcher] need not be run over it.
    private boolean currentRowsAlwaysMatch;
    /// The matcher in force for the batch being served: [#recordMatcher], or `null` when
    /// statistics decided the batch and there is no cap to count matches against. A
    /// proven batch is then an unfiltered batch, served by the plain cursor with no
    /// per-row state to consult — which is the whole of what the proof is worth.
    private RowMatcher activeMatcher;

    /// Record-level predicate, or `null` when the read has no filter. Present or absent
    /// purely by whether the caller asked for a filter — never by what a file holds.
    private final RowMatcher recordMatcher;
    /// What [#recordMatcher] is tested against: the predicate columns of the current batch.
    /// Refreshed only for a batch the matcher evaluates, since a predicate column outside the
    /// projection is not decoded for a batch statistics proved. `null` exactly when
    /// [#recordMatcher] is.
    private final PredicateView predicateView;
    /// Cap on matching rows yielded (SQL LIMIT over the filtered relation);
    /// [ColumnWorker#UNLIMITED] means no cap.
    private final long maxMatchedRows;
    private long matchedRowsYielded;
    /// Row [#hasNext] picked for [#next] to hand out, or `-1` when none is parked.
    /// Only the record-matcher path parks; without a filter the cursor advances in `next`.
    private int pendingRowIndex = -1;
    private int rowIndex = -1;
    private int batchSize = 0;
    private boolean exhausted;
    private boolean closed;

    NestedRowReader(BatchExchange<NestedBatch>[] exchanges, NestedColumnWorker[] columnWorkers,
                    FileSchema fileSchema, ProjectedSchema payload,
                    long maxMatchedRows, RowMatcher recordMatcher, PredicateView predicateView,
                    RecordFilterTally tally, RowGroupIterator rowGroupIterator) {
        this.rowGroupIterator = rowGroupIterator;
        this.maxMatchedRows = maxMatchedRows;
        this.recordMatcher = recordMatcher;
        this.predicateView = predicateView;
        this.tally = tally;
        this.exchanges = exchanges;
        this.columnWorkers = columnWorkers;
        this.columnCount = exchanges.length;
        this.fileSchema = fileSchema;
        this.dataView = new NestedBatchDataView(fileSchema, payload);
        this.previousBatches = new NestedBatch[columnCount];

        int payloadCount = payload.getProjectedColumnCount();
        this.payloadSchemas = new ColumnSchema[payloadCount];
        this.payloadBatches = new NestedBatch[payloadCount];
        for (int i = 0; i < payloadCount; i++) {
            payloadSchemas[i] = fileSchema.getColumn(payload.toOriginalIndex(i));
        }
    }

    /// Eagerly loads the first batch. Must be called after construction.
    void initialize() throws IOException {
        if (!loadNextBatch()) {
            exhausted = true;
        }
    }

    // ==================== Factory ====================

    /// Creates a nested v3 pipeline and returns a [RowReader].
    ///
    /// Wires up `RowGroupIterator → PageSource → NestedColumnWorker → BatchExchange →
    /// NestedRowReader`, starts all column workers, and initializes the reader.
    /// When a filter is present, the reader evaluates it a record at a time.
    ///
    /// @param rowGroupIterator pre-configured iterator
    /// @param schema the file schema
    /// @param projection the exposed and decoded columns
    /// @param context the hardwood context
    /// @param fixedListFastPathEnabled whether the fixed-size-list read fast path may engage
    /// @param filter resolved predicate, or `null` for no filtering
    /// @param maxRows maximum rows (0 = unlimited). Without a filter this caps scanned
    ///                rows at the [ColumnWorker] drain. With a filter it caps *matching*
    ///                rows (SQL LIMIT): the drain holds it over the row groups statistics
    ///                prove match in full, and the reader counts matches from the first
    ///                row group they do not.
    /// @param batchSize records per batch, already resolved by the caller
    /// @return a [NestedRowReader]
    public static RowReader create(RowGroupIterator rowGroupIterator,
                            FileSchema schema,
                            ReadProjection projection,
                            HardwoodContextImpl context,
                            boolean fixedListFastPathEnabled,
                            ResolvedPredicate filter,
                            long maxRows,
                            int batchSize) throws IOException {
        ProjectedSchema decoded = projection.decoded();
        int projectedColumnCount = decoded.getProjectedColumnCount();
        // With a row-level filter, `maxRows` caps *matching* rows (SQL LIMIT). The
        // workers still take it — they hold it only while statistics prove every row
        // they assemble matches, and drop it at the first row group that is not proven,
        // from where the reader counts matches instead.
        //
        // Nothing here asks a question about the read as a whole, so nothing here plans
        // beyond the first file: statistics reach the reader per row group, on
        // NestedBatch.filterAlwaysMatches (see #1107).
        NestedColumnWorker[] workers = new NestedColumnWorker[projectedColumnCount];
        @SuppressWarnings("unchecked")
        BatchExchange<NestedBatch>[] buffers = new BatchExchange[projectedColumnCount];

        for (int i = 0; i < projectedColumnCount; i++) {
            int originalIndex = decoded.toOriginalIndex(i);
            ColumnSchema columnSchema = schema.getColumn(originalIndex);

            PageSource pageSource = new PageSource(rowGroupIterator, i);

            BatchExchange<NestedBatch> buffer = BatchExchange.recycling(
                    columnSchema.name(), () -> {
                        NestedBatch b = new NestedBatch();
                        b.values = BatchExchange.allocateArray(columnSchema, batchSize);
                        return b;
                    });
            NestedLevelComputer.Layers layers = NestedLevelComputer.computeLayers(
                    schema.getRootNode(), columnSchema.columnIndex());
            NestedColumnWorker worker = new NestedColumnWorker(
                    pageSource, buffer, columnSchema, batchSize,
                    context.decompressorFactory(), context.executor(), maxRows,
                    layers, NestedColumnWorker.IndexMode.ALL_ITEMS, fixedListFastPathEnabled);

            buffers[i] = buffer;
            workers[i] = worker;
            worker.start();
        }

        RecordFilterTally tally = filter != null ? new RecordFilterTally() : null;
        // The matcher is tested against a view of the predicate columns rather than the
        // reader itself, whose accessors reach the projected columns alone.
        PredicateView predicateView = filter != null
                ? PredicateView.create(schema, decoded, filter, p -> true, false)
                : null;
        RowMatcher recordMatcher = predicateView != null
                ? RecordFilterCompiler.compile(filter, schema, predicateView::indexOf)
                : null;
        long readerMatchLimit = filter != null ? maxRows : ColumnWorker.UNLIMITED;
        NestedRowReader reader = new NestedRowReader(buffers, workers, schema, projection.payload(),
                readerMatchLimit, recordMatcher, predicateView, tally, rowGroupIterator);
        reader.initialize();
        return reader;
    }

    // ==================== Iteration ====================

    @Override
    public boolean hasNext() throws IOException {
        if (exhausted) {
            return false;
        }
        if (activeMatcher != null) {
            return hasNextMatching();
        }
        if (rowIndex + 1 < batchSize) {
            return true;
        }
        return loadAndDecide();
    }

    /// Loads the next batch and says whether it yields a row. Kept out of [#hasNext] so
    /// that method stays loop-free and small enough to inline into a caller's row loop,
    /// which is worth more than the call this costs once per batch.
    private boolean loadAndDecide() throws IOException {
        if (!loadNextBatch()) {
            return false;
        }
        return activeMatcher == null || hasNextMatching();
    }

    /// Advances to the next record the matcher accepts, evaluating one at a time.
    /// Returns to [#hasNext]'s plain cursor as soon as a batch loads that statistics
    /// decided, so a proven batch never pays for the per-row protocol.
    private boolean hasNextMatching() throws IOException {
        if (maxMatchedRows != ColumnWorker.UNLIMITED && matchedRowsYielded >= maxMatchedRows) {
            exhausted = true;
            return false;
        }
        if (pendingRowIndex >= 0) {
            return true;
        }
        while (true) {
            if (rowIndex + 1 >= batchSize) {
                if (!loadNextBatch()) {
                    return false;
                }
                if (activeMatcher == null) {
                    return true;
                }
            }
            rowIndex++;
            // Only the predicate view is positioned here; `next()` positions the payload
            // view for the row it commits, so a rejected row costs it nothing.
            // Statistics already decided this batch's row group in full — reachable only
            // under a cap, which needs every match counted as it goes. The view is not
            // positioned then: it was not refreshed for this batch.
            boolean matched = currentRowsAlwaysMatch || matchesAt(rowIndex);
            tally.record(matched);
            if (matched) {
                pendingRowIndex = rowIndex;
                return true;
            }
        }
    }

    private boolean matchesAt(int row) {
        predicateView.setRecord(row);
        return activeMatcher.test(predicateView);
    }

    @Override
    public void next() throws IOException {
        // hasNext parks the row it picked in `pendingRowIndex`; this only commits it.
        if (activeMatcher != null) {
            if (pendingRowIndex < 0) {
                throw new NoSuchElementException("No matching row available. Call hasNext() first.");
            }
            rowIndex = pendingRowIndex;
            pendingRowIndex = -1;
            dataView.setRowIndex(rowIndex);
            matchedRowsYielded++;
            return;
        }
        // Fail early on an unguarded next() past the batch rather than letting
        // rowIndex point into the capacity tail and expose phantom/stale rows.
        // After any hasNext() == true this check never trips (a freshly loaded
        // batch resets rowIndex to -1).
        if (rowIndex + 1 >= batchSize) {
            throw new NoSuchElementException("No row available. Call hasNext() first.");
        }
        rowIndex++;
        dataView.setRowIndex(rowIndex);
    }

    // ==================== Batch Loading ====================

    private boolean loadNextBatch() throws IOException {
        // Poll columns sequentially with manual recycling and error checking.
        // Each poll is non-blocking when its exchange has a batch ready; the
        // pipeline runs ahead of the consumer in steady state, so the per-call
        // cost is dominated by the first non-blocking readyQueue.poll().
        NestedBatch[] batches = new NestedBatch[columnCount];
        for (int i = 0; i < columnCount; i++) {
            if (previousBatches[i] != null) {
                exchanges[i].recycle(previousBatches[i]);
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
                            "[" + batches[0].fileName + "] "
                            + "Column count mismatch: column " + i + " produced no data"
                            + " while earlier columns had " + batches[0].recordCount + " records");
                }
                exhausted = true;
                return false;
            }
            batches[i] = batch;
            previousBatches[i] = batch;
        }

        batchSize = batches[0].recordCount;

        // Index structures are pre-computed by the drain — just assemble the view
        currentFileName = batches[0].fileName;
        // Uniform across columns: the flag comes from the work item, and the workers
        // flush on its transitions, so no batch mixes the two.
        currentRowsAlwaysMatch = batches[0].filterAlwaysMatches;
        // A batch statistics decided, with no cap to count matches against, needs
        // nothing evaluated and nothing counted per row: hand it to the plain cursor
        // and tally it whole.
        activeMatcher = currentRowsAlwaysMatch && maxMatchedRows == ColumnWorker.UNLIMITED
                ? null : recordMatcher;
        if (tally != null) {
            // Ahead of any record of this batch being counted, so the counts land
            // on the file the batch came from. Batches never straddle files.
            tally.switchFile(currentFileName);
            if (activeMatcher == null) {
                // Statistics decided this batch and there is no cap, so no row of it is
                // evaluated or counted individually: count it whole.
                tally.recordBatch(batchSize, batchSize);
            }
        }
        System.arraycopy(batches, 0, payloadBatches, 0, payloadBatches.length);
        dataView.setBatchData(payloadBatches, payloadSchemas, currentFileName);
        if (predicateView != null && !currentRowsAlwaysMatch) {
            predicateView.refresh(null, batches, currentFileName);
        }
        rowIndex = -1;
        return true;
    }

    // ==================== Accessors (delegate to NestedBatchDataView) ====================

    @Override public String currentFileName() { return currentFileName; }

    @Override public boolean isNull(int i) { return dataView.isNull(i); }
    @Override public boolean isNull(String name) { return dataView.isNull(name); }

    @Override public int getInt(int i) { return dataView.getInt(i); }
    @Override public int getInt(String name) { return dataView.getInt(name); }
    @Override public long getLong(int i) { return dataView.getLong(i); }
    @Override public long getLong(String name) { return dataView.getLong(name); }
    @Override public float getFloat(int i) { return dataView.getFloat(i); }
    @Override public float getFloat(String name) { return dataView.getFloat(name); }
    @Override public double getDouble(int i) { return dataView.getDouble(i); }
    @Override public double getDouble(String name) { return dataView.getDouble(name); }
    @Override public boolean getBoolean(int i) { return dataView.getBoolean(i); }
    @Override public boolean getBoolean(String name) { return dataView.getBoolean(name); }

    @Override public String getString(int i) { return dataView.getString(i); }
    @Override public String getString(String name) { return dataView.getString(name); }
    @Override public byte[] getBinary(int i) { return dataView.getBinary(i); }
    @Override public byte[] getBinary(String name) { return dataView.getBinary(name); }
    @Override public LocalDate getDate(int i) { return dataView.getDate(i); }
    @Override public LocalDate getDate(String name) { return dataView.getDate(name); }
    @Override public LocalTime getTime(int i) { return dataView.getTime(i); }
    @Override public LocalTime getTime(String name) { return dataView.getTime(name); }
    @Override public Instant getTimestamp(int i) { return dataView.getTimestamp(i); }
    @Override public Instant getTimestamp(String name) { return dataView.getTimestamp(name); }
    @Override public LocalDateTime getLocalTimestamp(int i) { return dataView.getLocalTimestamp(i); }
    @Override public LocalDateTime getLocalTimestamp(String name) { return dataView.getLocalTimestamp(name); }
    @Override public BigDecimal getDecimal(int i) { return dataView.getDecimal(i); }
    @Override public BigDecimal getDecimal(String name) { return dataView.getDecimal(name); }
    @Override public UUID getUuid(int i) { return dataView.getUuid(i); }
    @Override public UUID getUuid(String name) { return dataView.getUuid(name); }

    @Override public PqInterval getInterval(int i) { return dataView.getInterval(i); }
    @Override public PqInterval getInterval(String name) { return dataView.getInterval(name); }

    @Override public Object getValue(int i) { return dataView.getValue(i); }
    @Override public Object getValue(String name) { return dataView.getValue(name); }
    @Override public Object getRawValue(int i) { return dataView.getRawValue(i); }
    @Override public Object getRawValue(String name) { return dataView.getRawValue(name); }

    @Override public PqStruct getStruct(String name) { return dataView.getStruct(name); }
    @Override public PqStruct getStruct(int i) { return dataView.getStruct(i); }
    @Override public PqList getList(String name) { return dataView.getList(name); }
    @Override public PqList getList(int i) { return dataView.getList(i); }
    @Override public PqMap getMap(String name) { return dataView.getMap(name); }
    @Override public PqMap getMap(int i) { return dataView.getMap(i); }
    @Override public PqVariant getVariant(String name) { return dataView.getVariant(name); }
    @Override public PqVariant getVariant(int i) { return dataView.getVariant(i); }

    // ==================== Metadata ====================

    @Override
    public int getFieldCount() {
        return dataView.getFieldCount();
    }

    @Override
    public String getFieldName(int index) {
        return dataView.getFieldName(index);
    }

    // ==================== Close ====================

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        // The iterator is released even when tearing the pipeline down fails: this
        // reader will not run its close body again, so skipping the release would
        // leave the work list reachable for the parent reader's whole lifetime. A
        // failure to release is suppressed beneath the original.
        try (rowGroupIterator) {
            if (tally != null) {
                tally.close();
            }
            if (columnWorkers != null) {
                for (NestedColumnWorker worker : columnWorkers) {
                    worker.close();
                }
            }
            for (int i = 0; i < columnCount; i++) {
                if (previousBatches[i] != null) {
                    exchanges[i].recycle(previousBatches[i]);
                    previousBatches[i] = null;
                }
                exchanges[i].drainReady();
            }
        }
    }

}
