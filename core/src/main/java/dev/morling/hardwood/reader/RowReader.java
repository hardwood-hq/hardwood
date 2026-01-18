/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.reader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import dev.morling.hardwood.internal.reader.ColumnBatch;
import dev.morling.hardwood.internal.reader.MutableStruct;
import dev.morling.hardwood.internal.reader.PqRowImpl;
import dev.morling.hardwood.internal.reader.RecordAssembler;
import dev.morling.hardwood.metadata.RowGroup;
import dev.morling.hardwood.row.PqRow;
import dev.morling.hardwood.schema.FileSchema;

/**
 * Provides row-oriented iteration over a Parquet file.
 * Internally uses parallel batch fetching from column readers for performance.
 * Supports reading across multiple row groups.
 */
public class RowReader implements Iterable<PqRow>, AutoCloseable {

    private static final int DEFAULT_BATCH_SIZE = 5000;

    private final FileSchema schema;
    private final FileChannel channel;
    private final List<RowGroup> rowGroups;
    private final ExecutorService executor;
    private final int batchSize;
    private final long totalRows;

    // Current row group state
    private int currentRowGroupIndex;
    private List<ColumnReader> currentColumnReaders;

    // Current batch state
    private List<ColumnBatch> currentBatches;
    private int currentBatchRecordsRead;
    private long totalRowsRead;
    private boolean closed;

    /**
     * Create a RowReader with default batch size.
     */
    public RowReader(FileSchema schema,
                     FileChannel channel,
                     List<RowGroup> rowGroups,
                     ExecutorService executor,
                     long totalRows) {
        this(schema, channel, rowGroups, executor, totalRows, DEFAULT_BATCH_SIZE);
    }

    /**
     * Create a RowReader with custom batch size.
     */
    public RowReader(FileSchema schema,
                     FileChannel channel,
                     List<RowGroup> rowGroups,
                     ExecutorService executor,
                     long totalRows,
                     int batchSize) {
        this.schema = schema;
        this.channel = channel;
        this.rowGroups = rowGroups;
        this.executor = executor;
        this.totalRows = totalRows;
        this.batchSize = batchSize;
        this.currentRowGroupIndex = 0;
        this.currentColumnReaders = null;
        this.currentBatches = null;
        this.currentBatchRecordsRead = 0;
        this.totalRowsRead = 0;
        this.closed = false;
    }

    /**
     * Initialize column readers for the current row group.
     */
    private void initializeCurrentRowGroupReaders() {
        if (currentRowGroupIndex >= rowGroups.size()) {
            currentColumnReaders = null;
            return;
        }

        RowGroup rowGroup = rowGroups.get(currentRowGroupIndex);
        currentColumnReaders = new ArrayList<>();

        try {
            for (int i = 0; i < schema.getColumnCount(); i++) {
                currentColumnReaders.add(new ColumnReader(channel, schema.getColumn(i), rowGroup.columns().get(i)));
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Iterator<PqRow> iterator() {
        return new PqRowIterator();
    }

    /**
     * Fetch the next batch of values from all columns in parallel.
     * Automatically moves to the next row group when current one is exhausted.
     */
    private void fetchNextBatch() throws IOException {
        if (closed) {
            throw new IllegalStateException("RowReader is closed");
        }

        // Check if we need to move to the next row group
        if (currentColumnReaders == null || !currentColumnReaders.get(0).hasNext()) {
            currentRowGroupIndex++;
            if (currentRowGroupIndex >= rowGroups.size()) {
                // No more row groups
                currentBatches = null;
                return;
            }
            initializeCurrentRowGroupReaders();
        }

        // Create futures for parallel batch fetching (always use raw mode)
        List<CompletableFuture<ColumnBatch>> futures = new ArrayList<>();
        for (ColumnReader reader : currentColumnReaders) {
            CompletableFuture<ColumnBatch> future = CompletableFuture.supplyAsync(() -> {
                try {
                    return reader.readBatch(batchSize);
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }, executor);
            futures.add(future);
        }

        // Wait for all futures to complete
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        }
        catch (Exception e) {
            if (e.getCause() instanceof UncheckedIOException) {
                throw ((UncheckedIOException) e.getCause()).getCause();
            }
            throw new IOException("Failed to fetch column batches", e);
        }

        // Collect results
        currentBatches = new ArrayList<>();
        for (CompletableFuture<ColumnBatch> future : futures) {
            try {
                currentBatches.add(future.get());
            }
            catch (Exception e) {
                if (e.getCause() instanceof UncheckedIOException) {
                    throw ((UncheckedIOException) e.getCause()).getCause();
                }
                throw new IOException("Failed to get column batch", e);
            }
        }

        currentBatchRecordsRead = 0;
    }

    @Override
    public void close() {
        closed = true;
        // Note: We don't shut down the executor here as it may be shared
        // The caller (ParquetFileReader) is responsible for managing the executor lifecycle
    }

    /**
     * Iterator implementation for type-safe PqRow access.
     */
    private class PqRowIterator implements Iterator<PqRow> {

        private final RecordAssembler assembler = new RecordAssembler(schema);

        @Override
        public boolean hasNext() {
            if (closed || totalRowsRead >= totalRows) {
                return false;
            }

            // Initialize first row group if needed
            if (currentColumnReaders == null && currentRowGroupIndex == 0) {
                initializeCurrentRowGroupReaders();
            }

            // Check if we need to fetch a new batch
            if (currentBatches == null ||
                    (currentBatchRecordsRead >= currentBatches.get(0).size() && totalRowsRead < totalRows)) {
                try {
                    fetchNextBatch();
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            return currentBatches != null &&
                    !currentBatches.isEmpty() &&
                    currentBatchRecordsRead < currentBatches.get(0).size();
        }

        @Override
        public PqRow next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more rows available");
            }

            // Advance all batches to the next record
            for (ColumnBatch batch : currentBatches) {
                batch.nextRecord();
            }

            MutableStruct rowValues = assembler.assembleRow(currentBatches);

            currentBatchRecordsRead++;
            totalRowsRead++;

            return new PqRowImpl(rowValues, schema);
        }
    }
}
