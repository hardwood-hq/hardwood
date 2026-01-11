/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.reader;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import dev.morling.hardwood.internal.reader.ColumnBatch;
import dev.morling.hardwood.internal.reader.PqRowImpl;
import dev.morling.hardwood.internal.reader.RecordAssembler;
import dev.morling.hardwood.internal.reader.SimpleColumnBatch;
import dev.morling.hardwood.metadata.RowGroup;
import dev.morling.hardwood.row.PqRow;
import dev.morling.hardwood.schema.FileSchema;
import dev.morling.hardwood.schema.SchemaNode;

/**
 * Provides row-oriented iteration over a Parquet file.
 * Internally uses parallel batch fetching from column readers for performance.
 * Supports reading across multiple row groups.
 */
public class RowReader implements Iterable<PqRow>, AutoCloseable {

    private static final int DEFAULT_BATCH_SIZE = 5000;

    private final FileSchema schema;
    private final RandomAccessFile file;
    private final List<RowGroup> rowGroups;
    private final ExecutorService executor;
    private final int batchSize;
    private final long totalRows;

    // Current row group state
    private int currentRowGroupIndex;
    private List<ColumnReader> currentColumnReaders;

    // Current batch state
    private List<ColumnBatch> currentBatches;
    private int currentBatchPosition;
    private long totalRowsRead;
    private boolean closed;

    /**
     * Create a RowReader with default batch size.
     */
    public RowReader(FileSchema schema,
                     RandomAccessFile file,
                     List<RowGroup> rowGroups,
                     ExecutorService executor,
                     long totalRows) {
        this(schema, file, rowGroups, executor, totalRows, DEFAULT_BATCH_SIZE);
    }

    /**
     * Create a RowReader with custom batch size.
     */
    public RowReader(FileSchema schema,
                     RandomAccessFile file,
                     List<RowGroup> rowGroups,
                     ExecutorService executor,
                     long totalRows,
                     int batchSize) {
        this.schema = schema;
        this.file = file;
        this.rowGroups = rowGroups;
        this.executor = executor;
        this.totalRows = totalRows;
        this.batchSize = batchSize;
        this.currentRowGroupIndex = 0;
        this.currentColumnReaders = null;
        this.currentBatches = null;
        this.currentBatchPosition = 0;
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
                currentColumnReaders.add(new ColumnReader(file, schema.getColumn(i), rowGroup.columns().get(i)));
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

        // Create futures for parallel batch fetching
        List<CompletableFuture<ColumnBatch>> futures = new ArrayList<>();
        for (int i = 0; i < currentColumnReaders.size(); i++) {
            ColumnReader reader = currentColumnReaders.get(i);
            // Use raw mode for columns in list-of-struct to enable multi-column assembly
            boolean rawMode = schema.isColumnInListOfStruct(i);
            CompletableFuture<ColumnBatch> future = CompletableFuture.supplyAsync(() -> {
                try {
                    return reader.readBatch(batchSize, rawMode);
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

        currentBatchPosition = 0;
    }

    @Override
    public void close() {
        closed = true;
        // Note: We don't shut down the executor here as it may be shared
        // The caller (ParquetFileReader) is responsible for managing the executor lifecycle
    }

    /**
     * Check if schema has nested fields (structs or lists).
     */
    private boolean hasNestedFields() {
        for (SchemaNode child : schema.getRootNode().children()) {
            if (child instanceof SchemaNode.GroupNode) {
                return true;
            }
        }
        return false;
    }

    /**
     * Iterator implementation for type-safe PqRow access.
     */
    private class PqRowIterator implements Iterator<PqRow> {

        @Override
        public boolean hasNext() {
            if (closed) {
                return false;
            }

            // Check if we've read all rows
            if (totalRowsRead >= totalRows) {
                return false;
            }

            // Initialize first row group if needed
            if (currentColumnReaders == null && currentRowGroupIndex == 0) {
                initializeCurrentRowGroupReaders();
            }

            // Check if we need to fetch a new batch
            if (currentBatches == null ||
                    (currentBatchPosition >= currentBatches.get(0).size() && totalRowsRead < totalRows)) {
                try {
                    fetchNextBatch();
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            // Check if the current batch has more rows
            return currentBatches != null &&
                    currentBatches.size() > 0 &&
                    currentBatches.get(0).size() > 0 &&
                    currentBatchPosition < currentBatches.get(0).size();
        }

        @Override
        public PqRow next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more rows available");
            }

            Object[] rowValues;

            // Check if this is a nested schema (has struct or list fields)
            if (hasNestedFields()) {
                // Use RecordAssembler for nested schemas
                RecordAssembler assembler = new RecordAssembler(schema);
                rowValues = assembler.assembleRow(currentBatches, currentBatchPosition);
            }
            else {
                // Simple flat schema - direct assembly
                rowValues = new Object[currentColumnReaders.size()];
                for (int i = 0; i < currentColumnReaders.size(); i++) {
                    rowValues[i] = ((SimpleColumnBatch) currentBatches.get(i)).get(currentBatchPosition);
                }
            }

            currentBatchPosition++;
            totalRowsRead++;

            return new PqRowImpl(rowValues, schema);
        }
    }
}
