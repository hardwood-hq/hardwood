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
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import dev.morling.hardwood.internal.reader.ColumnValueIterator;
import dev.morling.hardwood.internal.reader.PageCursor;
import dev.morling.hardwood.internal.reader.PageInfo;
import dev.morling.hardwood.internal.reader.PageScanner;
import dev.morling.hardwood.internal.reader.TypedColumnData;
import dev.morling.hardwood.metadata.ColumnChunk;
import dev.morling.hardwood.metadata.RowGroup;
import dev.morling.hardwood.schema.ColumnSchema;
import dev.morling.hardwood.schema.FileSchema;

/**
 * Base class for RowReader implementations providing common batch loading logic.
 */
abstract class AbstractRowReader implements RowReader {

    private static final System.Logger LOG = System.getLogger(AbstractRowReader.class.getName());
    private static final int BATCH_SIZE = 8192;

    protected final FileSchema schema;
    private final FileChannel channel;
    private final List<RowGroup> rowGroups;
    private final ExecutorService executor;
    private final String fileName;

    private ColumnValueIterator[] iterators;

    protected int rowIndex = -1;  // -1 means next() not yet called
    private int batchSize = 0;
    private boolean exhausted = false;
    private volatile boolean closed;
    private boolean initialized = false;

    protected AbstractRowReader(FileSchema schema, FileChannel channel, List<RowGroup> rowGroups,
                                ExecutorService executor, String fileName) {
        this.schema = schema;
        this.channel = channel;
        this.rowGroups = rowGroups;
        this.executor = executor;
        this.fileName = fileName;
    }

    private void initialize() {
        if (initialized) {
            return;
        }
        initialized = true;

        LOG.log(System.Logger.Level.DEBUG, "Starting to parse file ''{0}'' with {1} row groups, {2} columns",
                fileName, rowGroups.size(), schema.getColumnCount());

        int columnCount = schema.getColumnCount();

        // Collect page infos for each column across all row groups
        List<List<PageInfo>> pageInfosByColumn = new ArrayList<>(columnCount);
        for (int i = 0; i < columnCount; i++) {
            pageInfosByColumn.add(new ArrayList<>());
        }

        LOG.log(System.Logger.Level.DEBUG, "Scanning pages for {0} columns across {1} row groups",
                columnCount, rowGroups.size());

        // Scan each column in parallel
        @SuppressWarnings("unchecked")
        CompletableFuture<List<PageInfo>>[] scanFutures = new CompletableFuture[columnCount];

        for (int colIndex = 0; colIndex < columnCount; colIndex++) {
            final int col = colIndex;
            final ColumnSchema columnSchema = schema.getColumn(col);

            scanFutures[col] = CompletableFuture.supplyAsync(() -> {
                List<PageInfo> columnPages = new ArrayList<>();
                for (RowGroup rowGroup : rowGroups) {
                    ColumnChunk columnChunk = rowGroup.columns().get(col);
                    PageScanner scanner = new PageScanner(channel, columnSchema, columnChunk);
                    try {
                        columnPages.addAll(scanner.scanPages());
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException("Failed to scan pages for column " + columnSchema.name(), e);
                    }
                }
                return columnPages;
            }, executor);
        }

        // Wait for all scans to complete and collect results
        CompletableFuture.allOf(scanFutures).join();

        for (int colIndex = 0; colIndex < columnCount; colIndex++) {
            pageInfosByColumn.get(colIndex).addAll(scanFutures[colIndex].join());
        }

        int totalPages = pageInfosByColumn.stream().mapToInt(List::size).sum();
        LOG.log(System.Logger.Level.DEBUG, "Page scanning complete: {0} total pages across {1} columns",
                totalPages, columnCount);

        // Create iterators for each column
        iterators = new ColumnValueIterator[columnCount];
        for (int i = 0; i < columnCount; i++) {
            PageCursor pageCursor = new PageCursor(pageInfosByColumn.get(i), executor);
            iterators[i] = new ColumnValueIterator(pageCursor, schema.getColumn(i), executor, schema.isFlatSchema());
        }

        onInitialize();

        // Eagerly load first batch
        loadNextBatch();
    }

    /**
     * Called during initialization. Subclasses can override to perform additional setup.
     */
    protected void onInitialize() {
        // Default: do nothing
    }

    @Override
    public boolean hasNext() {
        initialize();

        if (closed || exhausted) {
            return false;
        }
        if (rowIndex + 1 < batchSize) {
            return true;
        }
        return loadNextBatch();
    }

    @Override
    public void next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more rows available");
        }
        rowIndex++;
        onNext();
    }

    /**
     * Called after advancing to the next row. Subclasses can override to perform row-specific setup.
     */
    protected void onNext() {
        // Default: do nothing
    }

    @Override
    public void close() {
        closed = true;
    }

    @SuppressWarnings("unchecked")
    private boolean loadNextBatch() {
        CompletableFuture<TypedColumnData>[] futures = new CompletableFuture[iterators.length];
        for (int i = 0; i < iterators.length; i++) {
            final int col = i;
            futures[i] = CompletableFuture.supplyAsync(() -> iterators[col].prefetch(BATCH_SIZE), executor);
        }

        CompletableFuture.allOf(futures).join();

        TypedColumnData[] newColumnData = new TypedColumnData[iterators.length];
        for (int i = 0; i < iterators.length; i++) {
            newColumnData[i] = futures[i].join();
            if (newColumnData[i].recordCount() == 0) {
                exhausted = true;
                return false;
            }
        }

        onBatchLoaded(newColumnData);

        batchSize = newColumnData[0].recordCount();
        rowIndex = -1;  // Reset to -1 so next() increments to 0
        return batchSize > 0;
    }

    /**
     * Called when a new batch of column data is loaded. Subclasses must implement to store the data.
     */
    protected abstract void onBatchLoaded(TypedColumnData[] columnData);
}
