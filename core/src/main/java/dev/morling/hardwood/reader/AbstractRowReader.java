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
import java.util.concurrent.ForkJoinPool;

import dev.morling.hardwood.internal.reader.ColumnValueIterator;
import dev.morling.hardwood.internal.reader.PageCursor;
import dev.morling.hardwood.internal.reader.PageInfo;
import dev.morling.hardwood.internal.reader.PageScanner;
import dev.morling.hardwood.internal.reader.TypedColumnData;
import dev.morling.hardwood.metadata.ColumnChunk;
import dev.morling.hardwood.metadata.RowGroup;
import dev.morling.hardwood.schema.ColumnSchema;
import dev.morling.hardwood.schema.FileSchema;
import dev.morling.hardwood.schema.ProjectedSchema;

/**
 * Base class for RowReader implementations providing common batch loading logic.
 */
abstract class AbstractRowReader implements RowReader {

    private static final System.Logger LOG = System.getLogger(AbstractRowReader.class.getName());
    private static final int BATCH_SIZE = 16384;

    protected final FileSchema schema;
    protected final ProjectedSchema projectedSchema;
    private final FileChannel channel;
    private final List<RowGroup> rowGroups;
    private final HardwoodContext context;
    private final String fileName;

    private ColumnValueIterator[] iterators;

    protected int rowIndex = -1;  // -1 means next() not yet called
    private int batchSize = 0;
    private boolean exhausted = false;
    private volatile boolean closed;
    private boolean initialized = false;

    protected AbstractRowReader(FileSchema schema, ProjectedSchema projectedSchema, FileChannel channel,
                                List<RowGroup> rowGroups, HardwoodContext context, String fileName) {
        this.schema = schema;
        this.projectedSchema = projectedSchema;
        this.channel = channel;
        this.rowGroups = rowGroups;
        this.context = context;
        this.fileName = fileName;
    }

    private void initialize() {
        if (initialized) {
            return;
        }
        initialized = true;

        int projectedColumnCount = projectedSchema.getProjectedColumnCount();

        LOG.log(System.Logger.Level.DEBUG, "Starting to parse file ''{0}'' with {1} row groups, {2} projected columns (of {3} total)",
                fileName, rowGroups.size(), projectedColumnCount, schema.getColumnCount());

        // Collect page infos for each projected column across all row groups
        List<List<PageInfo>> pageInfosByColumn = new ArrayList<>(projectedColumnCount);
        for (int i = 0; i < projectedColumnCount; i++) {
            pageInfosByColumn.add(new ArrayList<>());
        }

        LOG.log(System.Logger.Level.DEBUG, "Scanning pages for {0} projected columns across {1} row groups",
                projectedColumnCount, rowGroups.size());

        // Scan each projected column in parallel
        @SuppressWarnings("unchecked")
        CompletableFuture<List<PageInfo>>[] scanFutures = new CompletableFuture[projectedColumnCount];

        for (int projectedIndex = 0; projectedIndex < projectedColumnCount; projectedIndex++) {
            final int projIdx = projectedIndex;
            final int originalIndex = projectedSchema.toOriginalIndex(projectedIndex);
            final ColumnSchema columnSchema = schema.getColumn(originalIndex);

            scanFutures[projIdx] = CompletableFuture.supplyAsync(() -> {
                List<PageInfo> columnPages = new ArrayList<>();
                for (RowGroup rowGroup : rowGroups) {
                    ColumnChunk columnChunk = rowGroup.columns().get(originalIndex);
                    PageScanner scanner = new PageScanner(channel, columnSchema, columnChunk, context);
                    try {
                        columnPages.addAll(scanner.scanPages());
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException("Failed to scan pages for column " + columnSchema.name(), e);
                    }
                }
                return columnPages;
            }, context.executor());
        }

        // Wait for all scans to complete and collect results
        CompletableFuture.allOf(scanFutures).join();

        for (int projectedIndex = 0; projectedIndex < projectedColumnCount; projectedIndex++) {
            pageInfosByColumn.get(projectedIndex).addAll(scanFutures[projectedIndex].join());
        }

        int totalPages = pageInfosByColumn.stream().mapToInt(List::size).sum();
        LOG.log(System.Logger.Level.DEBUG, "Page scanning complete: {0} total pages across {1} projected columns",
                totalPages, projectedColumnCount);

        // Create iterators for each projected column
        iterators = new ColumnValueIterator[projectedColumnCount];
        for (int i = 0; i < projectedColumnCount; i++) {
            int originalIndex = projectedSchema.toOriginalIndex(i);
            PageCursor pageCursor = new PageCursor(pageInfosByColumn.get(i), context);
            iterators[i] = new ColumnValueIterator(pageCursor, schema.getColumn(originalIndex), schema.isFlatSchema());
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
        // Use commonPool for batch tasks to avoid deadlock with prefetch tasks on context.executor().
        // Batch tasks block waiting for prefetches; using separate pools prevents thread starvation.
        CompletableFuture<TypedColumnData>[] futures = new CompletableFuture[iterators.length];
        for (int i = 0; i < iterators.length; i++) {
            final int col = i;
            futures[i] = CompletableFuture.supplyAsync(() -> iterators[col].readBatch(BATCH_SIZE), ForkJoinPool.commonPool());
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
