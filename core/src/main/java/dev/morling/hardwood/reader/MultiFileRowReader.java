/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.reader;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;

import dev.morling.hardwood.internal.reader.BatchDataView;
import dev.morling.hardwood.internal.reader.ColumnValueIterator;
import dev.morling.hardwood.internal.reader.FileManager;
import dev.morling.hardwood.internal.reader.PageCursor;
import dev.morling.hardwood.internal.reader.TypedColumnData;
import dev.morling.hardwood.schema.FileSchema;
import dev.morling.hardwood.schema.ProjectedSchema;

/**
 * A RowReader that reads across multiple Parquet files with automatic file prefetching.
 * <p>
 * This reader uses a {@link FileManager} to handle file lifecycle and prefetching.
 * The next file is automatically prepared while reading the current file, minimizing
 * latency at file boundaries.
 * </p>
 *
 * <p>Usage:</p>
 * <pre>{@code
 * try (Hardwood hardwood = Hardwood.create();
 *      MultiFileRowReader reader = hardwood.openAll(files)) {
 *     while (reader.hasNext()) {
 *         reader.next();
 *         // access data using same API as RowReader
 *     }
 * }
 * }</pre>
 */
public class MultiFileRowReader extends AbstractRowReader {

    private final FileSchema schema;
    private final ProjectedSchema projectedSchema;
    private final HardwoodContext context;
    private final FileManager fileManager;
    private final FileManager.InitResult initResult;
    private final int adaptiveBatchSize;

    // Iterators for each projected column
    private ColumnValueIterator[] iterators;

    /**
     * Creates a MultiFileRowReader for the given files.
     *
     * @param files the Parquet files to read (must not be empty)
     * @param context the Hardwood context
     * @param projection column projection
     * @throws IOException if the first file cannot be opened or read
     */
    MultiFileRowReader(List<Path> files, HardwoodContext context, ColumnProjection projection) throws IOException {
        if (files.isEmpty()) {
            throw new IllegalArgumentException("At least one file must be provided");
        }

        this.context = context;
        this.fileManager = new FileManager(files, context);
        this.initResult = fileManager.initialize(projection);
        this.schema = initResult.schema();
        this.projectedSchema = initResult.projectedSchema();
        this.adaptiveBatchSize = computeOptimalBatchSize(projectedSchema);
    }

    @Override
    protected void initialize() {
        if (initialized) {
            return;
        }
        initialized = true;

        int projectedColumnCount = projectedSchema.getProjectedColumnCount();

        // Create iterators using pages from the first file
        iterators = new ColumnValueIterator[projectedColumnCount];
        for (int i = 0; i < projectedColumnCount; i++) {
            int originalIndex = projectedSchema.toOriginalIndex(i);
            PageCursor pageCursor = new PageCursor(
                    initResult.firstFileState().pageInfosByColumn().get(i), context, fileManager, i);
            iterators[i] = new ColumnValueIterator(pageCursor, schema.getColumn(originalIndex), schema.isFlatSchema());
        }

        // Initialize the unified data view
        dataView = BatchDataView.create(schema, projectedSchema);

        // Load first batch
        loadNextBatch();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected boolean loadNextBatch() {
        // Read columns in parallel using ForkJoinPool.commonPool()
        CompletableFuture<TypedColumnData>[] futures = new CompletableFuture[iterators.length];
        for (int i = 0; i < iterators.length; i++) {
            final int col = i;
            futures[i] = CompletableFuture.supplyAsync(
                    () -> iterators[col].readBatch(adaptiveBatchSize), ForkJoinPool.commonPool());
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

        // Within a single file, all columns should have the same number of values
        // Use minimum as a safety check
        int minRecordCount = newColumnData[0].recordCount();
        for (int i = 1; i < newColumnData.length; i++) {
            minRecordCount = Math.min(minRecordCount, newColumnData[i].recordCount());
        }

        dataView.setBatchData(newColumnData);

        batchSize = minRecordCount;
        rowIndex = -1;
        return batchSize > 0;
    }

    @Override
    public void close() {
        closed = true;
        fileManager.close();
    }
}
