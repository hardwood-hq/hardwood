/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;

import dev.hardwood.HardwoodContext;
import dev.hardwood.internal.reader.BatchDataView;
import dev.hardwood.internal.reader.ColumnAssemblyBuffer;
import dev.hardwood.internal.reader.ColumnValueIterator;
import dev.hardwood.internal.reader.FileManager;
import dev.hardwood.internal.reader.PageCursor;
import dev.hardwood.internal.reader.TypedColumnData;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.ProjectedSchema;

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
 *      MultiFileParquetReader parquet = hardwood.openAll(files);
 *      MultiFileRowReader reader = parquet.createRowReader()) {
 *     while (reader.hasNext()) {
 *         reader.next();
 *         // access data using same API as RowReader
 *     }
 * }
 * }</pre>
 */
public class MultiFileRowReader extends AbstractRowReader {

    private static final System.Logger LOG = System.getLogger(MultiFileRowReader.class.getName());

    private final FileSchema schema;
    private final ProjectedSchema projectedSchema;
    private final HardwoodContext context;
    private final FileManager fileManager;
    private final FileManager.InitResult initResult;
    private final int adaptiveBatchSize;

    // Iterators for each projected column
    private ColumnValueIterator[] iterators;

    /**
     * Creates a MultiFileRowReader from a pre-initialized FileManager.
     *
     * @param files the Parquet files (for logging)
     * @param context the Hardwood context
     * @param fileManager the shared file manager
     * @param initResult the initialization result from the first file
     */
    MultiFileRowReader(java.util.List<java.nio.file.Path> files, HardwoodContext context,
                       FileManager fileManager, FileManager.InitResult initResult) {
        this.context = context;
        this.fileManager = fileManager;
        this.initResult = initResult;
        this.schema = initResult.schema();
        this.projectedSchema = initResult.projectedSchema();
        this.adaptiveBatchSize = computeOptimalBatchSize(projectedSchema);

        LOG.log(System.Logger.Level.DEBUG, "Created MultiFileRowReader for {0} files starting with {1}, {2} projected columns",
                files.size(), files.get(0).getFileName(), projectedSchema.getProjectedColumnCount());
    }

    @Override
    protected void initialize() {
        if (initialized) {
            return;
        }
        initialized = true;

        int projectedColumnCount = projectedSchema.getProjectedColumnCount();
        boolean flatSchema = schema.isFlatSchema();

        // Create iterators using pages from the first file
        String firstFileName = initResult.firstFileState().path().getFileName().toString();
        iterators = new ColumnValueIterator[projectedColumnCount];
        for (int i = 0; i < projectedColumnCount; i++) {
            int originalIndex = projectedSchema.toOriginalIndex(i);
            ColumnSchema columnSchema = schema.getColumn(originalIndex);

            // Create assembly buffer for eager batch assembly (flat schemas only)
            ColumnAssemblyBuffer assemblyBuffer = null;
            if (flatSchema) {
                assemblyBuffer = new ColumnAssemblyBuffer(columnSchema, adaptiveBatchSize);
            }

            PageCursor pageCursor = new PageCursor(
                    initResult.firstFileState().pageInfosByColumn().get(i), context, fileManager, i, firstFileName,
                    assemblyBuffer);
            iterators[i] = new ColumnValueIterator(pageCursor, columnSchema, flatSchema);
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
    }
}
