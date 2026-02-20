/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;

import dev.hardwood.HardwoodContext;
import dev.hardwood.internal.reader.BatchDataView;
import dev.hardwood.internal.reader.ColumnAssemblyBuffer;
import dev.hardwood.internal.reader.ColumnValueIterator;
import dev.hardwood.internal.reader.PageCursor;
import dev.hardwood.internal.reader.PageInfo;
import dev.hardwood.internal.reader.PageScanner;
import dev.hardwood.internal.reader.TypedColumnData;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.ProjectedSchema;

/**
 * RowReader implementation for reading a single Parquet file.
 * Handles both flat and nested schemas using the unified RowDataView.
 */
final class SingleFileRowReader extends AbstractRowReader {

    private static final System.Logger LOG = System.getLogger(SingleFileRowReader.class.getName());

    private final FileSchema schema;
    private final ProjectedSchema projectedSchema;
    private final MappedByteBuffer fileMapping;
    private final List<RowGroup> rowGroups;
    private final HardwoodContext context;
    private final String fileName;
    private final int adaptiveBatchSize;

    private ColumnValueIterator[] iterators;

    SingleFileRowReader(FileSchema schema, ProjectedSchema projectedSchema, MappedByteBuffer fileMapping,
                        List<RowGroup> rowGroups, HardwoodContext context, String fileName) {
        this.schema = schema;
        this.projectedSchema = projectedSchema;
        this.fileMapping = fileMapping;
        this.rowGroups = rowGroups;
        this.context = context;
        this.fileName = fileName;
        this.adaptiveBatchSize = computeOptimalBatchSize(projectedSchema);
    }

    @Override
    protected void initialize() {
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

        // File mapping covers entire file, so base offset is 0
        final long mappingBaseOffset = 0;

        // Scan each projected column in parallel using the file mapping
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
                    PageScanner scanner = new PageScanner(columnSchema, columnChunk, context,
                            fileMapping, mappingBaseOffset);
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
        boolean flatSchema = schema.isFlatSchema();
        iterators = new ColumnValueIterator[projectedColumnCount];
        for (int i = 0; i < projectedColumnCount; i++) {
            int originalIndex = projectedSchema.toOriginalIndex(i);
            ColumnSchema columnSchema = schema.getColumn(originalIndex);

            // Create assembly buffer for eager batch assembly (flat schemas only)
            ColumnAssemblyBuffer assemblyBuffer = null;
            if (flatSchema) {
                assemblyBuffer = new ColumnAssemblyBuffer(columnSchema, adaptiveBatchSize);
            }

            PageCursor pageCursor = new PageCursor(pageInfosByColumn.get(i), context, assemblyBuffer);
            iterators[i] = new ColumnValueIterator(pageCursor, columnSchema, flatSchema);
        }

        // Initialize the data view
        dataView = BatchDataView.create(schema, projectedSchema);

        // Eagerly load first batch
        loadNextBatch();
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected boolean loadNextBatch() {
        // Use commonPool for batch tasks to avoid deadlock with prefetch tasks on context.executor().
        // Batch tasks block waiting for prefetches; using separate pools prevents thread starvation.
        CompletableFuture<TypedColumnData>[] futures = new CompletableFuture[iterators.length];
        for (int i = 0; i < iterators.length; i++) {
            final int col = i;
            futures[i] = CompletableFuture.supplyAsync(() -> iterators[col].readBatch(adaptiveBatchSize), ForkJoinPool.commonPool());
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

        dataView.setBatchData(newColumnData);

        batchSize = newColumnData[0].recordCount();
        rowIndex = -1;
        return batchSize > 0;
    }
}
