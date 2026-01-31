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
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import dev.morling.hardwood.internal.reader.BatchDataView;
import dev.morling.hardwood.internal.reader.ColumnValueIterator;
import dev.morling.hardwood.internal.reader.CrossFilePrefetchCoordinator;
import dev.morling.hardwood.internal.reader.PageCursor;
import dev.morling.hardwood.internal.reader.PageInfo;
import dev.morling.hardwood.internal.reader.PageScanner;
import dev.morling.hardwood.internal.reader.TypedColumnData;
import dev.morling.hardwood.metadata.ColumnChunk;
import dev.morling.hardwood.metadata.FileMetaData;
import dev.morling.hardwood.metadata.RowGroup;
import dev.morling.hardwood.schema.ColumnSchema;
import dev.morling.hardwood.schema.FileSchema;
import dev.morling.hardwood.schema.ProjectedSchema;

/**
 * A RowReader that reads across multiple Parquet files with cross-file prefetching.
 * <p>
 * This reader coordinates prefetching so that when pages from file N are running low,
 * pages from file N+1 are already being prefetched. This eliminates prefetch queue
 * misses at file boundaries.
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

    private static final System.Logger LOG = System.getLogger(MultiFileRowReader.class.getName());
    private static final int BATCH_SIZE = 16384;

    private final FileSchema schema;
    private final ProjectedSchema projectedSchema;
    private final HardwoodContext context;
    private final CrossFilePrefetchCoordinator coordinator;

    // First file state
    private final FileChannel firstFileChannel;
    private final List<RowGroup> firstFileRowGroups;

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

        // Open first file and get schema
        Path firstPath = files.get(0);
        this.firstFileChannel = FileChannel.open(firstPath, StandardOpenOption.READ);

        try {
            // Read metadata using ParquetFileReader
            try (ParquetFileReader firstReader = ParquetFileReader.open(firstPath)) {
                FileMetaData fileMetaData = firstReader.getFileMetaData();
                this.schema = firstReader.getFileSchema();
                this.firstFileRowGroups = fileMetaData.rowGroups();
            }

            this.projectedSchema = ProjectedSchema.create(schema, projection);

            // Create coordinator for remaining files
            List<Path> remainingFiles = files.size() > 1 ? new ArrayList<>(files.subList(1, files.size())) : List.of();
            this.coordinator = new CrossFilePrefetchCoordinator(
                    remainingFiles, context, projectedSchema, schema);

            LOG.log(System.Logger.Level.DEBUG,
                    "MultiFileRowReader created for {0} files, {1} projected columns",
                    files.size(), projectedSchema.getProjectedColumnCount());
        }
        catch (Exception e) {
            try {
                firstFileChannel.close();
            }
            catch (IOException closeEx) {
                e.addSuppressed(closeEx);
            }
            throw e;
        }
    }

    @Override
    protected void initialize() {
        if (initialized) {
            return;
        }
        initialized = true;

        int projectedColumnCount = projectedSchema.getProjectedColumnCount();
        List<List<PageInfo>> pageInfosByColumn = scanFirstFilePages();

        // Create iterators with cross-file prefetching
        iterators = new ColumnValueIterator[projectedColumnCount];
        for (int i = 0; i < projectedColumnCount; i++) {
            int originalIndex = projectedSchema.toOriginalIndex(i);
            PageCursor pageCursor = new PageCursor(
                    pageInfosByColumn.get(i), context, coordinator, i);
            iterators[i] = new ColumnValueIterator(pageCursor, schema.getColumn(originalIndex), schema.isFlatSchema());
        }

        // Initialize the unified data view
        dataView = BatchDataView.create(schema, projectedSchema);

        // Load first batch
        loadNextBatch();
    }

    private List<List<PageInfo>> scanFirstFilePages() {
        int projectedColumnCount = projectedSchema.getProjectedColumnCount();

        // Scan pages for each projected column in parallel
        @SuppressWarnings("unchecked")
        CompletableFuture<List<PageInfo>>[] scanFutures = new CompletableFuture[projectedColumnCount];

        for (int projectedIndex = 0; projectedIndex < projectedColumnCount; projectedIndex++) {
            final int projIdx = projectedIndex;
            final int originalIndex = projectedSchema.toOriginalIndex(projectedIndex);
            final ColumnSchema columnSchema = schema.getColumn(originalIndex);

            scanFutures[projIdx] = CompletableFuture.supplyAsync(() -> {
                List<PageInfo> columnPages = new ArrayList<>();
                for (RowGroup rowGroup : firstFileRowGroups) {
                    ColumnChunk columnChunk = rowGroup.columns().get(originalIndex);
                    PageScanner scanner = new PageScanner(firstFileChannel, columnSchema, columnChunk, context);
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

        CompletableFuture.allOf(scanFutures).join();

        List<List<PageInfo>> result = new ArrayList<>(projectedColumnCount);
        for (int i = 0; i < projectedColumnCount; i++) {
            result.add(scanFutures[i].join());
        }
        return result;
    }

    @Override
    protected boolean loadNextBatch() {
        // Before reading, check if all columns have exhausted their current file
        // If so, add next file pages to all columns together
        coordinateCrossFileTransition();

        // Read columns sequentially
        TypedColumnData[] newColumnData = new TypedColumnData[iterators.length];
        for (int i = 0; i < iterators.length; i++) {
            newColumnData[i] = iterators[i].readBatch(BATCH_SIZE);
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

    /**
     * Coordinates cross-file transitions so all columns move to the next file together.
     * Only adds pages when ALL columns have exhausted their current file pages.
     * This ensures data alignment across file boundaries.
     */
    private void coordinateCrossFileTransition() {
        // Check if ALL columns need pages from the next file
        boolean allNeedNextFile = true;
        for (ColumnValueIterator iterator : iterators) {
            if (!iterator.needsNextFilePages()) {
                allNeedNextFile = false;
                break;
            }
        }

        // Only add pages when ALL columns are ready to transition
        if (allNeedNextFile) {
            LOG.log(System.Logger.Level.DEBUG,
                    "All {0} columns ready for cross-file transition", iterators.length);
            for (ColumnValueIterator iterator : iterators) {
                iterator.addNextFilePages();
            }
        }
    }

    @Override
    public void close() {
        closed = true;
        coordinator.closeChannels();
        try {
            firstFileChannel.close();
        }
        catch (IOException e) {
            LOG.log(System.Logger.Level.WARNING, "Failed to close first file channel", e);
        }
    }
}
