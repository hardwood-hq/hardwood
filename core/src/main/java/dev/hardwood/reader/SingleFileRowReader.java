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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;

import dev.hardwood.InputFile;
import dev.hardwood.internal.reader.BatchDataView;
import dev.hardwood.internal.reader.ChunkRange;
import dev.hardwood.internal.reader.ColumnAssemblyBuffer;
import dev.hardwood.internal.reader.ColumnValueIterator;
import dev.hardwood.internal.reader.HardwoodContextImpl;
import dev.hardwood.internal.reader.IndexedNestedColumnData;
import dev.hardwood.internal.reader.NestedBatchDataView;
import dev.hardwood.internal.reader.NestedColumnData;
import dev.hardwood.internal.reader.NestedLevelComputer;
import dev.hardwood.internal.reader.PageCursor;
import dev.hardwood.internal.reader.PageInfo;
import dev.hardwood.internal.reader.PageScanner;
import dev.hardwood.internal.reader.RowGroupIndexBuffers;
import dev.hardwood.internal.reader.TypedColumnData;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.ProjectedSchema;

/// RowReader implementation for reading a single Parquet file.
/// Handles both flat and nested schemas using the unified RowDataView.
final class SingleFileRowReader extends AbstractRowReader {

    private static final System.Logger LOG = System.getLogger(SingleFileRowReader.class.getName());

    private final FileSchema schema;
    private final ProjectedSchema projectedSchema;
    private final InputFile inputFile;
    private final List<RowGroup> rowGroups;
    private final HardwoodContextImpl context;
    private final int adaptiveBatchSize;

    private ColumnValueIterator[] iterators;
    private int[][] levelNullThresholds; // [projectedCol] -> thresholds for nested columns
    private CompletableFuture<IndexedNestedColumnData[]> pendingBatch;

    SingleFileRowReader(FileSchema schema, ProjectedSchema projectedSchema, InputFile inputFile,
                        List<RowGroup> rowGroups, HardwoodContextImpl context) {
        this.schema = schema;
        this.projectedSchema = projectedSchema;
        this.inputFile = inputFile;
        this.rowGroups = rowGroups;
        this.context = context;
        this.adaptiveBatchSize = computeOptimalBatchSize(projectedSchema);
    }

    @Override
    protected void initialize() {
        if (initialized) {
            return;
        }
        initialized = true;

        int projectedColumnCount = projectedSchema.getProjectedColumnCount();

        String fileName = inputFile.name();
        LOG.log(System.Logger.Level.DEBUG, "Starting to parse file ''{0}'' with {1} row groups, {2} projected columns (of {3} total)",
                fileName, rowGroups.size(), projectedColumnCount, schema.getColumnCount());

        // Collect page infos for each projected column across all row groups
        List<List<PageInfo>> pageInfosByColumn = new ArrayList<>(projectedColumnCount);
        for (int i = 0; i < projectedColumnCount; i++) {
            pageInfosByColumn.add(new ArrayList<>());
        }

        LOG.log(System.Logger.Level.DEBUG, "Scanning pages for {0} projected columns across {1} row groups",
                projectedColumnCount, rowGroups.size());

        // Build projected column index array
        int[] projectedOriginalIndices = new int[projectedColumnCount];
        for (int i = 0; i < projectedColumnCount; i++) {
            projectedOriginalIndices[i] = projectedSchema.toOriginalIndex(i);
        }

        // Pre-fetch indexes and column chunk data per row group, then scan columns in parallel
        @SuppressWarnings("unchecked")
        CompletableFuture<List<PageInfo>>[] scanFutures = new CompletableFuture[projectedColumnCount];
        for (int i = 0; i < projectedColumnCount; i++) {
            scanFutures[i] = CompletableFuture.completedFuture(new ArrayList<>());
        }

        for (int rowGroupIndex = 0; rowGroupIndex < rowGroups.size(); rowGroupIndex++) {
            final int rgIdx = rowGroupIndex;
            RowGroup rowGroup = rowGroups.get(rgIdx);

            // Pre-fetch all offset/column indexes for this row group in a single read
            RowGroupIndexBuffers indexBuffers;
            try {
                indexBuffers = RowGroupIndexBuffers.fetch(inputFile, rowGroup);
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to fetch index buffers for row group " + rgIdx, e);
            }

            // Coalesce projected column chunks and pre-fetch data
            List<ChunkRange> chunkRanges = ChunkRange.coalesce(
                    rowGroup.columns(), projectedOriginalIndices, ChunkRange.MAX_GAP_BYTES);
            ByteBuffer[] rangeBuffers = new ByteBuffer[chunkRanges.size()];
            try {
                for (int r = 0; r < chunkRanges.size(); r++) {
                    ChunkRange range = chunkRanges.get(r);
                    rangeBuffers[r] = inputFile.readRange(range.offset(), range.length());
                }
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to fetch column chunk data for row group " + rgIdx, e);
            }

            // Scan each projected column in parallel using pre-fetched data
            for (int projectedIndex = 0; projectedIndex < projectedColumnCount; projectedIndex++) {
                final int projIdx = projectedIndex;
                final int originalIndex = projectedOriginalIndices[projIdx];
                final ColumnSchema columnSchema = schema.getColumn(originalIndex);
                final ColumnChunk columnChunk = rowGroup.columns().get(originalIndex);

                // Find which ChunkRange contains this column's data
                final ByteBuffer colChunkData = sliceColumnChunk(chunkRanges, rangeBuffers, columnChunk);
                final long colChunkOffset = chunkStartOffset(columnChunk);

                final CompletableFuture<List<PageInfo>> previousFuture = scanFutures[projIdx];
                scanFutures[projIdx] = previousFuture.thenCombineAsync(
                        CompletableFuture.supplyAsync(() -> {
                            PageScanner scanner = new PageScanner(columnSchema, columnChunk, context,
                                    colChunkData, colChunkOffset, indexBuffers.forColumn(originalIndex),
                                    rgIdx, fileName);
                            try {
                                return scanner.scanPages();
                            }
                            catch (IOException e) {
                                throw new UncheckedIOException(
                                        "Failed to scan pages for column " + columnSchema.name(), e);
                            }
                        }, context.executor()),
                        (existing, newPages) -> {
                            existing.addAll(newPages);
                            return existing;
                        }, context.executor());
            }
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

            PageCursor pageCursor = PageCursor.create(pageInfosByColumn.get(i), context, fileName, assemblyBuffer);
            iterators[i] = new ColumnValueIterator(pageCursor, columnSchema, flatSchema);
        }

        // Precompute level-null thresholds per projected column (for nested schemas)
        if (!flatSchema) {
            levelNullThresholds = new int[projectedColumnCount][];
            for (int i = 0; i < projectedColumnCount; i++) {
                int originalIndex = projectedSchema.toOriginalIndex(i);
                ColumnSchema columnSchema = schema.getColumn(originalIndex);
                if (columnSchema.maxRepetitionLevel() > 0) {
                    levelNullThresholds[i] = NestedLevelComputer.computeLevelNullThresholds(
                            schema.getRootNode(), columnSchema.columnIndex());
                }
            }
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
    protected boolean loadNextBatch() {
        if (!schema.isFlatSchema()) {
            return loadNextNestedBatch();
        }
        return loadNextFlatBatch();
    }

    @SuppressWarnings("unchecked")
    private boolean loadNextFlatBatch() {
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

    /// Load the next nested batch. Index computation is fused into the column futures
    /// (Optimization A) and the next batch is pre-launched while the consumer iterates
    /// the current batch (Optimization B).
    private boolean loadNextNestedBatch() {
        IndexedNestedColumnData[] indexed;
        if (pendingBatch != null) {
            indexed = pendingBatch.join();
            pendingBatch = null;
        }
        else {
            indexed = launchNestedColumnFutures().join();
        }

        for (IndexedNestedColumnData icd : indexed) {
            if (icd.data().recordCount() == 0) {
                exhausted = true;
                return false;
            }
        }

        ((NestedBatchDataView) dataView).setBatchData(indexed);

        batchSize = indexed[0].data().recordCount();
        rowIndex = -1;

        // Pipeline: launch column futures for the next batch while consumer iterates this one
        pendingBatch = launchNestedColumnFutures();

        return batchSize > 0;
    }

    @SuppressWarnings("unchecked")
    private CompletableFuture<IndexedNestedColumnData[]> launchNestedColumnFutures() {
        CompletableFuture<IndexedNestedColumnData>[] futures = new CompletableFuture[iterators.length];
        for (int i = 0; i < iterators.length; i++) {
            final int col = i;
            final int[] thresholds = levelNullThresholds[col];
            futures[i] = CompletableFuture.supplyAsync(() -> {
                TypedColumnData data = iterators[col].readBatch(adaptiveBatchSize);
                if (data.recordCount() == 0) {
                    return new IndexedNestedColumnData((NestedColumnData) data, null, null, null, null);
                }
                return IndexedNestedColumnData.compute((NestedColumnData) data, thresholds);
            }, ForkJoinPool.commonPool());
        }
        return CompletableFuture.allOf(futures).thenApply(v -> {
            IndexedNestedColumnData[] result = new IndexedNestedColumnData[futures.length];
            for (int i = 0; i < futures.length; i++) {
                result[i] = futures[i].join();
            }
            return result;
        });
    }

    /// Slices a single column chunk's data from the pre-fetched ChunkRange buffers.
    static ByteBuffer sliceColumnChunk(List<ChunkRange> ranges, ByteBuffer[] rangeBuffers,
            ColumnChunk columnChunk) {
        long colStart = chunkStartOffset(columnChunk);
        int colLen = Math.toIntExact(columnChunk.metaData().totalCompressedSize());
        for (int r = 0; r < ranges.size(); r++) {
            ChunkRange range = ranges.get(r);
            if (colStart >= range.offset() && colStart + colLen <= range.offset() + range.length()) {
                int relOffset = Math.toIntExact(colStart - range.offset());
                return rangeBuffers[r].slice(relOffset, colLen);
            }
        }
        throw new IllegalStateException("Column chunk not found in any coalesced range");
    }

    /// Computes the absolute file offset where a column chunk's data starts,
    /// accounting for an optional dictionary page prefix.
    static long chunkStartOffset(ColumnChunk columnChunk) {
        Long dictOffset = columnChunk.metaData().dictionaryPageOffset();
        return (dictOffset != null && dictOffset > 0)
                ? dictOffset
                : columnChunk.metaData().dataPageOffset();
    }

}
