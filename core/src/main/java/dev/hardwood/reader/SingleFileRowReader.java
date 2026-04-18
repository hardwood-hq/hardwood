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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;

import dev.hardwood.InputFile;
import dev.hardwood.internal.predicate.PageFilterEvaluator;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.reader.BatchDataView;
import dev.hardwood.internal.reader.ChunkRange;
import dev.hardwood.internal.reader.ColumnAssemblyBuffer;
import dev.hardwood.internal.reader.ColumnIndexBuffers;
import dev.hardwood.internal.reader.ColumnValueIterator;
import dev.hardwood.internal.reader.HardwoodContextImpl;
import dev.hardwood.internal.reader.IndexedNestedColumnData;
import dev.hardwood.internal.reader.NestedBatchDataView;
import dev.hardwood.internal.reader.NestedColumnData;
import dev.hardwood.internal.reader.NestedLevelComputer;
import dev.hardwood.internal.reader.PageCursor;
import dev.hardwood.internal.reader.PageRange;
import dev.hardwood.internal.reader.PageRangeData;
import dev.hardwood.internal.reader.PageScanner;
import dev.hardwood.internal.reader.RowGroupIndexBuffers;
import dev.hardwood.internal.reader.RowGroupPageSource;
import dev.hardwood.internal.reader.RowRanges;
import dev.hardwood.internal.reader.TypedColumnData;
import dev.hardwood.internal.reader.WindowedChunkReader;
import dev.hardwood.internal.thrift.OffsetIndexReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.OffsetIndex;
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

    // Projected column index array (built once, reused across row groups)
    private int[] projectedOriginalIndices;

    private ColumnValueIterator[] iterators;
    private int[][] levelNullThresholds; // [projectedCol] -> thresholds for nested columns
    private CompletableFuture<IndexedNestedColumnData[]> pendingBatch;

    // Cached per-row-group shared state for RowGroupPageSource calls.
    // When PageCursor requests a scanner for row group N, column 0, we fetch and cache
    // the shared metadata (index buffers, filter evaluation) so that columns 1..K reuse it.
    private int cachedRowGroupIndex = -1;
    private RowGroupIndexBuffers cachedIndexBuffers;
    private RowRanges cachedMatchingRows;
    private PageRangeData[] cachedPageRangeData;

    SingleFileRowReader(FileSchema schema, ProjectedSchema projectedSchema, InputFile inputFile,
                        List<RowGroup> rowGroups, HardwoodContextImpl context) {
        this(schema, projectedSchema, inputFile, rowGroups, context, null, 0);
    }

    /// @param filterPredicate resolved predicate, or `null` for no filtering.
    SingleFileRowReader(FileSchema schema, ProjectedSchema projectedSchema, InputFile inputFile,
                        List<RowGroup> rowGroups, HardwoodContextImpl context, ResolvedPredicate filterPredicate) {
        this(schema, projectedSchema, inputFile, rowGroups, context, filterPredicate, 0);
    }

    /// @param filterPredicate resolved predicate, or `null` for no filtering.
    /// @param maxRows maximum number of rows to return (0 = unlimited).
    SingleFileRowReader(FileSchema schema, ProjectedSchema projectedSchema, InputFile inputFile,
                        List<RowGroup> rowGroups, HardwoodContextImpl context,
                        ResolvedPredicate filterPredicate, long maxRows) {
        this.schema = schema;
        this.projectedSchema = projectedSchema;
        this.inputFile = inputFile;
        this.rowGroups = rowGroups;
        this.context = context;
        this.adaptiveBatchSize = computeOptimalBatchSize(projectedSchema);
        this.maxRows = maxRows;
        this.filterPredicate = filterPredicate;
        this.projectedSchemaRef = projectedSchema;
        this.fileName = inputFile.name();
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

        if (rowGroups.isEmpty()) {
            exhausted = true;
            return;
        }

        // Build projected column index array (reused across row groups)
        projectedOriginalIndices = new int[projectedColumnCount];
        for (int i = 0; i < projectedColumnCount; i++) {
            projectedOriginalIndices[i] = projectedSchema.toOriginalIndex(i);
        }

        // Compute how many row groups are needed based on maxRows.
        // When no record-level filter is active, row group metadata row counts give an
        // exact answer. With a filter, we can't predict how many rows will match, so
        // all row groups remain available.
        int totalRowGroups = rowGroups.size();
        if (maxRows > 0 && filterPredicate == null) {
            long rowsBudget = maxRows;
            totalRowGroups = 0;
            for (RowGroup rg : rowGroups) {
                totalRowGroups++;
                rowsBudget -= rg.numRows();
                if (rowsBudget <= 0) {
                    break;
                }
            }
        }

        // Scan only the first row group; subsequent row groups are fetched lazily by PageCursor
        int firstRowGroupIndex = 0;
        RowGroupPageSource rowGroupSource = this::getScannerForRowGroup;

        // Create iterators for each projected column with lazy row-group support
        boolean flatSchema = schema.isFlatSchema();
        iterators = new ColumnValueIterator[projectedColumnCount];
        for (int i = 0; i < projectedColumnCount; i++) {
            int originalIndex = projectedSchema.toOriginalIndex(i);
            ColumnSchema columnSchema = schema.getColumn(originalIndex);

            PageScanner firstScanner = getScannerForRowGroup(firstRowGroupIndex, i);

            // Create assembly buffer for eager batch assembly (flat schemas only)
            ColumnAssemblyBuffer assemblyBuffer = null;
            if (flatSchema) {
                assemblyBuffer = new ColumnAssemblyBuffer(columnSchema, adaptiveBatchSize);
            }

            PageCursor pageCursor = PageCursor.create(firstScanner, context, i, fileName,
                    assemblyBuffer, rowGroupSource, firstRowGroupIndex, totalRowGroups);
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

    /// Creates a [PageScanner] for a single projected column in a single row group.
    /// Shared per-row-group state (index buffers, filter evaluation) is cached so that
    /// multiple columns in the same row group reuse the same metadata I/O.
    ///
    /// For the page-range path (filter active + OffsetIndex), the scanner uses pre-fetched
    /// page range data. For the sequential path, the scanner uses a [WindowedChunkReader]
    /// that fetches column chunk data on demand.
    ///
    /// @param rowGroupIndex the row group index
    /// @param projectedColumnIndex the projected column index
    /// @return a PageScanner for the column, or null if row group index is out of bounds
    synchronized PageScanner getScannerForRowGroup(int rowGroupIndex, int projectedColumnIndex) {
        if (rowGroupIndex >= rowGroups.size()) {
            return null;
        }

        String fileName = inputFile.name();

        // Fetch and cache shared per-row-group metadata on first column request
        if (cachedRowGroupIndex != rowGroupIndex) {
            cachedRowGroupIndex = rowGroupIndex;
            RowGroup rowGroup = rowGroups.get(rowGroupIndex);
            int projectedColumnCount = projectedSchema.getProjectedColumnCount();

            try {
                cachedIndexBuffers = RowGroupIndexBuffers.fetch(inputFile, rowGroup);
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to fetch index buffers for row group " + rowGroupIndex, e);
            }

            cachedMatchingRows = RowRanges.ALL;
            if (filterPredicate != null) {
                cachedMatchingRows = PageFilterEvaluator.computeMatchingRows(
                        filterPredicate, rowGroup, cachedIndexBuffers);
            }

            cachedPageRangeData = new PageRangeData[projectedColumnCount];
            if (!cachedMatchingRows.isAll()) {
                for (int projIdx = 0; projIdx < projectedColumnCount; projIdx++) {
                    int originalIndex = projectedOriginalIndices[projIdx];
                    ColumnIndexBuffers colBuffers = cachedIndexBuffers.forColumn(originalIndex);
                    if (colBuffers != null && colBuffers.offsetIndex() != null) {
                        try {
                            OffsetIndex offsetIndex = OffsetIndexReader.read(
                                    new ThriftCompactReader(colBuffers.offsetIndex()));
                            ColumnChunk columnChunk = rowGroup.columns().get(originalIndex);
                            List<PageRange> pageRanges = PageRange.forColumn(
                                    offsetIndex, cachedMatchingRows, columnChunk, rowGroup.numRows(), ChunkRange.MAX_GAP_BYTES);
                            if (!pageRanges.isEmpty()) {
                                cachedPageRangeData[projIdx] = PageRangeData.fetch(inputFile, pageRanges);
                            }
                        }
                        catch (IOException e) {
                            throw new UncheckedIOException("Failed to fetch page ranges for row group " + rowGroupIndex, e);
                        }
                    }
                }
            }
        }

        // Create a PageScanner for this specific column
        RowGroup rowGroup = rowGroups.get(rowGroupIndex);
        int originalIndex = projectedOriginalIndices[projectedColumnIndex];
        ColumnSchema columnSchema = schema.getColumn(originalIndex);
        ColumnChunk columnChunk = rowGroup.columns().get(originalIndex);

        if (cachedPageRangeData[projectedColumnIndex] != null) {
            // Page-range path: data already fetched selectively, use batch scanner
            return new PageScanner(columnSchema, columnChunk, context,
                    cachedPageRangeData[projectedColumnIndex],
                    cachedIndexBuffers.forColumn(originalIndex),
                    rowGroupIndex, fileName, cachedMatchingRows, maxRows);
        }

        // Determine the column chunk's file offset and length
        long colChunkOffset = chunkStartOffset(columnChunk);
        int colChunkLength = Math.toIntExact(columnChunk.metaData().totalCompressedSize());

        // OffsetIndex path (no filter): fetch the full chunk for direct page lookup
        if (columnChunk.offsetIndexOffset() != null) {
            try {
                ByteBuffer colChunkData = inputFile.readRange(colChunkOffset, colChunkLength);
                return new PageScanner(columnSchema, columnChunk, context,
                        colChunkData, colChunkOffset,
                        cachedIndexBuffers.forColumn(originalIndex),
                        rowGroupIndex, fileName, cachedMatchingRows, maxRows);
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to fetch column chunk data for row group " + rowGroupIndex, e);
            }
        }

        // Sequential path (no OffsetIndex): use WindowedChunkReader for lazy fetching
        WindowedChunkReader chunkReader = new WindowedChunkReader(
                inputFile, colChunkOffset, colChunkLength);
        return new PageScanner(columnSchema, columnChunk, context,
                chunkReader, cachedIndexBuffers.forColumn(originalIndex),
                rowGroupIndex, fileName, cachedMatchingRows, maxRows);
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
