/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import dev.hardwood.InputFile;
import dev.hardwood.jfr.FileOpenedEvent;
import dev.hardwood.jfr.RowGroupFilterEvent;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.ProjectedSchema;

/// Manages file lifecycle for multi-file Parquet reading.
///
/// Handles opening, mapping, metadata reading, and page scanning for Parquet files.
/// Automatically prefetches the next file to minimize latency at file boundaries.
///
/// Thread safety: Uses [ConcurrentHashMap] to safely handle concurrent
/// page requests from multiple column cursors.
public class FileManager {

    private static final System.Logger LOG = System.getLogger(FileManager.class.getName());

    private final List<InputFile> inputFiles;
    private final HardwoodContextImpl context;

    // Thread-safe storage for file states and loading futures
    private final ConcurrentHashMap<Integer, CompletableFuture<FileState>> fileFutures = new ConcurrentHashMap<>();

    // Set after first file is opened
    private volatile ProjectedSchema projectedSchema;
    private volatile FileSchema referenceSchema;
    private volatile FilterPredicate filterPredicate;
    private OpenedFile firstOpenedFile;

    /// Creates a FileManager for the given input files.
    ///
    /// @param inputFiles the input files to read (must not be empty)
    /// @param context the Hardwood context with executor and decompressor
    public FileManager(List<InputFile> inputFiles, HardwoodContextImpl context) {
        if (inputFiles.isEmpty()) {
            throw new IllegalArgumentException("At least one file must be provided");
        }
        this.inputFiles = new ArrayList<>(inputFiles);
        this.context = context;
    }

    /// Opens the first file and reads its schema. This is a lightweight operation
    /// that does not scan pages. Call [#initialize(ColumnProjection)] afterwards
    /// to prepare for reading with a specific column projection.
    ///
    /// @return the file schema from the first file
    /// @throws IOException if the first file cannot be read
    public FileSchema openFirst() throws IOException {
        InputFile first = inputFiles.get(0);
        first.open();
        firstOpenedFile = openAndReadMetadata(first);
        referenceSchema = firstOpenedFile.schema;
        return referenceSchema;
    }

    /// Applies a column projection and optional filter, scans pages for the first file,
    /// and triggers prefetching. Must be called after [#openFirst()].
    ///
    /// @param projection column projection (use [ColumnProjection#all()] for all columns)
    /// @param filter predicate for row group filtering based on statistics, or `null` for no filtering
    /// @return result containing the file state, file schema, and projected schema
    public InitResult initialize(ColumnProjection projection, FilterPredicate filter) {
        this.filterPredicate = filter;
        return initialize(projection);
    }

    /// Applies a column projection, scans pages for the first file, and triggers
    /// prefetching. Must be called after [#openFirst()].
    ///
    /// @param projection column projection (use [ColumnProjection#all()] for all columns)
    /// @return result containing the file state, file schema, and projected schema
    public InitResult initialize(ColumnProjection projection) {
        if (firstOpenedFile == null) {
            throw new IllegalStateException("openFirst() must be called before initialize()");
        }

        projectedSchema = ProjectedSchema.create(referenceSchema, projection);

        InputFile first = inputFiles.get(0);

        // Scan pages for the first file
        List<List<PageInfo>> pageInfosByColumn = scanAllProjectedColumns(first,
                firstOpenedFile);

        FileState firstFileState = new FileState(
                first,
                firstOpenedFile.metaData, firstOpenedFile.schema, pageInfosByColumn);

        // No longer needed
        firstOpenedFile = null;

        // Store as completed future
        fileFutures.put(0, CompletableFuture.completedFuture(firstFileState));

        LOG.log(System.Logger.Level.DEBUG,
                "Initialized with first file: {0}, {1} projected columns",
                first.name(), projectedSchema.getProjectedColumnCount());

        // Trigger prefetch of second file
        triggerPrefetch(1);

        return new InitResult(firstFileState, referenceSchema, projectedSchema);
    }

    /// Result of initializing the FileManager with the first file.
    public record InitResult(FileState firstFileState, FileSchema schema, ProjectedSchema projectedSchema) {
    }

    /// Checks if a file exists at the given index.
    ///
    /// @param fileIndex the file index
    /// @return true if the file exists
    public boolean hasFile(int fileIndex) {
        return fileIndex >= 0 && fileIndex < inputFiles.size();
    }

    /// Gets the file name for the given index.
    ///
    /// @param fileIndex the file index
    /// @return the file name, or null if index is out of bounds
    public String getFileName(int fileIndex) {
        if (!hasFile(fileIndex)) {
            return null;
        }
        return inputFiles.get(fileIndex).name();
    }

    /// Checks if a file is ready (fully loaded) without blocking.
    ///
    /// @param fileIndex the file index
    /// @return true if the file is loaded and ready to use
    public boolean isFileReady(int fileIndex) {
        CompletableFuture<FileState> future = fileFutures.get(fileIndex);
        return future != null && future.isDone() && !future.isCompletedExceptionally();
    }

    /// Ensures a file is being loaded (triggers async load if not already started).
    /// This is non-blocking - it just ensures the loading process has been initiated.
    ///
    /// @param fileIndex the file index to ensure is loading
    public void ensureFileLoading(int fileIndex) {
        if (hasFile(fileIndex)) {
            fileFutures.computeIfAbsent(fileIndex, this::loadFileAsync);
        }
    }

    /// Gets pages for the specified file and column.
    ///
    /// If the file is already loaded, returns immediately. If still loading,
    /// blocks until ready. Also triggers prefetch of the next file.
    ///
    /// @param fileIndex the file index
    /// @param projectedColumnIndex the projected column index
    /// @return list of PageInfo for the column, or null if file index is out of bounds
    public List<PageInfo> getPages(int fileIndex, int projectedColumnIndex) {
        if (!hasFile(fileIndex)) {
            return null;
        }

        // Get or start loading this file
        CompletableFuture<FileState> future = fileFutures.computeIfAbsent(
                fileIndex,
                this::loadFileAsync);

        // Trigger prefetch of N+1 (idempotent via computeIfAbsent)
        triggerPrefetch(fileIndex + 1);

        // Wait for file to be ready - may throw if load failed
        FileState state = future.join();
        return state.pageInfosByColumn().get(projectedColumnIndex);
    }

    /// Triggers async prefetch of a file if it exists and isn't already loading.
    private void triggerPrefetch(int fileIndex) {
        if (hasFile(fileIndex)) {
            fileFutures.computeIfAbsent(fileIndex, this::loadFileAsync);
        }
    }

    /// Starts async loading of a file.
    private CompletableFuture<FileState> loadFileAsync(int fileIndex) {
        LOG.log(System.Logger.Level.DEBUG, "Starting async load of file {0}: {1}",
                fileIndex, inputFiles.get(fileIndex).name());
        return CompletableFuture.supplyAsync(
                () -> loadFile(fileIndex),
                context.executor());
    }

    /// Loads a file synchronously: opens, reads metadata, validates schema, scans pages.
    private FileState loadFile(int fileIndex) {
        InputFile inputFile = inputFiles.get(fileIndex);

        try {
            inputFile.open();
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to open file: " + inputFile.name(), e);
        }

        OpenedFile openedFile = openAndReadMetadata(inputFile);

        // Validate schema compatibility
        validateSchemaCompatibility(inputFile, openedFile.schema);

        // Scan pages for all projected columns
        List<List<PageInfo>> pageInfosByColumn = scanAllProjectedColumns(inputFile, openedFile);

        LOG.log(System.Logger.Level.DEBUG, "Loaded file {0}: {1}", fileIndex, inputFile.name());

        return new FileState(inputFile, openedFile.metaData,
                openedFile.schema, pageInfosByColumn);
    }

    /// Opens a file and reads its metadata.
    private OpenedFile openAndReadMetadata(InputFile inputFile) {
        FileOpenedEvent fileOpenedEvent = new FileOpenedEvent();
        fileOpenedEvent.begin();

        try {
            FileMetaData metaData = ParquetMetadataReader.readMetadata(inputFile);
            FileSchema schema = FileSchema.fromSchemaElements(metaData.schema());

            fileOpenedEvent.file = inputFile.name();
            fileOpenedEvent.fileSize = inputFile.length();
            fileOpenedEvent.rowGroupCount = metaData.rowGroups().size();
            fileOpenedEvent.columnCount = schema.getColumnCount();
            fileOpenedEvent.commit();

            return new OpenedFile(metaData, schema);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to read metadata: " + inputFile.name(), e);
        }
    }

    /// Holds the result of opening a file and reading its metadata.
    private record OpenedFile(FileMetaData metaData, FileSchema schema) {
    }

    /// Validates that the file schema is compatible with the reference schema.
    private void validateSchemaCompatibility(InputFile inputFile, FileSchema fileSchema) {
        int projectedColumnCount = projectedSchema.getProjectedColumnCount();
        for (int projectedIndex = 0; projectedIndex < projectedColumnCount; projectedIndex++) {
            int originalIndex = projectedSchema.toOriginalIndex(projectedIndex);
            ColumnSchema refColumn = referenceSchema.getColumn(originalIndex);

            // Find column in new file by field path (unambiguous even with duplicate leaf names)
            ColumnSchema fileColumn;
            try {
                fileColumn = fileSchema.getColumn(refColumn.fieldPath());
            }
            catch (IllegalArgumentException e) {
                throw new SchemaIncompatibleException(
                        "Column '" + refColumn.fieldPath() + "' not found in file: " + inputFile.name());
            }

            // Validate physical type matches
            PhysicalType refType = refColumn.type();
            PhysicalType fileType = fileColumn.type();
            if (refType != fileType) {
                throw new SchemaIncompatibleException(
                        "Column '" + refColumn.fieldPath() + "' has incompatible type in file " + inputFile.name() +
                                ": expected " + refType + " but found " + fileType);
            }
        }
    }

    /// Scans pages for all projected columns.
    ///
    /// @param inputFile the input file
    /// @param openedFile the opened file with metadata and schema
    /// @return list of page info lists, one per projected column
    private List<List<PageInfo>> scanAllProjectedColumns(InputFile inputFile, OpenedFile openedFile) {
        int projectedColumnCount = projectedSchema.getProjectedColumnCount();
        List<RowGroup> rowGroups = filterRowGroups(openedFile.metaData.rowGroups(), openedFile.schema,
                inputFile.name());

        // Build column index mapping using field paths for unambiguous cross-file lookup
        int[] columnIndices = new int[projectedColumnCount];
        ColumnSchema[] columnSchemas = new ColumnSchema[projectedColumnCount];
        for (int projectedIndex = 0; projectedIndex < projectedColumnCount; projectedIndex++) {
            int originalIndex = projectedSchema.toOriginalIndex(projectedIndex);
            ColumnSchema refColumn = referenceSchema.getColumn(originalIndex);
            columnSchemas[projectedIndex] = openedFile.schema.getColumn(refColumn.fieldPath());
            columnIndices[projectedIndex] = columnSchemas[projectedIndex].columnIndex();
        }

        String fileName = inputFile.name();

        // Scan each projected column in parallel, coalescing chunk reads per row group
        @SuppressWarnings("unchecked")
        CompletableFuture<List<PageInfo>>[] scanFutures = new CompletableFuture[projectedColumnCount];
        for (int i = 0; i < projectedColumnCount; i++) {
            scanFutures[i] = CompletableFuture.completedFuture(new ArrayList<>());
        }

        for (int rowGroupIndex = 0; rowGroupIndex < rowGroups.size(); rowGroupIndex++) {
            final int rgIdx = rowGroupIndex;
            RowGroup rowGroup = rowGroups.get(rgIdx);

            // Pre-fetch indexes and coalesced column chunk data
            RowGroupIndexBuffers indexBuffers;
            try {
                indexBuffers = RowGroupIndexBuffers.fetch(inputFile, rowGroup);
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to fetch index buffers for row group " + rgIdx, e);
            }

            List<ChunkRange> chunkRanges = ChunkRange.coalesce(
                    rowGroup.columns(), columnIndices, ChunkRange.MAX_GAP_BYTES);
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

            for (int projectedIndex = 0; projectedIndex < projectedColumnCount; projectedIndex++) {
                final int columnIndex = columnIndices[projectedIndex];
                final ColumnSchema columnSchema = columnSchemas[projectedIndex];
                final ColumnChunk columnChunk = rowGroup.columns().get(columnIndex);

                final ByteBuffer colChunkData = sliceColumnChunk(chunkRanges, rangeBuffers, columnChunk);
                final long colChunkOffset = chunkStartOffset(columnChunk);

                final CompletableFuture<List<PageInfo>> previousFuture = scanFutures[projectedIndex];
                scanFutures[projectedIndex] = previousFuture.thenCombineAsync(
                        CompletableFuture.supplyAsync(() -> {
                            PageScanner scanner = new PageScanner(columnSchema, columnChunk, context,
                                    colChunkData, colChunkOffset, indexBuffers.forColumn(columnIndex),
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

        // Wait for all scans to complete
        CompletableFuture.allOf(scanFutures).join();

        List<List<PageInfo>> result = new ArrayList<>(projectedColumnCount);
        for (int i = 0; i < projectedColumnCount; i++) {
            result.add(scanFutures[i].join());
        }

        return result;
    }

    /// Filters row groups using the active filter predicate.
    /// If no filter is set, returns the original list unmodified.
    private List<RowGroup> filterRowGroups(List<RowGroup> rowGroups, FileSchema schema, String fileName) {
        if (filterPredicate == null) {
            return rowGroups;
        }
        List<RowGroup> filtered = rowGroups.stream()
                .filter(rg -> !RowGroupFilterEvaluator.canDropRowGroup(filterPredicate, rg, schema))
                .toList();

        RowGroupFilterEvent event = new RowGroupFilterEvent();
        event.file = fileName;
        event.totalRowGroups = rowGroups.size();
        event.rowGroupsKept = filtered.size();
        event.rowGroupsSkipped = rowGroups.size() - filtered.size();
        event.commit();

        return filtered;
    }

    /// Waits for any in-flight prefetch to finish, then closes all opened files.
    public void close() {
        // Wait for in-flight prefetches so we don't leak files opened by background tasks
        for (CompletableFuture<FileState> future : fileFutures.values()) {
            try {
                future.join();
            }
            catch (Exception ignored) {
                // File may have failed to load — close will still be attempted below
            }
        }
        fileFutures.clear();

        for (InputFile file : inputFiles) {
            try {
                file.close();
            }
            catch (IOException e) {
                LOG.log(System.Logger.Level.WARNING, "Failed to close file: " + file.name(), e);
            }
        }
    }

    /// Exception thrown when schema incompatibility is detected between files.
    public static class SchemaIncompatibleException extends RuntimeException {
        public SchemaIncompatibleException(String message) {
            super(message);
        }
    }

    private static ByteBuffer sliceColumnChunk(List<ChunkRange> ranges, ByteBuffer[] rangeBuffers,
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

    private static long chunkStartOffset(ColumnChunk columnChunk) {
        Long dictOffset = columnChunk.metaData().dictionaryPageOffset();
        return (dictOffset != null && dictOffset > 0)
                ? dictOffset
                : columnChunk.metaData().dataPageOffset();
    }

}
