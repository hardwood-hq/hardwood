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
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import dev.hardwood.internal.reader.event.FileMappingEvent;
import dev.hardwood.internal.reader.event.FileOpenedEvent;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.ProjectedSchema;

/**
 * Manages file lifecycle for multi-file Parquet reading.
 * <p>
 * Handles opening, mapping, metadata reading, and page scanning for Parquet files.
 * Automatically prefetches the next file to minimize latency at file boundaries.
 * </p>
 * <p>
 * File channels are closed immediately after memory-mapping. The MappedByteBuffers
 * remain valid and are released when garbage collected.
 * </p>
 * <p>
 * Thread safety: Uses {@link ConcurrentHashMap} to safely handle concurrent
 * page requests from multiple column cursors.
 * </p>
 */
public class FileManager {

    private static final System.Logger LOG = System.getLogger(FileManager.class.getName());

    private final List<Path> files;
    private final HardwoodContextImpl context;

    // Thread-safe storage for file states and loading futures
    private final ConcurrentHashMap<Integer, CompletableFuture<FileState>> fileFutures = new ConcurrentHashMap<>();

    // Set after first file is opened
    private volatile ProjectedSchema projectedSchema;
    private volatile FileSchema referenceSchema;
    private MappedFile firstMappedFile;

    /**
     * Creates a FileManager for the given files.
     *
     * @param files the Parquet files to read (must not be empty)
     * @param context the Hardwood context with executor and decompressor
     */
    public FileManager(List<Path> files, HardwoodContextImpl context) {
        if (files.isEmpty()) {
            throw new IllegalArgumentException("At least one file must be provided");
        }
        this.files = new ArrayList<>(files);
        this.context = context;
    }

    /**
     * Opens the first file and reads its schema. This is a lightweight operation
     * that does not scan pages. Call {@link #initialize(ColumnProjection)} afterwards
     * to prepare for reading with a specific column projection.
     *
     * @return the file schema from the first file
     * @throws IOException if the first file cannot be read
     */
    public FileSchema openFirst() throws IOException {
        firstMappedFile = mapAndReadMetadata(files.get(0));
        referenceSchema = firstMappedFile.schema;
        return referenceSchema;
    }

    /**
     * Applies a column projection, scans pages for the first file, and triggers
     * prefetching. Must be called after {@link #openFirst()}.
     *
     * @param projection column projection (use {@link ColumnProjection#all()} for all columns)
     * @return result containing the file state, file schema, and projected schema
     */
    public InitResult initialize(ColumnProjection projection) {
        if (firstMappedFile == null) {
            throw new IllegalStateException("openFirst() must be called before initialize()");
        }

        projectedSchema = ProjectedSchema.create(referenceSchema, projection);

        // Scan pages for the first file
        List<List<PageInfo>> pageInfosByColumn = scanAllProjectedColumns(firstMappedFile,
                files.get(0).toString());

        FileState firstFileState = new FileState(
                files.get(0), firstMappedFile.mapping,
                firstMappedFile.metaData, firstMappedFile.schema, pageInfosByColumn);

        // No longer needed
        firstMappedFile = null;

        // Store as completed future
        fileFutures.put(0, CompletableFuture.completedFuture(firstFileState));

        LOG.log(System.Logger.Level.DEBUG,
                "Initialized with first file: {0}, {1} projected columns",
                files.get(0), projectedSchema.getProjectedColumnCount());

        // Trigger prefetch of second file
        triggerPrefetch(1);

        return new InitResult(firstFileState, referenceSchema, projectedSchema);
    }

    /**
     * Result of initializing the FileManager with the first file.
     */
    public record InitResult(FileState firstFileState, FileSchema schema, ProjectedSchema projectedSchema) {
    }

    /**
     * Checks if a file exists at the given index.
     *
     * @param fileIndex the file index
     * @return true if the file exists
     */
    public boolean hasFile(int fileIndex) {
        return fileIndex >= 0 && fileIndex < files.size();
    }

    /**
     * Gets the file name (without path) for the given index.
     *
     * @param fileIndex the file index
     * @return the file name, or null if index is out of bounds
     */
    public String getFileName(int fileIndex) {
        if (!hasFile(fileIndex)) {
            return null;
        }
        return files.get(fileIndex).getFileName().toString();
    }

    /**
     * Checks if a file is ready (fully loaded) without blocking.
     *
     * @param fileIndex the file index
     * @return true if the file is loaded and ready to use
     */
    public boolean isFileReady(int fileIndex) {
        CompletableFuture<FileState> future = fileFutures.get(fileIndex);
        return future != null && future.isDone() && !future.isCompletedExceptionally();
    }

    /**
     * Ensures a file is being loaded (triggers async load if not already started).
     * This is non-blocking - it just ensures the loading process has been initiated.
     *
     * @param fileIndex the file index to ensure is loading
     */
    public void ensureFileLoading(int fileIndex) {
        if (hasFile(fileIndex)) {
            fileFutures.computeIfAbsent(fileIndex, this::loadFileAsync);
        }
    }

    /**
     * Gets pages for the specified file and column.
     * <p>
     * If the file is already loaded, returns immediately. If still loading,
     * blocks until ready. Also triggers prefetch of the next file.
     * </p>
     *
     * @param fileIndex the file index
     * @param projectedColumnIndex the projected column index
     * @return list of PageInfo for the column, or null if file index is out of bounds
     */
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

    /**
     * Triggers async prefetch of a file if it exists and isn't already loading.
     */
    private void triggerPrefetch(int fileIndex) {
        if (hasFile(fileIndex)) {
            fileFutures.computeIfAbsent(fileIndex, this::loadFileAsync);
        }
    }

    /**
     * Starts async loading of a file.
     */
    private CompletableFuture<FileState> loadFileAsync(int fileIndex) {
        LOG.log(System.Logger.Level.DEBUG, "Starting async load of file {0}: {1}",
                fileIndex, files.get(fileIndex));
        return CompletableFuture.supplyAsync(
                () -> loadFile(fileIndex),
                context.executor());
    }

    /**
     * Loads a file synchronously: opens, maps, reads metadata, validates schema, scans pages.
     * The file channel is closed immediately after mapping.
     */
    private FileState loadFile(int fileIndex) {
        Path path = files.get(fileIndex);

        MappedFile mappedFile = mapAndReadMetadata(path);

        // Validate schema compatibility
        validateSchemaCompatibility(path, mappedFile.schema);

        // Scan pages for all projected columns
        List<List<PageInfo>> pageInfosByColumn = scanAllProjectedColumns(mappedFile, path.toString());

        LOG.log(System.Logger.Level.DEBUG, "Loaded file {0}: {1}", fileIndex, path);

        return new FileState(path, mappedFile.mapping, mappedFile.metaData,
                mappedFile.schema, pageInfosByColumn);

    }

    /**
     * Maps a file and reads its metadata. The file channel is closed immediately after mapping.
     */
    private MappedFile mapAndReadMetadata(Path path) {
        FileOpenedEvent fileOpenedEvent = new FileOpenedEvent();
        fileOpenedEvent.begin();

        MappedByteBuffer mapping;
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            long fileSize = channel.size();
            String fileName = path.getFileName().toString();

            FileMappingEvent event = new FileMappingEvent();
            event.begin();

            mapping = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize);

            event.file = fileName;
            event.offset = 0;
            event.size = fileSize;
            event.commit();

            FileMetaData metaData = ParquetMetadataReader.readMetadata(mapping, path);
            FileSchema schema = FileSchema.fromSchemaElements(metaData.schema());

            fileOpenedEvent.file = fileName;
            fileOpenedEvent.fileSize = fileSize;
            fileOpenedEvent.rowGroupCount = metaData.rowGroups().size();
            fileOpenedEvent.columnCount = schema.getColumnCount();
            fileOpenedEvent.commit();

            return new MappedFile(mapping, metaData, schema);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to map file: " + path, e);
        }
    }

    /**
     * Holds the result of mapping a file and reading its metadata.
     */
    private record MappedFile(MappedByteBuffer mapping, FileMetaData metaData, FileSchema schema) {
    }

    /**
     * Validates that the file schema is compatible with the reference schema.
     */
    private void validateSchemaCompatibility(Path path, FileSchema fileSchema) {
        int projectedColumnCount = projectedSchema.getProjectedColumnCount();
        for (int projectedIndex = 0; projectedIndex < projectedColumnCount; projectedIndex++) {
            int originalIndex = projectedSchema.toOriginalIndex(projectedIndex);
            ColumnSchema refColumn = referenceSchema.getColumn(originalIndex);

            // Find column in new file by name
            ColumnSchema fileColumn;
            try {
                fileColumn = fileSchema.getColumn(refColumn.name());
            }
            catch (IllegalArgumentException e) {
                throw new SchemaIncompatibleException(
                        "Column '" + refColumn.name() + "' not found in file: " + path);
            }

            // Validate physical type matches
            PhysicalType refType = refColumn.type();
            PhysicalType fileType = fileColumn.type();
            if (refType != fileType) {
                throw new SchemaIncompatibleException(
                        "Column '" + refColumn.name() + "' has incompatible type in file " + path +
                                ": expected " + refType + " but found " + fileType);
            }
        }
    }

    /**
     * Scans pages for all projected columns.
     *
     * @param mappedFile the mapped file with metadata and schema
     * @param filePath the file path for JFR event reporting
     * @return list of page info lists, one per projected column
     */
    private List<List<PageInfo>> scanAllProjectedColumns(MappedFile mappedFile, String filePath) {
        int projectedColumnCount = projectedSchema.getProjectedColumnCount();
        List<RowGroup> rowGroups = mappedFile.metaData.rowGroups();

        // Build column index mapping using column names for consistent lookup
        int[] columnIndices = new int[projectedColumnCount];
        ColumnSchema[] columnSchemas = new ColumnSchema[projectedColumnCount];
        for (int projectedIndex = 0; projectedIndex < projectedColumnCount; projectedIndex++) {
            int originalIndex = projectedSchema.toOriginalIndex(projectedIndex);
            ColumnSchema refColumn = referenceSchema.getColumn(originalIndex);
            columnSchemas[projectedIndex] = mappedFile.schema.getColumn(refColumn.name());
            columnIndices[projectedIndex] = columnSchemas[projectedIndex].columnIndex();
        }

        // Scan each projected column in parallel
        @SuppressWarnings("unchecked")
        CompletableFuture<List<PageInfo>>[] scanFutures = new CompletableFuture[projectedColumnCount];

        for (int projectedIndex = 0; projectedIndex < projectedColumnCount; projectedIndex++) {
            final int columnIndex = columnIndices[projectedIndex];
            final ColumnSchema columnSchema = columnSchemas[projectedIndex];

            scanFutures[projectedIndex] = CompletableFuture.supplyAsync(() -> {
                List<PageInfo> columnPages = new ArrayList<>();
                for (int rowGroupIndex = 0; rowGroupIndex < rowGroups.size(); rowGroupIndex++) {
                    ColumnChunk columnChunk = rowGroups.get(rowGroupIndex).columns().get(columnIndex);
                    PageScanner scanner = new PageScanner(columnSchema, columnChunk, context,
                            mappedFile.mapping, 0, filePath, rowGroupIndex);
                    try {
                        columnPages.addAll(scanner.scanPages());
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException(
                                "Failed to scan pages for column " + columnSchema.name(), e);
                    }
                }
                return columnPages;
            }, context.executor());
        }

        // Wait for all scans to complete
        CompletableFuture.allOf(scanFutures).join();

        List<List<PageInfo>> result = new ArrayList<>(projectedColumnCount);
        for (int i = 0; i < projectedColumnCount; i++) {
            result.add(scanFutures[i].join());
        }

        return result;
    }

    /**
     * Clears internal state.
     * <p>
     * File channels are already closed immediately after mapping, so this just
     * clears the futures map. MappedByteBuffers are released when garbage collected.
     * </p>
     */
    public void close() {
        fileFutures.clear();
    }

    /**
     * Exception thrown when schema incompatibility is detected between files.
     */
    public static class SchemaIncompatibleException extends RuntimeException {
        public SchemaIncompatibleException(String message) {
            super(message);
        }
    }
}
