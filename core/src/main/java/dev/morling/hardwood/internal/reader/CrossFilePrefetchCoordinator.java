/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.reader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import dev.morling.hardwood.metadata.ColumnChunk;
import dev.morling.hardwood.metadata.FileMetaData;
import dev.morling.hardwood.metadata.PhysicalType;
import dev.morling.hardwood.metadata.RowGroup;
import dev.morling.hardwood.reader.HardwoodContext;
import dev.morling.hardwood.schema.ColumnSchema;
import dev.morling.hardwood.schema.FileSchema;
import dev.morling.hardwood.schema.ProjectedSchema;

/**
 * Coordinates file preparation and cross-file prefetching for multi-file reading.
 * <p>
 * This coordinator handles all files including the first one. It:
 * <ul>
 *   <li>Prepares the first file synchronously to establish the reference schema</li>
 *   <li>Prefetches subsequent files asynchronously before they're needed</li>
 *   <li>Validates schema compatibility across files</li>
 *   <li>Manages file channel lifecycle</li>
 * </ul>
 * </p>
 * <p>
 * Thread safety: Multiple columns may request pages simultaneously. File preparation
 * happens once per file transition, using {@link CompletableFuture} for async coordination.
 * </p>
 */
public class CrossFilePrefetchCoordinator {

    private static final System.Logger LOG = System.getLogger(CrossFilePrefetchCoordinator.class.getName());

    private final HardwoodContext context;
    private final List<Path> remainingFiles;

    // Set after first file is opened
    private ProjectedSchema projectedSchema;
    private FileSchema referenceSchema;
    private FileChannel firstFileChannel;
    private MappedByteBuffer firstFileMapping;
    private FileMetaData firstFileMetaData;
    private Path firstFilePath;

    // First file state (prepared after scanning)
    private FileState firstFileState;

    // Synchronization for subsequent file preparation
    private final Object preparationLock = new Object();
    private final AtomicBoolean preparationTriggered = new AtomicBoolean(false);
    private CompletableFuture<FileState> nextFileStateFuture;
    private FileState currentPreparedState;

    // Track which columns have consumed pages from current prepared file
    private boolean[] columnsConsumed;
    private int columnsConsumedCount = 0;

    // Channels to close after all pages consumed
    private final List<FileChannel> channelsToClose = new ArrayList<>();

    /**
     * Creates a coordinator for all files.
     *
     * @param files all files to read (must not be empty)
     * @param context the hardwood context with executor and decompressor
     */
    public CrossFilePrefetchCoordinator(List<Path> files, HardwoodContext context) {
        if (files.isEmpty()) {
            throw new IllegalArgumentException("At least one file must be provided");
        }
        this.remainingFiles = new ArrayList<>(files);
        this.context = context;
    }

    /**
     * Opens the first file and returns its schema.
     * <p>
     * This is phase 1 of initialization. Call this first to get the schema,
     * then create a ProjectedSchema, then call {@link #prepareFirstFile(ProjectedSchema)}.
     * </p>
     *
     * @return the schema from the first file
     * @throws IOException if the first file cannot be read
     */
    public FileSchema openFirstFile() throws IOException {
        firstFilePath = remainingFiles.remove(0);

        OpenedFile opened = openAndMapFile(firstFilePath);
        firstFileChannel = opened.channel;
        firstFileMapping = opened.mapping;
        firstFileMetaData = opened.metaData;
        referenceSchema = opened.schema;

        return referenceSchema;
    }

    /**
     * Opens a file, maps it entirely, and reads its metadata.
     *
     * @param path the file path to open
     * @return the opened file with channel, mapping, metadata, and schema
     * @throws IOException if the file cannot be opened or read
     */
    private OpenedFile openAndMapFile(Path path) throws IOException {
        FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);

        try {
            long fileSize = channel.size();

            FileMappingEvent event = new FileMappingEvent();
            event.begin();

            MappedByteBuffer mapping = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize);

            event.path = path.toString();
            event.offset = 0;
            event.size = fileSize;
            event.column = "(entire file)";
            event.commit();

            FileMetaData metaData = ParquetMetadataReader.readMetadata(mapping, path);
            FileSchema schema = FileSchema.fromSchemaElements(metaData.schema());

            return new OpenedFile(channel, mapping, metaData, schema);
        }
        catch (Exception e) {
            try {
                channel.close();
            }
            catch (IOException closeEx) {
                e.addSuppressed(closeEx);
            }
            throw e;
        }
    }

    private record OpenedFile(FileChannel channel, MappedByteBuffer mapping, FileMetaData metaData, FileSchema schema) {}

    /**
     * Scans pages for the first file after schema is established.
     * <p>
     * This is phase 2 of initialization. Must be called after {@link #openFirstFile()}.
     * </p>
     *
     * @param projectedSchema the projected schema (created from first file's schema)
     * @return the first file's state including channel, metadata, and scanned pages
     */
    public FileState prepareFirstFile(ProjectedSchema projectedSchema) {
        if (firstFileChannel == null) {
            throw new IllegalStateException("openFirstFile() must be called first");
        }

        this.projectedSchema = projectedSchema;
        this.columnsConsumed = new boolean[projectedSchema.getProjectedColumnCount()];

        // Scan pages for the first file using the existing mapping
        List<List<PageInfo>> pageInfosByColumn = scanAllProjectedColumns(
                firstFileMapping, firstFileMetaData, referenceSchema, true);

        firstFileState = new FileState(firstFilePath, firstFileChannel, firstFileMapping,
                firstFileMetaData, referenceSchema, pageInfosByColumn);

        LOG.log(System.Logger.Level.DEBUG,
                "First file prepared: {0}, {1} projected columns",
                firstFilePath, projectedSchema.getProjectedColumnCount());

        // Pre-trigger preparation of next file if available
        triggerNextFilePrefetch();

        return firstFileState;
    }

    /**
     * Returns the reference schema established from the first file.
     *
     * @return the reference file schema
     */
    public FileSchema getReferenceSchema() {
        return referenceSchema;
    }

    /**
     * Check if there are more files available for prefetching.
     *
     * @return true if there are remaining files to process
     */
    public boolean hasMoreFiles() {
        synchronized (preparationLock) {
            return !remainingFiles.isEmpty() || currentPreparedState != null;
        }
    }

    /**
     * Gets pages from the next file for the specified column.
     * <p>
     * On first call for a file transition, triggers async preparation of the next file.
     * Blocks if the next file isn't yet prepared.
     * </p>
     *
     * @param projectedColumnIndex the projected column index
     * @return list of PageInfo for the column from the next file, or null if no more files
     */
    public List<PageInfo> getNextFilePages(int projectedColumnIndex) {
        FileState state = getOrPrepareNextFile();
        if (state == null) {
            return null;
        }

        List<PageInfo> pages = state.pageInfosByColumn().get(projectedColumnIndex);

        // Mark this column as having consumed its pages
        markColumnConsumed(projectedColumnIndex);

        return pages;
    }

    /**
     * Gets or prepares the next file state.
     *
     * @return the prepared file state, or null if no more files
     */
    private FileState getOrPrepareNextFile() {
        synchronized (preparationLock) {
            // Check if we already have a prepared state ready
            if (currentPreparedState != null) {
                return currentPreparedState;
            }

            // Check if there are more files
            if (remainingFiles.isEmpty()) {
                return null;
            }

            // Trigger preparation if not already done
            if (preparationTriggered.compareAndSet(false, true)) {
                Path nextPath = remainingFiles.get(0);
                LOG.log(System.Logger.Level.DEBUG,
                        "Cross-file prefetch triggered, preparing file: {0}", nextPath);

                nextFileStateFuture = CompletableFuture.supplyAsync(
                        () -> prepareFile(nextPath), context.executor());
            }
        }

        // Wait for preparation to complete (outside lock to avoid blocking other threads)
        FileState state = nextFileStateFuture.join();

        synchronized (preparationLock) {
            if (currentPreparedState == null) {
                currentPreparedState = state;
            }
            return currentPreparedState;
        }
    }

    /**
     * Marks a column as having consumed its pages from the current prepared file.
     * When all columns have consumed, advances to the next file.
     */
    private void markColumnConsumed(int projectedColumnIndex) {
        synchronized (preparationLock) {
            if (!columnsConsumed[projectedColumnIndex]) {
                columnsConsumed[projectedColumnIndex] = true;
                columnsConsumedCount++;

                // Check if all columns have consumed their pages
                if (columnsConsumedCount == columnsConsumed.length) {
                    advanceToNextFile();
                }
            }
        }
    }

    /**
     * Advances to the next file after all columns have consumed current file's pages.
     */
    private void advanceToNextFile() {
        if (currentPreparedState != null) {
            // Schedule channel for closing (MappedByteBuffers remain valid)
            channelsToClose.add(currentPreparedState.channel());
        }

        // Remove the consumed file from remaining
        if (!remainingFiles.isEmpty()) {
            remainingFiles.remove(0);
        }

        // Reset for next file
        currentPreparedState = null;
        preparationTriggered.set(false);
        nextFileStateFuture = null;
        Arrays.fill(columnsConsumed, false);
        columnsConsumedCount = 0;

        // Pre-trigger preparation of next file if available
        triggerNextFilePrefetch();
    }

    /**
     * Triggers prefetch of the next file if available.
     */
    private void triggerNextFilePrefetch() {
        synchronized (preparationLock) {
            if (!remainingFiles.isEmpty() && !preparationTriggered.get()) {
                Path nextPath = remainingFiles.get(0);
                LOG.log(System.Logger.Level.DEBUG,
                        "Pre-triggering preparation of next file: {0}", nextPath);
                preparationTriggered.set(true);
                nextFileStateFuture = CompletableFuture.supplyAsync(
                        () -> prepareFile(nextPath), context.executor());
            }
        }
    }

    /**
     * Prepares a file by reading metadata and scanning pages for all projected columns.
     */
    private FileState prepareFile(Path path) {
        try {
            OpenedFile opened = openAndMapFile(path);

            // Validate schema compatibility
            validateSchemaCompatibility(path, opened.schema);

            // Scan pages for each projected column using the mapping
            List<List<PageInfo>> pageInfosByColumn = scanAllProjectedColumns(
                    opened.mapping, opened.metaData, opened.schema, false);

            return new FileState(path, opened.channel, opened.mapping, opened.metaData, opened.schema, pageInfosByColumn);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to prepare file: " + path, e);
        }
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
     * Scans pages for all projected columns using a pre-mapped file buffer.
     *
     * @param fileMapping the memory-mapped buffer covering the entire file
     * @param fileMetaData the file metadata
     * @param fileSchema the file schema
     * @param isFirstFile true if this is the first file (uses direct index mapping)
     * @return list of page info lists, one per projected column
     */
    private List<List<PageInfo>> scanAllProjectedColumns(MappedByteBuffer fileMapping, FileMetaData fileMetaData,
                                                         FileSchema fileSchema, boolean isFirstFile) {
        int projectedColumnCount = projectedSchema.getProjectedColumnCount();
        List<RowGroup> rowGroups = fileMetaData.rowGroups();

        // Build column index mapping
        int[] columnIndices = new int[projectedColumnCount];
        ColumnSchema[] columnSchemas = new ColumnSchema[projectedColumnCount];
        for (int projectedIndex = 0; projectedIndex < projectedColumnCount; projectedIndex++) {
            int originalIndex = projectedSchema.toOriginalIndex(projectedIndex);
            if (isFirstFile) {
                columnSchemas[projectedIndex] = fileSchema.getColumn(originalIndex);
                columnIndices[projectedIndex] = originalIndex;
            }
            else {
                ColumnSchema refColumn = referenceSchema.getColumn(originalIndex);
                columnSchemas[projectedIndex] = fileSchema.getColumn(refColumn.name());
                columnIndices[projectedIndex] = columnSchemas[projectedIndex].columnIndex();
            }
        }

        // File mapping covers entire file, so base offset is 0
        final long mappingBaseOffset = 0;

        // Scan each projected column in parallel using the file mapping
        @SuppressWarnings("unchecked")
        CompletableFuture<List<PageInfo>>[] scanFutures = new CompletableFuture[projectedColumnCount];

        for (int projectedIndex = 0; projectedIndex < projectedColumnCount; projectedIndex++) {
            final int projIdx = projectedIndex;
            final int columnIndex = columnIndices[projectedIndex];
            final ColumnSchema columnSchema = columnSchemas[projectedIndex];

            scanFutures[projIdx] = CompletableFuture.supplyAsync(() -> {
                List<PageInfo> columnPages = new ArrayList<>();
                for (RowGroup rowGroup : rowGroups) {
                    ColumnChunk columnChunk = rowGroup.columns().get(columnIndex);
                    PageScanner scanner = new PageScanner(columnSchema, columnChunk, context,
                            fileMapping, mappingBaseOffset);
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
     * Closes all accumulated file channels.
     * Should be called when the MultiFileRowReader is closed.
     */
    public void closeChannels() {
        // Close first file channel
        if (firstFileState != null) {
            try {
                firstFileState.channel().close();
            }
            catch (IOException e) {
                LOG.log(System.Logger.Level.WARNING, "Failed to close first file channel", e);
            }
        }

        // Close accumulated channels from consumed files
        for (FileChannel channel : channelsToClose) {
            try {
                channel.close();
            }
            catch (IOException e) {
                LOG.log(System.Logger.Level.WARNING, "Failed to close channel", e);
            }
        }
        channelsToClose.clear();

        // Close current prepared state's channel if any
        if (currentPreparedState != null) {
            try {
                currentPreparedState.channel().close();
            }
            catch (IOException e) {
                LOG.log(System.Logger.Level.WARNING, "Failed to close channel", e);
            }
        }
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
