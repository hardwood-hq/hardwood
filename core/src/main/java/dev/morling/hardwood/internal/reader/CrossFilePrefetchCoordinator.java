/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.reader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import dev.morling.hardwood.internal.thrift.FileMetaDataReader;
import dev.morling.hardwood.internal.thrift.ThriftCompactReader;
import dev.morling.hardwood.metadata.ColumnChunk;
import dev.morling.hardwood.metadata.FileMetaData;
import dev.morling.hardwood.metadata.PhysicalType;
import dev.morling.hardwood.metadata.RowGroup;
import dev.morling.hardwood.reader.HardwoodContext;
import dev.morling.hardwood.schema.ColumnSchema;
import dev.morling.hardwood.schema.FileSchema;
import dev.morling.hardwood.schema.ProjectedSchema;

/**
 * Coordinates cross-file prefetching by preparing next file's pages before current file is exhausted.
 * <p>
 * When a PageCursor detects its queue isn't full and current file pages are exhausted,
 * it requests pages from this coordinator. The coordinator:
 * <ul>
 *   <li>Triggers async file preparation (metadata read + page scanning) on first column request</li>
 *   <li>Returns pre-scanned pages for the requested column</li>
 *   <li>Tracks which columns have consumed pages from the current prepared file</li>
 * </ul>
 * </p>
 * <p>
 * Thread safety: Multiple columns may request simultaneously. File preparation happens once
 * per file transition, using {@link CompletableFuture} for async coordination.
 * </p>
 */
public class CrossFilePrefetchCoordinator {

    private static final System.Logger LOG = System.getLogger(CrossFilePrefetchCoordinator.class.getName());

    private static final byte[] MAGIC = "PAR1".getBytes(StandardCharsets.UTF_8);
    private static final int FOOTER_LENGTH_SIZE = 4;
    private static final int MAGIC_SIZE = 4;

    private final List<Path> remainingFiles;
    private final HardwoodContext context;
    private final ProjectedSchema projectedSchema;
    private final FileSchema referenceSchema;

    // Synchronization for file preparation
    private final Object preparationLock = new Object();
    private final AtomicBoolean preparationTriggered = new AtomicBoolean(false);
    private CompletableFuture<FileState> nextFileStateFuture;
    private FileState currentPreparedState;

    // Track which columns have consumed pages from current prepared file
    private final boolean[] columnsConsumed;
    private int columnsConsumedCount = 0;

    // Channels to close after all pages consumed
    private final List<FileChannel> channelsToClose = new ArrayList<>();

    /**
     * Creates a coordinator for the remaining files after the first file.
     *
     * @param remainingFiles files to prepare (excluding the already-opened first file)
     * @param context the hardwood context with executor and decompressor
     * @param projectedSchema the projected schema for column mapping
     * @param referenceSchema the schema from the first file for validation
     */
    public CrossFilePrefetchCoordinator(List<Path> remainingFiles, HardwoodContext context,
                                        ProjectedSchema projectedSchema, FileSchema referenceSchema) {
        this.remainingFiles = new ArrayList<>(remainingFiles);
        this.context = context;
        this.projectedSchema = projectedSchema;
        this.referenceSchema = referenceSchema;
        this.columnsConsumed = new boolean[projectedSchema.getProjectedColumnCount()];
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
        if (!remainingFiles.isEmpty()) {
            Path nextPath = remainingFiles.get(0);
            LOG.log(System.Logger.Level.DEBUG,
                    "Pre-triggering preparation of next file: {0}", nextPath);
            preparationTriggered.set(true);
            nextFileStateFuture = CompletableFuture.supplyAsync(
                    () -> prepareFile(nextPath), context.executor());
        }
    }

    /**
     * Prepares a file by reading metadata and scanning pages for all projected columns.
     */
    private FileState prepareFile(Path path) {
        try {
            FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
            try {
                FileMetaData fileMetaData = readMetadata(channel, path);
                FileSchema fileSchema = FileSchema.fromSchemaElements(fileMetaData.schema());

                // Validate schema compatibility
                validateSchemaCompatibility(path, fileSchema);

                // Scan pages for each projected column
                List<List<PageInfo>> pageInfosByColumn = scanAllProjectedColumns(channel, fileMetaData, fileSchema);

                return new FileState(path, channel, fileMetaData, fileSchema, pageInfosByColumn);
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
        catch (IOException e) {
            throw new UncheckedIOException("Failed to prepare file: " + path, e);
        }
    }

    /**
     * Reads file metadata from the Parquet file.
     */
    private FileMetaData readMetadata(FileChannel channel, Path path) throws IOException {
        long fileSize = channel.size();
        if (fileSize < MAGIC_SIZE + MAGIC_SIZE + FOOTER_LENGTH_SIZE) {
            throw new IOException("File too small to be a valid Parquet file: " + path);
        }

        // Read and validate magic number at start
        ByteBuffer startMagicBuf = ByteBuffer.allocate(MAGIC_SIZE);
        readFully(channel, startMagicBuf, 0);
        if (!Arrays.equals(startMagicBuf.array(), MAGIC)) {
            throw new IOException("Not a Parquet file (invalid magic number at start): " + path);
        }

        // Read footer size and magic number at end
        long footerInfoPos = fileSize - MAGIC_SIZE - FOOTER_LENGTH_SIZE;
        ByteBuffer footerInfoBuf = ByteBuffer.allocate(FOOTER_LENGTH_SIZE + MAGIC_SIZE);
        readFully(channel, footerInfoBuf, footerInfoPos);
        footerInfoBuf.order(ByteOrder.LITTLE_ENDIAN);
        int footerLength = footerInfoBuf.getInt();
        byte[] endMagic = new byte[MAGIC_SIZE];
        footerInfoBuf.get(endMagic);
        if (!Arrays.equals(endMagic, MAGIC)) {
            throw new IOException("Not a Parquet file (invalid magic number at end): " + path);
        }

        // Validate footer length
        long footerStart = fileSize - MAGIC_SIZE - FOOTER_LENGTH_SIZE - footerLength;
        if (footerStart < MAGIC_SIZE) {
            throw new IOException("Invalid footer length: " + footerLength);
        }

        // Read footer
        ByteBuffer footerBuf = ByteBuffer.allocate(footerLength);
        readFully(channel, footerBuf, footerStart);

        // Parse file metadata
        ThriftCompactReader reader = new ThriftCompactReader(new ByteArrayInputStream(footerBuf.array()));
        return FileMetaDataReader.read(reader);
    }

    /**
     * Reads from channel at position until buffer is full.
     */
    private static void readFully(FileChannel channel, ByteBuffer buffer, long position) throws IOException {
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer, position + buffer.position());
            if (read < 0) {
                throw new IOException("Unexpected end of file");
            }
        }
        buffer.flip();
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
     */
    private List<List<PageInfo>> scanAllProjectedColumns(FileChannel channel, FileMetaData fileMetaData,
                                                         FileSchema fileSchema) {
        int projectedColumnCount = projectedSchema.getProjectedColumnCount();
        List<RowGroup> rowGroups = fileMetaData.rowGroups();

        @SuppressWarnings("unchecked")
        CompletableFuture<List<PageInfo>>[] scanFutures = new CompletableFuture[projectedColumnCount];

        for (int projectedIndex = 0; projectedIndex < projectedColumnCount; projectedIndex++) {
            final int projIdx = projectedIndex;
            final int originalIndex = projectedSchema.toOriginalIndex(projectedIndex);
            final ColumnSchema refColumn = referenceSchema.getColumn(originalIndex);

            // Find the column index in this file (may differ if schema order varies)
            final ColumnSchema fileColumn = fileSchema.getColumn(refColumn.name());
            final int fileColumnIndex = fileColumn.columnIndex();

            scanFutures[projIdx] = CompletableFuture.supplyAsync(() -> {
                List<PageInfo> columnPages = new ArrayList<>();
                for (RowGroup rowGroup : rowGroups) {
                    ColumnChunk columnChunk = rowGroup.columns().get(fileColumnIndex);
                    PageScanner scanner = new PageScanner(channel, fileColumn, columnChunk, context);
                    try {
                        columnPages.addAll(scanner.scanPages());
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException(
                                "Failed to scan pages for column " + fileColumn.name(), e);
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
