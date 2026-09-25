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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import dev.hardwood.InputFile;
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.predicate.BoundsReadability;
import dev.hardwood.internal.thrift.FileMetaDataReader.ReadFooter;
import dev.hardwood.jfr.FileOpenedEvent;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.schema.FileSchema;

/// Lazily opens input files and caches each parsed footer for one
/// [dev.hardwood.reader.ParquetFileReader].
public final class FileMetadataCache {

    private final List<InputFile> inputFiles;
    private final Object lifecycleLock = new Object();
    private final Map<Integer, CompletableFuture<PreparedFile>> fileFutures = new HashMap<>();
    private final CompletableFuture<Void> closeCompletion = new CompletableFuture<>();
    private boolean closeStarted;

    FileMetadataCache(List<InputFile> inputFiles) {
        if (inputFiles.isEmpty()) {
            throw new IllegalArgumentException("At least one file must be provided");
        }
        this.inputFiles = List.copyOf(inputFiles);
    }

    /// Seeds the first file's footer, already read by the owning
    /// [dev.hardwood.reader.ParquetFileReader], so that opening the reader and
    /// inspecting index `0` never read it twice.
    public FileMetadataCache(List<InputFile> inputFiles, ReadFooter firstFileFooter,
                             FileSchema firstFileSchema) {
        this(inputFiles);
        fileFutures.put(0, CompletableFuture.completedFuture(
                PreparedFile.of(this.inputFiles.getFirst(), firstFileFooter, firstFileSchema)));
    }

    List<InputFile> inputFiles() {
        return inputFiles;
    }

    public FileMetaData getFileMetaData(int fileIndex) throws IOException {
        return getFile(fileIndex).metaData();
    }

    /// Gets or loads a prepared file, blocking if necessary.
    ///
    /// A failed load arrives twice-wrapped and leaves as neither. The loader runs
    /// in a [java.util.function.Supplier], which cannot declare `IOException`, so
    /// it wraps; [CompletableFuture#join] then wraps whatever it finds in a
    /// [CompletionException]. This is the frame that can say what went wrong, so
    /// both are undone here and the original failure is what a caller catches.
    PreparedFile getFile(int fileIndex) throws IOException {
        CompletableFuture<PreparedFile> future = registerLoad(fileIndex, true);
        try {
            return future.join();
        }
        catch (CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof UncheckedIOException unchecked) {
                throw ExceptionContext.unwrap(unchecked);
            }
            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw e;
        }
    }

    void prefetch(int fileIndex) {
        registerLoad(fileIndex, false);
    }

    /// Prevents new loads, waits for every admitted load, and releases cached
    /// results. Input-file ownership remains with the caller.
    public void close() {
        List<CompletableFuture<PreparedFile>> registeredLoads;
        synchronized (lifecycleLock) {
            if (closeStarted) {
                registeredLoads = null;
            }
            else {
                closeStarted = true;
                registeredLoads = List.copyOf(fileFutures.values());
            }
        }

        if (registeredLoads == null) {
            closeCompletion.join();
            return;
        }

        try {
            for (CompletableFuture<PreparedFile> future : registeredLoads) {
                try {
                    future.join();
                }
                catch (RuntimeException ignored) {
                }
            }
        }
        finally {
            synchronized (lifecycleLock) {
                fileFutures.clear();
            }
            closeCompletion.complete(null);
        }
    }

    private CompletableFuture<PreparedFile> registerLoad(int fileIndex, boolean required) {
        synchronized (lifecycleLock) {
            if (closeStarted) {
                if (required) {
                    throw new IllegalStateException("FileMetadataCache is closed");
                }
                return null;
            }
            if (fileIndex < 0 || fileIndex >= inputFiles.size()) {
                if (required) {
                    Objects.checkIndex(fileIndex, inputFiles.size());
                }
                return null;
            }
            return fileFutures.computeIfAbsent(fileIndex, this::loadFileAsync);
        }
    }

    private CompletableFuture<PreparedFile> loadFileAsync(int fileIndex) {
        return CompletableFuture.supplyAsync(() -> loadFile(fileIndex));
    }

    private PreparedFile loadFile(int fileIndex) {
        InputFile inputFile = inputFiles.get(fileIndex);
        FileOpenedEvent event = new FileOpenedEvent();
        event.begin();
        try {
            inputFile.open();
        }
        catch (IOException e) {
            throw new UncheckedIOException(
                    ExceptionContext.filePrefix(inputFile.name()) + "Failed to open file", e);
        }

        try {
            ReadFooter footer = ParquetMetadataReader.readFooter(inputFile);
            FileMetaData metaData = footer.metaData();
            FileSchema schema = FileSchema.fromSchemaElements(metaData.schema());

            event.file = inputFile.name();
            event.fileSize = inputFile.length();
            event.rowGroupCount = metaData.rowGroups().size();
            event.columnCount = schema.getColumnCount();
            event.commit();

            return PreparedFile.of(inputFile, footer, schema);
        }
        catch (IOException e) {
            throw new UncheckedIOException(
                    ExceptionContext.filePrefix(inputFile.name()) + "Failed to read metadata", e);
        }
        catch (RuntimeException e) {
            throw ExceptionContext.addFileContext(inputFile.name(), ExceptionContext.asReadFailure(e));
        }
    }

    /// One file's reusable, projection-independent state. Every component is
    /// parsed from that file's own footer, so none of them is ever absent.
    ///
    /// @param boundsReadability which of the file's columns have `min` / `max` bounds
    ///        in an order this reader can read
    record PreparedFile(
            InputFile inputFile,
            FileMetaData metaData,
            FileSchema schema,
            List<RowGroup> rowGroups,
            BoundsReadability boundsReadability
    ) {
        PreparedFile {
            Objects.requireNonNull(inputFile, "inputFile");
            Objects.requireNonNull(metaData, "metaData");
            Objects.requireNonNull(schema, "schema");
            Objects.requireNonNull(rowGroups, "rowGroups");
            Objects.requireNonNull(boundsReadability, "boundsReadability");
        }

        static PreparedFile of(InputFile inputFile, ReadFooter footer, FileSchema schema) {
            FileMetaData metaData = footer.metaData();
            return new PreparedFile(inputFile, metaData, schema, metaData.rowGroups(),
                    BoundsReadability.of(schema, footer));
        }
    }
}
