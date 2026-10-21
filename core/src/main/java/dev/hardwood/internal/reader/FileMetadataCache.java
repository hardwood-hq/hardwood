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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import dev.hardwood.InputFile;
import dev.hardwood.MetadataSource;
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.predicate.BoundsReadability;
import dev.hardwood.jfr.FileOpenedEvent;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.StaleMetadataException;
import dev.hardwood.schema.FileSchema;

/// Lazily opens input files and caches each parsed footer for one
/// [dev.hardwood.reader.ParquetFileReader].
///
/// A footer a caller asks for is loaded on the caller's thread; one [#prefetch] asks for ahead of
/// the read is loaded on the context's I/O executor. Either way the load registers first, so a
/// later request for the same file joins it and [#close()] waits for it. A prefetched load that
/// failed and that no caller asked for is reported by [#close()], as [PrefetchTasks] reports a
/// failed task.
public final class FileMetadataCache {

    private final List<InputFile> inputFiles;
    private final MetadataSource metadataSource;
    private final HardwoodContextImpl context;
    private final Object lifecycleLock = new Object();
    private final Map<Integer, CompletableFuture<PreparedFile>> fileFutures = new HashMap<>();
    /// Prefetched loads no caller has asked for, whose failure [#close()] reports. Guarded by
    /// `lifecycleLock`.
    private final Set<CompletableFuture<PreparedFile>> unasked = new HashSet<>();
    private final CompletableFuture<Void> closeCompletion = new CompletableFuture<>();
    private boolean closeStarted;

    /// @param context the read's context: its source of footers, the I/O executor the loads
    ///        [#prefetch] starts run on, and the pool each footer is parsed on
    FileMetadataCache(List<InputFile> inputFiles, HardwoodContextImpl context) {
        if (inputFiles.isEmpty()) {
            throw new IllegalArgumentException("At least one file must be provided");
        }
        this.inputFiles = List.copyOf(inputFiles);
        this.metadataSource = context.metadataSource();
        this.context = context;
    }

    /// Seeds the first file's footer, already loaded by the owning
    /// [dev.hardwood.reader.ParquetFileReader], so that opening the reader and
    /// inspecting index `0` never load it twice.
    public FileMetadataCache(List<InputFile> inputFiles, FileFooter firstFileFooter,
                             HardwoodContextImpl context) {
        this(inputFiles, context);
        fileFutures.put(0, CompletableFuture.completedFuture(
                PreparedFile.of(this.inputFiles.getFirst(), firstFileFooter)));
    }

    List<InputFile> inputFiles() {
        return inputFiles;
    }

    /// Whether a [MetadataSource] supplied the footers rather than each being read from its file.
    /// It supplies every file's footer or none: [ParquetMetadataReader#load] takes the footer from
    /// a source it is given and fails the file when the source returns none.
    boolean footersSupplied() {
        return metadataSource != ParquetMetadataReader.FROM_FILE;
    }

    public FileMetaData getFileMetaData(int fileIndex) throws IOException {
        return getFile(fileIndex).metaData();
    }

    /// Gets a prepared file, loading it on the calling thread unless a load is registered, which
    /// it waits for.
    ///
    /// A failed load arrives twice-wrapped and leaves as neither. The loader cannot
    /// declare `IOException`, since a prefetch runs it as a task, so it wraps;
    /// [CompletableFuture#join] then wraps whatever it finds in a
    /// [CompletionException]. This is the frame that can say what went wrong, so
    /// both are undone here and the original failure is what a caller catches.
    PreparedFile getFile(int fileIndex) throws IOException {
        Registration registration = registerLoad(fileIndex, true);
        CompletableFuture<PreparedFile> future = registration.future();
        if (registration.isNew()) {
            load(fileIndex, future);
        }
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

    /// Starts loading the footer of `fileIndex` on the I/O executor, unless a load is registered,
    /// the index is past the last file, or the cache is closed.
    void prefetch(int fileIndex) {
        Registration registration = registerLoad(fileIndex, false);
        if (registration == null || !registration.isNew()) {
            return;
        }
        CompletableFuture<PreparedFile> future = registration.future();
        try {
            context.ioExecutor().execute(() -> load(fileIndex, future));
        }
        catch (RuntimeException | Error e) {
            // A load that never starts must not leave its waiters, and close, waiting forever.
            // The failure goes to this caller, so close does not report it again.
            synchronized (lifecycleLock) {
                unasked.remove(future);
            }
            future.completeExceptionally(e);
            throw e;
        }
    }

    /// Prevents new loads, waits for every admitted load, and releases cached
    /// results. Input-file ownership remains with the caller.
    ///
    /// A prefetched load no caller asked for that failed is reported as [PrefetchTasks] reports a
    /// task's failure: a `RuntimeException` is logged, and an `Error` thrown once the cache is
    /// released.
    ///
    /// @throws Error the `Error` of the first such load, the others suppressed beneath it
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

        Error failure = null;
        try {
            for (CompletableFuture<PreparedFile> future : registeredLoads) {
                try {
                    future.join();
                }
                catch (CompletionException e) {
                    // A load a caller asked for reported its failure to that caller.
                    if (wasUnasked(future)) {
                        failure = PrefetchTasks.report(e.getCause(), failure);
                    }
                }
            }
        }
        finally {
            synchronized (lifecycleLock) {
                fileFutures.clear();
                unasked.clear();
            }
            closeCompletion.complete(null);
        }
        if (failure != null) {
            throw failure;
        }
    }

    private boolean wasUnasked(CompletableFuture<PreparedFile> future) {
        synchronized (lifecycleLock) {
            return unasked.contains(future);
        }
    }

    /// Returns the load registered for `fileIndex`, registering a new one, which the caller
    /// runs, if there is none; `null` when a load is not `required` and the cache is closed or
    /// the index out of range. A load not `required` is a prefetch, which no caller has asked for
    /// until a `required` registration finds it.
    private Registration registerLoad(int fileIndex, boolean required) {
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
            CompletableFuture<PreparedFile> registered = fileFutures.get(fileIndex);
            if (registered != null) {
                if (required) {
                    unasked.remove(registered);
                }
                return new Registration(registered, false);
            }
            CompletableFuture<PreparedFile> future = new CompletableFuture<>();
            fileFutures.put(fileIndex, future);
            if (!required) {
                unasked.add(future);
            }
            return new Registration(future, true);
        }
    }

    /// Runs the load of `fileIndex` and completes `future` with its outcome.
    private void load(int fileIndex, CompletableFuture<PreparedFile> future) {
        try {
            future.complete(loadFile(fileIndex));
        }
        catch (RuntimeException | Error e) {
            future.completeExceptionally(e);
        }
    }

    /// A file's registered load, and whether this registration created it.
    private record Registration(CompletableFuture<PreparedFile> future, boolean isNew) {
    }

    private PreparedFile loadFile(int fileIndex) {
        InputFile inputFile = inputFiles.get(fileIndex);
        FileOpenedEvent event = new FileOpenedEvent();
        event.begin();
        try {
            ParquetMetadataReader.open(inputFile, metadataSource);
        }
        catch (IOException e) {
            throw new UncheckedIOException(
                    ExceptionContext.filePrefix(inputFile.name()) + "Failed to open file", e);
        }

        FileFooter footer;
        try {
            footer = ParquetMetadataReader.load(inputFile, metadataSource, context);
        }
        catch (StaleMetadataException e) {
            // Carried as the cause alone, so that getFile unwraps it to the exception itself.
            throw new UncheckedIOException(e);
        }
        catch (IOException e) {
            throw new UncheckedIOException(
                    ExceptionContext.filePrefix(inputFile.name()) + "Failed to read metadata", e);
        }

        try {
            event.file = inputFile.name();
            event.fileSize = inputFile.length();
            event.rowGroupCount = footer.metaData().rowGroups().size();
            event.columnCount = footer.schema().getColumnCount();
            event.commit();

            return PreparedFile.of(inputFile, footer);
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

        static PreparedFile of(InputFile inputFile, FileFooter footer) {
            FileMetaData metaData = footer.metaData();
            return new PreparedFile(inputFile, metaData, footer.schema(), metaData.rowGroups(),
                    BoundsReadability.of(footer.schema(), footer.readFooter()));
        }
    }
}
