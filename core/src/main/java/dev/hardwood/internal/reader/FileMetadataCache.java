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
import java.util.concurrent.FutureTask;

import dev.hardwood.InputFile;
import dev.hardwood.MetadataSource;
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.predicate.BoundsReadability;
import dev.hardwood.internal.reader.ParquetMetadataReader.FooterRead;
import dev.hardwood.jfr.FileOpenedEvent;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.StaleMetadataException;
import dev.hardwood.schema.FileSchema;

/// Lazily opens input files and caches each parsed footer for one
/// [dev.hardwood.reader.ParquetFileReader].
///
/// A footer a caller asks for is loaded on the caller's thread, and the load registers first, so
/// a later request for the same file joins it and [#close()] waits for it. [#prefetch] reads a
/// footer ahead of the read on the context's I/O executor, as far as I/O goes
/// ([ParquetMetadataReader#readFooter]); the load of that file then parses what it read. A
/// caller of the load may be a task of the context's pool, which must not wait for a parse
/// queued behind it on that pool, so the prefetch does no parsing. A prefetch that failed and
/// that no load took is reported by [#close()], as [PrefetchTasks] reports a failed task.
public final class FileMetadataCache {

    private final List<InputFile> inputFiles;
    private final MetadataSource metadataSource;
    private final HardwoodContextImpl context;
    private final Object lifecycleLock = new Object();
    private final Map<Integer, Load> loads = new HashMap<>();
    /// Footer reads [#prefetch] started that no load has taken yet. Guarded by `lifecycleLock`.
    private final Map<Integer, CompletableFuture<OpenedFooter>> prefetched = new HashMap<>();
    private final CompletableFuture<Void> closeCompletion = new CompletableFuture<>();
    private boolean closeStarted;

    /// @param context the read's context: its source of footers, the I/O executor the reads
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
        loads.put(0, Load.of(PreparedFile.of(this.inputFiles.getFirst(), firstFileFooter)));
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
    /// it waits for. A thread of the context's pool that waits for a load runs the load's parse
    /// in place unless a thread has started it ([Load]).
    ///
    /// A failed load arrives twice-wrapped and leaves as neither. The loader cannot
    /// declare `IOException`, since its outcome goes to every caller through a future, so it wraps;
    /// [CompletableFuture#join] then wraps whatever it finds in a
    /// [CompletionException]. This is the frame that can say what went wrong, so
    /// both are undone here and the original failure is what a caller catches.
    PreparedFile getFile(int fileIndex) throws IOException {
        Registration registration = registerLoad(fileIndex);
        Load load = registration.load();
        if (registration.isNew()) {
            load(fileIndex, load);
        }
        else if (context.runsInPlace()) {
            load.runHandedOffParse();
        }
        try {
            return load.outcome.join();
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

    /// Starts reading the footer of `fileIndex` on the I/O executor, unless a load or a read is
    /// registered, the index is past the last file, the cache is closed, or the file's bytes are in
    /// memory and the footer is read from the file or the context has no pool. A footer a
    /// [MetadataSource] supplies is prefetched on a context with a pool, since the source may wait
    /// for a remote store.
    void prefetch(int fileIndex) {
        if (fileIndex >= 0 && fileIndex < inputFiles.size()
                && RangeRead.inMemory(inputFiles.get(fileIndex))
                && (!footersSupplied() || !context.hasPool())) {
            // Its read waits for nothing, so the load on the thread that asks for the file reads it
            return;
        }
        CompletableFuture<OpenedFooter> read = new CompletableFuture<>();
        synchronized (lifecycleLock) {
            if (closeStarted || fileIndex < 0 || fileIndex >= inputFiles.size()
                    || loads.containsKey(fileIndex) || prefetched.containsKey(fileIndex)) {
                return;
            }
            prefetched.put(fileIndex, read);
        }
        InputFile inputFile = inputFiles.get(fileIndex);
        try {
            context.ioExecutor().execute(() -> {
                try {
                    read.complete(readFooter(inputFile));
                }
                catch (RuntimeException | Error e) {
                    read.completeExceptionally(e);
                }
            });
        }
        catch (RuntimeException | Error e) {
            // A read that never starts must not leave its load, and close, waiting forever. The
            // failure goes to this caller, so close does not report it again.
            synchronized (lifecycleLock) {
                prefetched.remove(fileIndex);
            }
            read.completeExceptionally(e);
            throw e;
        }
    }

    /// Prevents new loads, waits for every admitted load, and releases cached
    /// results. Input-file ownership remains with the caller.
    ///
    /// A prefetched read no load took that failed is reported as [PrefetchTasks] reports a task's
    /// failure: a `RuntimeException` is logged, and an `Error` thrown once the cache is released.
    ///
    /// @throws Error the `Error` of the first such read, the others suppressed beneath it
    public void close() {
        List<Load> registeredLoads;
        synchronized (lifecycleLock) {
            if (closeStarted) {
                registeredLoads = null;
            }
            else {
                closeStarted = true;
                registeredLoads = List.copyOf(loads.values());
            }
        }

        if (registeredLoads == null) {
            closeCompletion.join();
            return;
        }

        Error failure = null;
        try {
            for (Load load : registeredLoads) {
                try {
                    load.outcome.join();
                }
                catch (CompletionException e) {
                    // A load reported its failure to the caller that asked for it
                }
            }
            // After the loads, each of which took its read off the map: the reads left over hold
            // their files, and no load reported their failures
            List<CompletableFuture<OpenedFooter>> untakenReads;
            synchronized (lifecycleLock) {
                untakenReads = List.copyOf(prefetched.values());
            }
            for (CompletableFuture<OpenedFooter> read : untakenReads) {
                try {
                    read.join();
                }
                catch (CompletionException e) {
                    failure = PrefetchTasks.report(e.getCause(), failure);
                }
            }
        }
        finally {
            synchronized (lifecycleLock) {
                loads.clear();
                prefetched.clear();
            }
            closeCompletion.complete(null);
        }
        if (failure != null) {
            throw failure;
        }
    }

    /// Returns the load registered for `fileIndex`, registering a new one, which the caller
    /// runs, if there is none.
    private Registration registerLoad(int fileIndex) {
        synchronized (lifecycleLock) {
            if (closeStarted) {
                throw new IllegalStateException("FileMetadataCache is closed");
            }
            Objects.checkIndex(fileIndex, inputFiles.size());
            Load registered = loads.get(fileIndex);
            if (registered != null) {
                return new Registration(registered, false);
            }
            Load load = new Load();
            loads.put(fileIndex, load);
            return new Registration(load, true);
        }
    }

    /// Runs the load of `fileIndex` and completes `load` with its outcome.
    private void load(int fileIndex, Load load) {
        try {
            load.outcome.complete(loadFile(fileIndex, load));
        }
        catch (RuntimeException | Error e) {
            load.outcome.completeExceptionally(e);
        }
        finally {
            load.handOff.complete(null);
        }
    }

    /// A file's registered load, and whether this registration created it.
    private record Registration(Load load, boolean isNew) {
    }

    /// A file's load: its outcome, and the parse of its footer a thread off the context's pool
    /// handed to the pool. A thread of the pool may wait for the load, such as a column's fetch
    /// step reaching the file; the pool may then have no other thread to run the parse, so the
    /// waiting thread runs it in place unless a thread has started it.
    private static final class Load {

        final CompletableFuture<PreparedFile> outcome = new CompletableFuture<>();
        /// The parse handed to the pool, once the load has read the footer; `null` when the load
        /// hands none over.
        final CompletableFuture<FutureTask<FileFooter>> handOff = new CompletableFuture<>();

        static Load of(PreparedFile file) {
            Load load = new Load();
            load.outcome.complete(file);
            load.handOff.complete(null);
            return load;
        }

        /// Waits until the load has read the footer and runs the parse it handed over, unless a
        /// thread has started it.
        void runHandedOffParse() {
            FutureTask<FileFooter> parse = handOff.join();
            if (parse != null) {
                parse.run();
            }
        }
    }

    /// The footer read [#prefetch] started for `fileIndex`, once it completed; `null` if none was
    /// started. A failed read fails the load with its failure.
    private OpenedFooter takePrefetched(int fileIndex) {
        CompletableFuture<OpenedFooter> read;
        synchronized (lifecycleLock) {
            read = prefetched.remove(fileIndex);
        }
        return read == null ? null : read.join();
    }

    private PreparedFile loadFile(int fileIndex, Load load) {
        InputFile inputFile = inputFiles.get(fileIndex);
        OpenedFooter opened;
        try {
            opened = takePrefetched(fileIndex);
        }
        catch (CompletionException e) {
            // Thrown as the read threw it; getFile unwraps it again
            throw e.getCause() instanceof RuntimeException runtime ? runtime : e;
        }
        if (opened == null) {
            opened = readFooter(inputFile);
        }
        FileFooter footer = parse(opened.read(), load);
        FileOpenedEvent event = opened.event();

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

    /// Parses `read` on the context's pool: in place on a thread of the pool or on a context
    /// without one, and otherwise as a task `load` hands over ([Load]).
    private FileFooter parse(FooterRead read, Load load) {
        if (read.supplied() != null || context.runsInPlace()) {
            return read.parse(context);
        }
        FutureTask<FileFooter> parse = new FutureTask<>(() -> read.parse(context));
        load.handOff.complete(parse);
        return context.awaitFirst(parse);
    }

    /// A footer as [#readFooter] read it, and the event that spans its load from the open of the
    /// file, which the load that parses it commits.
    private record OpenedFooter(FooterRead read, FileOpenedEvent event) {
    }

    /// Opens `inputFile` and reads its footer as far as I/O goes.
    private OpenedFooter readFooter(InputFile inputFile) {
        FileOpenedEvent event = new FileOpenedEvent();
        event.begin();
        try {
            ParquetMetadataReader.open(inputFile, metadataSource);
        }
        catch (IOException e) {
            throw new UncheckedIOException(
                    ExceptionContext.filePrefix(inputFile.name()) + "Failed to open file", e);
        }
        try {
            return new OpenedFooter(ParquetMetadataReader.readFooter(inputFile, metadataSource), event);
        }
        catch (StaleMetadataException e) {
            // Carried as the cause alone, so that getFile unwraps it to the exception itself.
            throw new UncheckedIOException(e);
        }
        catch (IOException e) {
            throw new UncheckedIOException(
                    ExceptionContext.filePrefix(inputFile.name()) + "Failed to read metadata", e);
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
