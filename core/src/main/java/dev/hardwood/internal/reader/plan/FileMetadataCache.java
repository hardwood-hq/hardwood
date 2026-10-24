/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.plan;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import dev.hardwood.InputFile;
import dev.hardwood.MetadataSource;
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.predicate.BoundsReadability;
import dev.hardwood.internal.reader.fetch.PrefetchTasks;
import dev.hardwood.internal.reader.fetch.RangeRead;
import dev.hardwood.internal.reader.fetch.TailedInputFile;
import dev.hardwood.internal.reader.footer.FileFooter;
import dev.hardwood.internal.reader.footer.ParquetMetadataReader;
import dev.hardwood.internal.reader.footer.ParquetMetadataReader.FooterRead;
import dev.hardwood.internal.reader.step.Need;
import dev.hardwood.internal.util.Lazy;
import dev.hardwood.jfr.FileOpenedEvent;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.StaleMetadataException;
import dev.hardwood.schema.FileSchema;

/// Lazily opens input files and caches each parsed footer for one
/// [dev.hardwood.reader.ParquetFileReader].
///
/// Each file has one footer entry: the read of its footer, which opens the file and takes its tail
/// ([ParquetMetadataReader#readFooter]), and the load that parses it. A footer a caller asks for is
/// loaded on the caller's thread, and the load registers first, so a later request for the same
/// file joins it and [#close()] waits for it. A read starts the footer read ahead of the load
/// ([#prefetch]) as a task of its own [PrefetchTasks], whose close waits for it, as for any read of
/// the read; the load of that file then parses what it read, on the read's planning chain, which
/// never waits for the read itself. A read's failure is kept by its entry and raised by the load of
/// the file, when a read reaches it; an `Error` it failed with that no load took is raised by the
/// close of the read that started it, as for a [RangeRead]. A footer read its read's close dropped
/// before it started is no read: the next request starts it again.
///
/// A read drops the footer of each file it is done with, but the first's ([#drop]): a read of many
/// files holds the footers of the files it is reading, not of every file it has read. A later read
/// of the same reader reads a dropped footer again.
public final class FileMetadataCache {

    private static final Lazy<System.Logger> LOG = Lazy.of(() -> System.getLogger(FileMetadataCache.class.getName()));

    private final List<InputFile> inputFiles;
    private final MetadataSource metadataSource;
    private final HardwoodContextImpl context;
    private final Object lifecycleLock = new Object();
    /// The footer entries, by file. Guarded by `lifecycleLock`.
    private final Map<Integer, Footer> footers = new HashMap<>();
    private final CompletableFuture<Void> closeCompletion = new CompletableFuture<>();
    private boolean closeStarted;

    /// @param context the read's context: its source of footers, and the pool each footer is
    ///        parsed on
    public FileMetadataCache(List<InputFile> inputFiles, HardwoodContextImpl context) {
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
    ///
    /// @param firstFile the first file as its footer read left it, with its tail
    public FileMetadataCache(List<InputFile> inputFiles, InputFile firstFile, FileFooter firstFileFooter,
                             HardwoodContextImpl context) {
        this(inputFiles, context);
        footers.put(0, Footer.of(PreparedFile.of(firstFile, firstFileFooter)));
    }

    List<InputFile> inputFiles() {
        return inputFiles;
    }

    /// Whether a [MetadataSource] supplied the footers rather than each being read from its file.
    /// It supplies every file's footer or none: [ParquetMetadataReader#readFooter] takes the footer
    /// from a source it is given and fails the file when the source returns none.
    boolean footersSupplied() {
        return metadataSource != ParquetMetadataReader.FROM_FILE;
    }

    public FileMetaData getFileMetaData(int fileIndex) throws IOException {
        return getFile(fileIndex).metaData();
    }

    /// Gets a prepared file, loading it on the calling thread unless a load is registered, which
    /// it waits for. Called by a caller outside a read; the read's planning chain, which runs on
    /// the pool, never waits for a load ([#fileNeed]).
    ///
    /// A failed load arrives twice-wrapped and leaves as neither. The loader cannot
    /// declare `IOException`, since its outcome goes to every caller through a future, so it wraps;
    /// [CompletableFuture#join] then wraps whatever it finds in a
    /// [CompletionException]. This is the frame that can say what went wrong, so
    /// both are undone here and the original failure is what a caller catches.
    public PreparedFile getFile(int fileIndex) throws IOException {
        Footer footer;
        boolean loads;
        synchronized (lifecycleLock) {
            if (closeStarted) {
                throw new IllegalStateException("FileMetadataCache is closed");
            }
            Objects.checkIndex(fileIndex, inputFiles.size());
            footer = footers.computeIfAbsent(fileIndex, i -> new Footer());
            loads = !footer.loading;
            footer.loading = true;
        }
        if (loads) {
            load(fileIndex, footer);
        }
        return outcome(footer);
    }

    /// What the read's planning chain waits for before it takes the prepared file of `fileIndex`
    /// ([#getFile]) without waiting: the footer's read, which this starts on `tasks`, the read's,
    /// if none is, or another thread's load of it; `null` once the file is loaded. Once the footer
    /// has been read, or where the file's footer is read in place, the chain loads the file here,
    /// on its own thread. A caller outside a read takes the load on itself instead ([#getFile]),
    /// so that a close waits for it.
    Need fileNeed(int fileIndex, PrefetchTasks tasks) {
        Footer footer;
        CompletableFuture<?> pending;
        boolean loads = false;
        synchronized (lifecycleLock) {
            if (closeStarted) {
                throw new IllegalStateException("FileMetadataCache is closed");
            }
            Objects.checkIndex(fileIndex, inputFiles.size());
            footer = footers.computeIfAbsent(fileIndex, i -> new Footer());
            if (footer.loading) {
                pending = footer.outcome;
            }
            else if (readsInPlace(fileIndex) || footer.read != null && footer.read.result.isDone()) {
                footer.loading = true;
                loads = true;
                pending = null;
            }
            else {
                pending = footer.read != null ? footer.read.result : null;
            }
        }
        if (loads) {
            load(fileIndex, footer);
            return null;
        }
        if (pending == null) {
            CompletableFuture<?> started = prefetch(fileIndex, tasks, () -> {});
            // Null where another thread registered the read or the load since: look again
            return started != null ? whenDone(started) : fileNeed(fileIndex, tasks);
        }
        return pending.isDone() ? null : whenDone(pending);
    }

    /// The prepared file of `fileIndex` once loaded, or what the read's planning chain waits for
    /// before it is ([#fileNeed]): never a load that waits for I/O on the chain's thread, also
    /// where another read drops the file's footer meanwhile, which starts its read again.
    ///
    /// @throws IOException the load's failure
    Loading fileForPlanning(int fileIndex, PrefetchTasks tasks) throws IOException {
        while (true) {
            Need need = fileNeed(fileIndex, tasks);
            if (need != null) {
                return new Loading(null, need);
            }
            Footer footer;
            synchronized (lifecycleLock) {
                footer = footers.get(fileIndex);
            }
            if (footer != null && footer.outcome.isDone()) {
                return new Loading(outcome(footer), null);
            }
        }
    }

    /// The prepared file of a footer loaded, or what loading it waits for.
    record Loading(PreparedFile file, Need need) {
    }

    /// A need met once `future` completes, failed or not.
    private static Need whenDone(CompletableFuture<?> future) {
        return resume -> future.whenComplete((ignored, failure) -> resume.run());
    }

    /// The outcome of `footer`'s load, which has completed or is waited for.
    private static PreparedFile outcome(Footer footer) throws IOException {
        try {
            return footer.outcome.join();
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

    /// Starts reading the footer of `fileIndex` as a task of `tasks`, a read's, unless its entry has
    /// started or loaded it, the index is past the last file, the cache is closed, or the file's
    /// bytes are in memory and the footer is read from the file or the context has no pool. A
    /// footer a [MetadataSource] supplies is read ahead on a context with a pool, since the source
    /// may wait for a remote store. Runs `whenRead` once the footer has been read, by the read this
    /// starts or by the entry's read or load already under way, on the thread that completes it,
    /// or at once on the caller's where it is read in place or already read. Not run once the
    /// cache is closed or for an index past the last file.
    ///
    /// @return the read this started, which completes once it has, whether or not it failed;
    ///         `null` when it started none
    public CompletableFuture<?> prefetch(int fileIndex, PrefetchTasks tasks, Runnable whenRead) {
        if (readsInPlace(fileIndex)) {
            // Its read waits for nothing, so the load on the thread that asks for the file reads it
            whenRead.run();
            return null;
        }
        FooterFetch read = new FooterFetch();
        Footer footer;
        CompletableFuture<?> registered;
        synchronized (lifecycleLock) {
            if (closeStarted || fileIndex < 0 || fileIndex >= inputFiles.size()) {
                return null;
            }
            footer = footers.computeIfAbsent(fileIndex, i -> new Footer());
            registered = footer.loading ? footer.outcome : footer.read != null ? footer.read.result : null;
            if (registered == null) {
                footer.read = read;
            }
        }
        if (registered != null) {
            registered.whenComplete((ignored, failure) -> whenRead.run());
            return null;
        }
        read.result.whenComplete((ignored, failure) -> whenRead.run());
        InputFile inputFile = inputFiles.get(fileIndex);
        tasks.submit(() -> read.run(inputFile, tasks), () -> {
            // Dropped by the close of the read that started it: no read, so a later request
            // starts another rather than taking a failure no file caused
            synchronized (lifecycleLock) {
                if (footer.read == read) {
                    footer.read = null;
                }
            }
            read.result.complete(null);
        });
        return read.result.handle((ignored, failure) -> null);
    }

    /// Drops the tail of `fileIndex` once its footer has been read ([TailedInputFile#dropTail]), and,
    /// but for the first file's, its footer once loaded, for a read that is done with the file or
    /// will not reach it.
    void drop(int fileIndex) {
        Footer footer;
        synchronized (lifecycleLock) {
            footer = footers.get(fileIndex);
            if (footer != null && fileIndex > 0 && footer.loading && footer.outcome.isDone()) {
                footers.remove(fileIndex);
            }
        }
        if (footer == null) {
            return;
        }
        if (footer.outcome.isDone() && !footer.outcome.isCompletedExceptionally()) {
            TailedInputFile.dropTail(footer.outcome.join().inputFile());
        }
        FooterFetch read = footer.read;
        if (read != null && read.result.isDone() && !read.result.isCompletedExceptionally()
                && read.result.join() != null) {
            TailedInputFile.dropTail(read.result.join().read().inputFile());
        }
    }

    /// Whether the cache holds a footer entry of `fileIndex`, for tests.
    boolean holdsFooter(int fileIndex) {
        synchronized (lifecycleLock) {
            return footers.containsKey(fileIndex);
        }
    }

    /// Whether the footer of `fileIndex` is read in place by its load, since the read waits for
    /// nothing: the file's bytes are in memory, and the footer is read from the file or the
    /// context has no pool.
    private boolean readsInPlace(int fileIndex) {
        return fileIndex >= 0 && fileIndex < inputFiles.size()
                && RangeRead.inMemory(inputFiles.get(fileIndex))
                && (!footersSupplied() || !context.hasPool());
    }

    /// Prevents new loads, waits for every admitted load, and releases cached
    /// results. Input-file ownership remains with the caller. The footer reads a read started are
    /// its own, which its close waited for.
    public void close() {
        List<Footer> registered;
        synchronized (lifecycleLock) {
            if (closeStarted) {
                registered = null;
            }
            else {
                closeStarted = true;
                registered = List.copyOf(footers.values());
            }
        }

        if (registered == null) {
            closeCompletion.join();
            return;
        }

        try {
            for (Footer footer : registered) {
                if (footer.loading) {
                    // A load reports its failure to the caller that asked for it
                    footer.outcome.exceptionally(failure -> null).join();
                }
            }
        }
        finally {
            synchronized (lifecycleLock) {
                footers.clear();
            }
            closeCompletion.complete(null);
        }
    }

    /// Runs the load of `fileIndex` and completes `footer` with its outcome.
    private void load(int fileIndex, Footer footer) {
        try {
            footer.outcome.complete(loadFile(fileIndex, footer));
        }
        catch (RuntimeException | Error e) {
            footer.outcome.completeExceptionally(e);
        }
    }

    /// A file's footer entry: the read of its footer, once [#prefetch] started it, and its load,
    /// once a caller or the read's planning chain asked for the file.
    private static final class Footer {

        final CompletableFuture<PreparedFile> outcome = new CompletableFuture<>();
        /// The read [#prefetch] started; `null` when none was. Written under `lifecycleLock`.
        volatile FooterFetch read;
        /// Whether a load has taken the entry. Guarded by `lifecycleLock`.
        boolean loading;

        static Footer of(PreparedFile file) {
            Footer footer = new Footer();
            footer.loading = true;
            footer.outcome.complete(file);
            return footer;
        }
    }

    /// A footer read [#prefetch] started on a read's tasks: completes with the footer as read,
    /// `null` when the read's close dropped it before it started, or the failure, whose `Error`
    /// the read's close raises unless a load took it, as a [RangeRead]'s.
    private final class FooterFetch implements PrefetchTasks.Untaken {

        final CompletableFuture<OpenedFooter> result = new CompletableFuture<>();
        private volatile Error untakenError;

        void run(InputFile inputFile, PrefetchTasks tasks) {
            try {
                result.complete(readFooter(inputFile));
            }
            catch (Throwable e) {
                // Ahead of the load: the load that takes the failure reports it, so DEBUG
                LOG.get().log(System.Logger.Level.DEBUG, "Footer read ahead failed for {0}", inputFile.name(), e);
                if (e instanceof Error error) {
                    // Before the completion, so a load that takes the failure finds it to clear
                    untakenError = error;
                    tasks.recordUntaken(this);
                }
                result.completeExceptionally(e);
            }
        }

        /// The footer as read, `null` when the read was dropped before it started.
        OpenedFooter take() {
            try {
                return result.join();
            }
            catch (CompletionException e) {
                if (e.getCause() instanceof Error) {
                    untakenError = null;
                }
                throw e;
            }
        }

        @Override
        public Error untakenError() {
            return untakenError;
        }
    }

    private PreparedFile loadFile(int fileIndex, Footer footer) {
        InputFile inputFile = inputFiles.get(fileIndex);
        FooterFetch read = footer.read;
        OpenedFooter opened;
        try {
            opened = read != null ? read.take() : null;
        }
        catch (CompletionException e) {
            // Thrown as the read threw it; getFile unwraps it again
            throw e.getCause() instanceof RuntimeException runtime ? runtime : e;
        }
        if (opened == null) {
            opened = readFooter(inputFile);
        }
        // In place on the planning chain, which runs on the pool or the read's queue; a caller
        // outside a read hands the parse to the pool and waits for it
        FileFooter parsed = opened.read().parse(context);
        FileOpenedEvent event = opened.event();

        try {
            event.file = inputFile.name();
            event.fileSize = inputFile.length();
            event.rowGroupCount = parsed.metaData().rowGroups().size();
            event.columnCount = parsed.schema().getColumnCount();
            event.commit();

            return PreparedFile.of(opened.read().inputFile(), parsed);
        }
        catch (IOException e) {
            throw new UncheckedIOException(
                    ExceptionContext.filePrefix(inputFile.name()) + "Failed to read metadata", e);
        }
        catch (RuntimeException e) {
            throw ExceptionContext.addFileContext(inputFile.name(), ExceptionContext.asReadFailure(e));
        }
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
