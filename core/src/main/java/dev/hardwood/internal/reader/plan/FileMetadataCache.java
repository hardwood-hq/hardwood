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
import dev.hardwood.internal.io.HeldBytes;
import dev.hardwood.internal.predicate.BoundsReadability;
import dev.hardwood.internal.reader.fetch.PrefetchTasks;
import dev.hardwood.internal.reader.fetch.RequestAttempt;
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
/// close of the read that started it, as for every [RequestAttempt]. A footer read its read's close dropped
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

    public List<InputFile> inputFiles() {
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
        try {
            return outcome(footer);
        }
        catch (Abandoned e) {
            // The read engine that claimed the load closed before it read the footer, and dropped
            // the entry: the file is loaded afresh
            return getFile(fileIndex);
        }
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
            // One look at the read, which completes on its own thread: a read that fails after it
            // is waited for below, and taken by the next look, never loaded here
            boolean readDone = footer.attempt != null && footer.attempt.isDone();
            if (readDone && footer.attempt.failedAhead()) {
                // Read again on demand, as a data read that failed ahead of demand is
                footer.attempt = null;
                readDone = false;
            }
            if (footer.loading) {
                pending = footer.outcome;
            }
            else if (readsInPlace(fileIndex) || readDone) {
                footer.loading = true;
                loads = true;
                pending = null;
            }
            else {
                pending = footer.attempt != null ? footer.attempt.future() : null;
            }
        }
        if (loads) {
            load(fileIndex, footer);
            return null;
        }
        if (pending == null) {
            CompletableFuture<?> started = start(fileIndex, tasks, () -> {}, true);
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
        return start(fileIndex, tasks, whenRead, false);
    }

    /// As [#prefetch], for a read the read's planning chain `demanded`, whose failure fails the
    /// file, or one started ahead of demand, whose failure is read again once the file is needed.
    private CompletableFuture<?> start(int fileIndex, PrefetchTasks tasks, Runnable whenRead, boolean demanded) {
        if (readsInPlace(fileIndex)) {
            // Its read waits for nothing, so the load on the thread that asks for the file reads it
            whenRead.run();
            return null;
        }
        RequestAttempt<OpenedFooter> attempt;
        CompletableFuture<?> registered;
        synchronized (lifecycleLock) {
            if (closeStarted || fileIndex < 0 || fileIndex >= inputFiles.size()) {
                return null;
            }
            Footer footer = footers.computeIfAbsent(fileIndex, i -> new Footer());
            registered = footer.loading ? footer.outcome : footer.attempt != null ? footer.attempt.future() : null;
            attempt = null;
            if (registered == null) {
                attempt = footerAttempt(fileIndex, footer, tasks, demanded);
                footer.attempt = attempt;
            }
        }
        if (registered != null) {
            registered.whenComplete((ignored, failure) -> whenRead.run());
            return null;
        }
        attempt.whenDone(whenRead);
        attempt.submit();
        return attempt.future().handle((ignored, failure) -> null);
    }

    /// The read of the footer of `fileIndex` for `footer`, its entry, on `tasks`.
    private RequestAttempt<OpenedFooter> footerAttempt(int fileIndex, Footer footer, PrefetchTasks tasks, boolean demanded) {
        InputFile inputFile = inputFiles.get(fileIndex);
        return new RequestAttempt<>(ExceptionContext.filePrefix(inputFile.name()) + "Failed to read metadata", !demanded,
                tasks, () -> readFooter(inputFile), () -> {
                    // Dropped by the close of the read that started it: no read, so a later request
                    // starts another rather than taking a failure no file caused. The entry's read
                    // not completed is this one, since an entry takes another read only once its
                    // read has failed ahead of demand (fileNeed)
                    synchronized (lifecycleLock) {
                        if (footer.attempt != null && !footer.attempt.isDone()) {
                            footer.attempt = null;
                        }
                    }
                    return null;
                });
    }

    /// Drops the tail of `fileIndex` once its footer has been read ([HeldBytes#dropHeld]), and,
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
            HeldBytes.drop(footer.outcome.join().inputFile());
        }
        RequestAttempt<OpenedFooter> attempt = footer.attempt;
        if (attempt != null) {
            // A footer read in flight drops its tail once it has read it
            attempt.future().thenAccept(opened -> {
                if (opened != null) {
                    HeldBytes.drop(opened.read().inputFile());
                }
            });
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
                && HeldBytes.inMemory(inputFiles.get(fileIndex))
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
        volatile RequestAttempt<OpenedFooter> attempt;
        /// Whether a load has taken the entry. Guarded by `lifecycleLock`.
        boolean loading;

        static Footer of(PreparedFile file) {
            Footer footer = new Footer();
            footer.loading = true;
            footer.outcome.complete(file);
            return footer;
        }
    }

    private PreparedFile loadFile(int fileIndex, Footer footer) {
        InputFile inputFile = inputFiles.get(fileIndex);
        RequestAttempt<OpenedFooter> attempt = footer.attempt;
        OpenedFooter opened;
        try {
            opened = attempt != null && !attempt.failedAhead() ? attempt.take() : null;
        }
        catch (IOException e) {
            // A checked exception the file threw undeclared; getFile unwraps it again
            throw new UncheckedIOException(e);
        }
        if (opened == null) {
            opened = readFooter(inputFile);
        }
        return parsed(inputFile, opened);
    }

    /// The prepared file of `opened`, the footer read of `inputFile`: its parse, and the commit of
    /// the event that spans its load.
    private PreparedFile parsed(InputFile inputFile, OpenedFooter opened) {
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

    // ==================== Loads a read engine runs ====================

    /// What a read engine finds for a file's footer ([#claim]): the prepared file, or the failure,
    /// of a load that has completed; a load or a footer read of another read under way, `pending`,
    /// after which it asks again; or the load, claimed for the caller, which runs its read and
    /// parse as jobs of its own and completes it ([EngineLoad]). Exactly one is set.
    record Claim(PreparedFile file, Throwable failure, CompletableFuture<?> pending, EngineLoad load) {
    }

    /// Claims the load of `fileIndex` for a read engine, unless a load of it has completed or is
    /// under way, or another read's footer read of it is in flight ([Claim]). A footer read another
    /// read started ahead of demand that failed is read again by the claimed load; one that
    /// completed is taken by its parse.
    ///
    /// @throws IllegalStateException if the cache is closed
    Claim claim(int fileIndex) {
        Footer footer;
        synchronized (lifecycleLock) {
            if (closeStarted) {
                throw new IllegalStateException("FileMetadataCache is closed");
            }
            Objects.checkIndex(fileIndex, inputFiles.size());
            footer = footers.computeIfAbsent(fileIndex, i -> new Footer());
            if (!footer.loading) {
                RequestAttempt<OpenedFooter> attempt = footer.attempt;
                if (attempt != null && !attempt.isDone()) {
                    return new Claim(null, null, attempt.future(), null);
                }
                if (attempt != null && attempt.failedAhead()) {
                    footer.attempt = null;
                }
                footer.loading = true;
                return new Claim(null, null, null, new EngineLoad(fileIndex, footer, footer.attempt != null));
            }
            if (!footer.outcome.isDone()) {
                return new Claim(null, null, footer.outcome, null);
            }
        }
        try {
            return new Claim(outcome(footer), null, null, null);
        }
        catch (IOException | RuntimeException | Error e) {
            return new Claim(null, e, null, null);
        }
    }

    /// The load of one file's footer a read engine claimed ([#claim]): its read, a job on an I/O
    /// thread unless the footer is read in place ([#readsInPlace()]), and its parse, a CPU job, which
    /// completes the file's entry for every read of the reader. The engine runs them one after the
    /// other, never at once, and orders them through its lock.
    final class EngineLoad {

        private final int fileIndex;
        private final Footer footer;
        /// Whether a footer read another read started is taken rather than read again.
        private final boolean takesRead;
        private OpenedFooter opened;

        private EngineLoad(int fileIndex, Footer footer, boolean takesRead) {
            this.fileIndex = fileIndex;
            this.footer = footer;
            this.takesRead = takesRead;
        }

        /// Whether the read waits for nothing, so that the parse's job reads the footer too: the
        /// file's bytes are in memory, or another read's footer read has completed.
        boolean readsInPlace() {
            return takesRead || FileMetadataCache.this.readsInPlace(fileIndex);
        }

        /// Opens the file and reads its footer as far as I/O goes. Run again after a failure.
        ///
        /// @throws IOException the read's failure, naming the file
        void read() throws IOException {
            if (takesRead) {
                return;
            }
            try {
                opened = readFooter(inputFiles.get(fileIndex));
            }
            catch (UncheckedIOException e) {
                throw ExceptionContext.unwrap(e);
            }
        }

        /// Parses the footer read, reading it first where it is read in place, and completes the
        /// file's entry with the prepared file or the failure.
        ///
        /// @throws IOException the load's failure, as [#getFile] raises it
        PreparedFile parse() throws IOException {
            try {
                if (takesRead) {
                    footer.outcome.complete(loadFile(fileIndex, footer));
                }
                else {
                    if (opened == null) {
                        read();
                    }
                    footer.outcome.complete(parsed(inputFiles.get(fileIndex), opened));
                }
            }
            catch (IOException e) {
                footer.outcome.completeExceptionally(new UncheckedIOException(e));
            }
            catch (RuntimeException | Error e) {
                footer.outcome.completeExceptionally(e);
            }
            return outcome(footer);
        }

        /// Completes the file's entry with `failure`, the read's last.
        void fail(Exception failure) {
            footer.outcome.completeExceptionally(failure instanceof IOException io ? new UncheckedIOException(io) : failure);
        }

        /// Gives the load up as the engine's read closes before completing it: the entry is
        /// dropped, so that the next request for the file loads it afresh, and a caller waiting
        /// for it asks again.
        void abandon() {
            synchronized (lifecycleLock) {
                if (footers.get(fileIndex) == footer) {
                    footers.remove(fileIndex);
                }
            }
            footer.outcome.completeExceptionally(new Abandoned());
        }
    }

    /// The outcome of a load its read engine gave up, which [#getFile] takes as a reason to load
    /// the file afresh.
    private static final class Abandoned extends RuntimeException {

        Abandoned() {
            super("The footer load was abandoned by its read", null, false, false);
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
