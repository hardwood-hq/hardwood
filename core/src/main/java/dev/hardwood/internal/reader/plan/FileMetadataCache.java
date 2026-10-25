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
import dev.hardwood.internal.reader.footer.FileFooter;
import dev.hardwood.internal.reader.footer.ParquetMetadataReader;
import dev.hardwood.internal.reader.footer.ParquetMetadataReader.FooterRead;
import dev.hardwood.internal.util.Lazy;
import dev.hardwood.jfr.FileOpenedEvent;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.StaleMetadataException;
import dev.hardwood.schema.FileSchema;

/// Lazily opens input files and caches each parsed footer for one
/// [dev.hardwood.reader.ParquetFileReader].
///
/// Each file has one footer entry: its load, which opens the file, takes its tail and reads its
/// footer ([ParquetMetadataReader#readFooter]), then parses it. A footer a caller outside a read
/// asks for is loaded on the caller's thread ([#getFile]); a read claims the load instead and runs
/// its read and parse as jobs of its own ([#claim]). Either way the load registers first, so a
/// later request for the same file joins it and [#close()] waits for it.
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
    /// it waits for. Called by a caller outside a read; a read never waits for a load ([#claim]).
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
            // The read that claimed the load closed before it read the footer, and dropped
            // the entry: the file is loaded afresh
            return getFile(fileIndex);
        }
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
    /// results. Input-file ownership remains with the caller. A load a read claimed is the read's
    /// own, which its close completed or abandoned.
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
            footer.outcome.complete(loadFile(fileIndex));
        }
        catch (RuntimeException | Error e) {
            footer.outcome.completeExceptionally(e);
        }
    }

    /// A file's footer entry: its load, once a caller or a read asked for the file.
    private static final class Footer {

        final CompletableFuture<PreparedFile> outcome = new CompletableFuture<>();
        /// Whether a load has taken the entry. Guarded by `lifecycleLock`.
        boolean loading;

        static Footer of(PreparedFile file) {
            Footer footer = new Footer();
            footer.loading = true;
            footer.outcome.complete(file);
            return footer;
        }
    }

    private PreparedFile loadFile(int fileIndex) {
        InputFile inputFile = inputFiles.get(fileIndex);
        return parsed(inputFile, readFooter(inputFile));
    }

    /// The prepared file of `opened`, the footer read of `inputFile`: its parse, and the commit of
    /// the event that spans its load.
    private PreparedFile parsed(InputFile inputFile, OpenedFooter opened) {
        // In place on a read's planning job, which runs on the pool or the read's queue; a caller
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

    // ==================== Loads a read runs ====================

    /// What a read finds for a file's footer ([#claim]): the prepared file, or the failure,
    /// of a load that has completed; a load of another read or caller under way, `pending`, after
    /// which it asks again; or the load, claimed for the caller, which runs its read and parse as
    /// jobs of its own and completes it ([ClaimedLoad]). Exactly one is set.
    record Claim(PreparedFile file, Throwable failure, CompletableFuture<?> pending, ClaimedLoad load) {
    }

    /// Claims the load of `fileIndex` for a read, unless a load of it has completed or is
    /// under way ([Claim]).
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
                footer.loading = true;
                return new Claim(null, null, null, new ClaimedLoad(fileIndex, footer));
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

    /// The load of one file's footer a read claimed ([#claim]): its read, a job on an I/O
    /// thread unless the footer is read in place ([#readsInPlace()]), and its parse, a CPU job, which
    /// completes the file's entry for every read of the reader. The read runs them one after the
    /// other, never at once, and orders them through its lock.
    final class ClaimedLoad {

        private final int fileIndex;
        private final Footer footer;
        private OpenedFooter opened;

        private ClaimedLoad(int fileIndex, Footer footer) {
            this.fileIndex = fileIndex;
            this.footer = footer;
        }

        /// Whether the read waits for nothing, so that the parse's job reads the footer too: the
        /// file's bytes are in memory.
        boolean readsInPlace() {
            return FileMetadataCache.this.readsInPlace(fileIndex);
        }

        /// Opens the file and reads its footer as far as I/O goes. Run again after a failure.
        ///
        /// @throws IOException the read's failure, naming the file
        void read() throws IOException {
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
                if (opened == null) {
                    read();
                }
                footer.outcome.complete(parsed(inputFiles.get(fileIndex), opened));
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
        void fail(Throwable failure) {
            footer.outcome.completeExceptionally(failure instanceof IOException io ? new UncheckedIOException(io) : failure);
        }

        /// Gives the load up as the read closes before completing it: the entry is
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

    /// The outcome of a load its read gave up, which [#getFile] takes as a reason to load
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
