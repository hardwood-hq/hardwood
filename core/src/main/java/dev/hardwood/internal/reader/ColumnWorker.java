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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.LockSupport;

import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.compression.DecompressorFactory;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.schema.ColumnSchema;

/// Per-column pipeline that decodes pages in parallel and assembles batches.
///
/// Two long-lived virtual threads per column:
///
/// - **Retriever VThread:** Pulls [PageInfo] objects from a [PageSource],
///   submits decode tasks to the provided executor. Throttles itself
///   when the gap between submitted and drained pages reaches `MAX_INFLIGHT_PAGES`.
///
/// - **Drain VThread:** Reads decoded pages from a circular reorder buffer in
///   sequence order, assembles them into batches via subclass-specific logic,
///   and publishes to the [BatchExchange].
///
/// The reorder buffer is an [AtomicReferenceArray] indexed by
/// `seqNum % MAX_INFLIGHT_PAGES`. This avoids the GC pressure of
/// `ConcurrentHashMap` (no integer boxing, no Node allocations).
/// Decode tasks store their result via `set()` and unpark the drain thread.
///
/// @param <B> the batch type (e.g. [BatchExchange.Batch] for flat, [NestedBatch] for nested)
public abstract class ColumnWorker<B> implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(ColumnWorker.class.getName());

    /// Sentinel page stored in the reorder buffer to signal end-of-stream.
    private static final Page EMPTY_SENTINEL = new Page.IntPage(new int[0], null, null, 0, -1);

    private final PageSource pageSource;
    private final DecompressorFactory decompressorFactory;
    private final Executor decodeExecutor;

    final BatchExchange<B> exchange;
    final ColumnSchema column;
    final PhysicalType physicalType;
    final int batchCapacity;
    final int maxDefinitionLevel;

    // === Circular reorder buffer: decode tasks write, drain thread reads ===
    private final AtomicReferenceArray<Page> reorderBuffer;

    // === File name per reorder-buffer slot (retriever writes, drain reads) ===
    // Visibility: retriever writes fileNameBuffer[slot] before submitting the
    // decode task. The decode task's volatile write to reorderBuffer[slot]
    // happens-after the retriever's plain write. The drain's volatile read of
    // reorderBuffer[slot] sees the fileName via the happens-before chain.
    //
    // Slot reuse safety: the retriever may only reuse a slot once consumePosition
    // has advanced past it (throttle: nextSeq - consumePosition < MAX_INFLIGHT_PAGES).
    // drainReadyPages reads fileNameBuffer[slot] before incrementing consumePosition,
    // so the previous occupant's fileName is always read before being overwritten.
    // Any future change to the throttle or to the read-then-increment ordering must
    // preserve this invariant.
    private final String[] fileNameBuffer;

    // === Drain position (only modified by drain thread, read by retriever for throttle) ===
    private volatile int consumePosition;

    // === Pipeline control ===
    /// Set when the worker should stop, for any of three reasons: the consumer
    /// called [#close()], the drain reached natural EOF or the configured
    /// `maxRows` (via [#finishDrain()]), or an error was raised
    /// (via [#signalError(Throwable)]). Both VThreads exit promptly when set.
    volatile boolean done;
    private final AtomicReference<Throwable> error = new AtomicReference<>();

    // === Thread references (for unpark) ===
    volatile Thread retrieverThread;
    volatile Thread drainThread;

    // === In-flight decode tasks (tracked so close() can await them) ===
    private final Set<CompletableFuture<Void>> inFlightDecodes = ConcurrentHashMap.newKeySet();

    // === Drain assembly state (drain thread only) ===
    final long maxRows;
    long totalRowsAssembled;
    B currentBatch;
    int rowsInCurrentBatch;

    /// File name of the file being assembled into the current batch.
    /// Written only by the drain thread.
    String currentBatchFileName;

    // === Instrumentation (drain thread only) ===
    long publishBlockNanos;
    int batchesPublished;

    /// Creates a new column worker.
    ///
    /// @param pageSource yields [PageInfo] objects for this column
    /// @param exchange the output exchange for assembled batches
    /// @param column the column schema
    /// @param batchCapacity rows per batch
    /// @param decompressorFactory for creating page decompressors
    /// @param decodeExecutor executor for decode tasks
    /// @param maxRows maximum rows to assemble (0 = unlimited). The drain stops
    ///        after assembling this many rows and publishes the partial batch.
    protected ColumnWorker(PageSource pageSource, BatchExchange<B> exchange, ColumnSchema column,
                           int batchCapacity, DecompressorFactory decompressorFactory,
                           Executor decodeExecutor, long maxRows) {
        this.pageSource = pageSource;
        this.exchange = exchange;
        this.column = column;
        this.physicalType = column.type();
        this.batchCapacity = batchCapacity;
        this.maxDefinitionLevel = column.maxDefinitionLevel();
        this.decompressorFactory = decompressorFactory;
        this.decodeExecutor = decodeExecutor;
        this.maxRows = maxRows;
        this.reorderBuffer = new AtomicReferenceArray<>(MAX_INFLIGHT_PAGES);
        this.fileNameBuffer = new String[MAX_INFLIGHT_PAGES];
    }

    /// Initializes subclass-specific drain state (called at the start of `runDrain`).
    abstract void initDrainState();

    /// Assembles a single decoded page into the current batch.
    abstract void assemblePage(Page page);

    /// Publishes the current batch to the [BatchExchange] and takes a new free batch.
    abstract void publishCurrentBatch();

    /// Starts both virtual threads. Must be called once.
    ///
    /// Thread fields are assigned before `start()` so an early
    /// `unparkRetriever()` from the drain cannot observe a null reference and
    /// silently drop the unpark.
    public void start() {
        this.drainThread = Thread.ofVirtual().unstarted(this::runDrain);
        this.retrieverThread = Thread.ofVirtual().unstarted(this::runRetriever);
        drainThread.start();
        retrieverThread.start();
    }

    /// Signals the worker to stop and blocks until the pipeline has fully quiesced:
    /// both VThreads have exited and every in-flight decode task has completed.
    ///
    /// This is required so that callers can safely release resources owned by the
    /// underlying [dev.hardwood.InputFile] (mapped or direct byte buffers, HTTP
    /// connections, etc.) without risking a SIGSEGV from a decode task still
    /// reading from a freed buffer.
    @Override
    public void close() {
        done = true;
        exchange.finish();  // signals BatchExchange's timeout loops to exit
        LockSupport.unpark(retrieverThread);
        LockSupport.unpark(drainThread);

        // Join the retriever and drain VThreads. If the calling thread is interrupted
        // during join(), we must NOT re-assert the interrupt flag until after draining
        // all in-flight decode tasks. CompletableFuture.allOf().join() returns
        // immediately when the calling thread's interrupt flag is already set, which
        // would leave decode tasks still running and violate the close() contract
        // (safe resource release requires all tasks to have finished first).
        boolean interrupted = false;
        try {
            retrieverThread.join();
        }
        catch (InterruptedException e) {
            interrupted = true;
        }
        try {
            drainThread.join();
        }
        catch (InterruptedException e) {
            interrupted = true;
        }

        // The retriever has exited, so no new decode tasks will be submitted.
        // Drain any that are still running. Tasks that hadn't yet started early-return
        // via the `done` check in decode(), so this typically waits only on the small
        // number that were mid-execution when `done` was set.
        //
        // NOTE: The interrupt flag must be clear here so that allOf().join() actually
        // waits. We restore it unconditionally after the join.
        CompletableFuture<?>[] pending = inFlightDecodes.toArray(new CompletableFuture<?>[0]);
        if (pending.length > 0) {
            try {
                CompletableFuture.allOf(pending).join();
            }
            catch (Exception ignored) {
                // decode tasks call signalError on failure; nothing to re-raise here
            }
        }

        // Restore interrupt status now that all tasks have drained.
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /// Whether the pipeline has stopped producing batches (for any reason —
    /// natural EOF, `maxRows`, error, or [#close()]).
    public boolean isFinished() {
        return done;
    }

    // ==================== Retriever VThread ====================

    private long sourceNanos;
    private long throttleNanos;
    private int totalPagesSubmitted;
    private int throttleParks;

    private void runRetriever() {
        try {
            LOG.log(System.Logger.Level.DEBUG,
                    "[{0}] ColumnWorker started, maxOutstanding={1}, batchCapacity={2}",
                    column.name(), MAX_INFLIGHT_PAGES, batchCapacity);

            PageDecoder pageDecoder = null;
            int nextSeq = 0;

            long t0;
            PageInfo pageInfo;
            while (!done) {
                // Pull next page from source
                t0 = System.nanoTime();
                pageInfo = pageSource.next();
                sourceNanos += System.nanoTime() - t0;
                if (pageInfo == null) {
                    break;
                }

                // Create/update PageDecoder when column metadata changes (file transitions)
                if (pageDecoder == null || !pageDecoder.isCompatibleWith(pageInfo.columnMetaData())) {
                    pageDecoder = new PageDecoder(
                            pageInfo.columnMetaData(),
                            pageInfo.columnSchema(),
                            decompressorFactory);
                }

                // Throttle: park while too many pages are in flight
                t0 = System.nanoTime();
                while (!done && nextSeq - consumePosition >= MAX_INFLIGHT_PAGES) {
                    throttleParks++;
                    LockSupport.park();
                }
                throttleNanos += System.nanoTime() - t0;
                if (done) {
                    break;
                }

                // Submit decode task to executor (reuses pooled threads, no VThread per page)
                int seq = nextSeq++;
                totalPagesSubmitted++;
                int slot = seq % MAX_INFLIGHT_PAGES;
                fileNameBuffer[slot] = pageSource.getCurrentFileName();
                PageInfo pi = pageInfo;
                PageDecoder rdr = pageDecoder;
                // Pre-register a ticket *before* submitting to the executor so that
                // close() can never miss an in-flight task. If the task completed and
                // called remove() before add() ran, the ticket would vanish from the
                // set and close() could return with tasks still executing — causing a
                // use-after-free on the mapped buffers that InputFile.close() releases.
                CompletableFuture<Void> ticket = new CompletableFuture<>();
                inFlightDecodes.add(ticket);
                CompletableFuture.runAsync(() -> decode(slot, pi, rdr), decodeExecutor)
                        .whenComplete((v, t) -> {
                            ticket.complete(null);
                            inFlightDecodes.remove(ticket);
                        });
            }

            if (!done) {
                // The sentinel needs a free slot. If all MAX_INFLIGHT_PAGES slots
                // are occupied (pages submitted but not yet drained), wait for
                // the drain to advance before writing.
                while (!done && nextSeq - consumePosition >= MAX_INFLIGHT_PAGES) {
                    LockSupport.park();
                }
                if (!done) {
                    int sentinelSlot = nextSeq % MAX_INFLIGHT_PAGES;
                    reorderBuffer.set(sentinelSlot, EMPTY_SENTINEL);
                    LockSupport.unpark(drainThread);
                }
            }

            LOG.log(System.Logger.Level.DEBUG,
                    "[{0}] Retriever finished: {1} pages submitted. "
                    + "source={2,number,0.0}ms, throttle={3,number,0.0}ms ({4} parks)",
                    column.name(), totalPagesSubmitted,
                    sourceNanos / 1_000_000.0, throttleNanos / 1_000_000.0, throttleParks);
        }
        catch (Throwable t) {
            signalError(enrichWithFileName(t, pageSource.getCurrentFileName()));
        }
    }

    /// Decode task: decodes one page, stores result in reorder buffer, unparks drain.
    private void decode(int slot, PageInfo pageInfo, PageDecoder pageDecoder) {
        if (done || error.get() != null) {
            return;
        }
        try {
            Page page = pageInfo.isNullPlaceholder()
                    ? pageDecoder.nullPage(pageInfo.placeholderNumValues())
                    : pageDecoder.decodePage(pageInfo.pageData(), pageInfo.dictionary());
            reorderBuffer.set(slot, page);
        }
        catch (Throwable t) {
            signalError(enrichWithFileName(t, fileNameBuffer[slot]));
        }
        LockSupport.unpark(drainThread);
    }

    // ==================== Drain VThread ====================

    private long assemblyNanos;
    private long decodeWaitNanos;
    private int totalPagesDrained;
    private int decodeWaitParks;

    private void runDrain() {
        try {
            currentBatch = exchange.takeBatch();
            initDrainState();

            while (!done) {
                long t0 = System.nanoTime();
                boolean drained = drainReadyPages();
                assemblyNanos += System.nanoTime() - t0;

                if (!done && !drained) {
                    // No pages were ready — park until a decode task completes
                    long parkStart = System.nanoTime();
                    decodeWaitParks++;
                    LockSupport.park();
                    decodeWaitNanos += System.nanoTime() - parkStart;
                }
                // If we drained something, loop immediately to check for more
            }

            // assemblyNanos includes publishBlock; subtract to get pure assembly
            long pureAssembly = assemblyNanos - publishBlockNanos;

            LOG.log(System.Logger.Level.DEBUG,
                    "[{0}] Drain finished: {1} pages drained, {2} batches. "
                    + "assembly={3,number,0.0}ms, decodeWait={4,number,0.0}ms ({5} parks), "
                    + "publishBlock={6,number,0.0}ms",
                    column.name(), totalPagesDrained, batchesPublished,
                    pureAssembly / 1_000_000.0, decodeWaitNanos / 1_000_000.0, decodeWaitParks,
                    publishBlockNanos / 1_000_000.0);
        }
        catch (Throwable t) {
            signalError(enrichWithFileName(t, currentBatchFileName));
        }
    }

    /// Drains all consecutive ready pages from the reorder buffer.
    /// Returns true if at least one page was drained.
    private boolean drainReadyPages() {
        boolean drained = false;
        while (!done) {
            int slot = consumePosition % MAX_INFLIGHT_PAGES;
            Page page = reorderBuffer.getAndSet(slot, null);
            if (page == null) {
                break;
            }
            if (page == EMPTY_SENTINEL) {
                finishDrain();
                return true;
            }

            // Detect file boundary: flush the current batch when the file changes
            // so that each batch is attributed to a single file.
            String pageFileName = fileNameBuffer[slot];
            if (pageFileName != null) {
                if (currentBatchFileName != null
                        && !pageFileName.equals(currentBatchFileName)
                        && rowsInCurrentBatch > 0) {
                    publishCurrentBatch();
                }
                currentBatchFileName = pageFileName;
            }

            assemblePage(page);
            consumePosition++;
            totalPagesDrained++;
            unparkRetriever();
            drained = true;
        }
        return drained;
    }

    void finishDrain() {
        if (rowsInCurrentBatch > 0) {
            publishCurrentBatch();
        }
        done = true;
        exchange.finish();
        // Wake the retriever so it can observe `done` and exit; otherwise it
        // could be parked on the throttle indefinitely (consumePosition never
        // advances again once drain has finished).
        unparkRetriever();
    }

    // ==================== Error Handling ====================

    void signalError(Throwable t) {
        error.compareAndSet(null, t);
        done = true;
        exchange.signalError(t);
        LockSupport.unpark(retrieverThread);
        LockSupport.unpark(drainThread);
    }

    /// Enriches a throwable with file-name context if possible.
    ///
    /// `RuntimeException` is enriched in-place via [ExceptionContext#addFileContext],
    /// preserving the original type. `IOException` is wrapped in a fresh
    /// [UncheckedIOException] carrying the prefix; this prevents
    /// [BatchExchange#checkError] from later wrapping it in a generic
    /// `RuntimeException("Error in pipeline for column 'X'")` that would lose the
    /// file context. `Error` and other throwables propagate unchanged.
    private static Throwable enrichWithFileName(Throwable t, String fileName) {
        if (fileName == null || fileName.isEmpty()) {
            return t;
        }
        if (t instanceof RuntimeException re) {
            return ExceptionContext.addFileContext(fileName, re);
        }
        if (t instanceof IOException ioe) {
            return new UncheckedIOException(
                    ExceptionContext.filePrefix(fileName)
                            + (ioe.getMessage() != null ? ioe.getMessage() : "I/O failure"),
                    ioe);
        }
        return t;
    }

    private void unparkRetriever() {
        Thread t = retrieverThread;
        if (t != null) {
            LockSupport.unpark(t);
        }
    }

    /// Maximum number of decoded-but-undrained pages before the retriever throttles.
    /// Kept low to limit decoded page retention and GC pressure. With large pages
    /// (~4-10 MB decoded), high values cause old-gen promotion and expensive G1
    /// evacuation pauses. Overridable via the `hardwood.internal.maxOutstanding` system property.
    public static final int MAX_INFLIGHT_PAGES =
            Integer.getInteger("hardwood.internal.maxOutstanding", 8);
}
