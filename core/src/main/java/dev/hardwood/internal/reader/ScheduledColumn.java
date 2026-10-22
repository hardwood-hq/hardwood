/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import dev.hardwood.internal.DecodeBuffers;
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.compression.DecompressorFactory;
import dev.hardwood.schema.ColumnSchema;

/// A column whose pages a [ReadScheduler] submits to the decode pool, in row order with the
/// read's other scheduled columns, and whose page tasks decode straight into the column's batches.
///
/// - The **fetcher** runs as fetch steps, tasks of the decode pool. A step pulls the column's
///   pages from its [PageSource] and queues them for the scheduler ([#queue]), yielding where
///   the source's walk needs bytes not yet read or its next work item is still being planned
///   ([Need]). It queues while fewer than [#FETCH_AHEAD] pages are queued and
///   returns once that many are; a change that makes room queues the column's next step
///   ([#scheduleFetch()]). At most one step of a column is queued or running, so the steps of a
///   column run one after another and see each other's state. The steps go to the head of the
///   pool's queue, as the read waits for the pages they fetch; a parse or a decode a step needs
///   runs in place on its thread.
/// - A **page task**, on the decode pool, decodes its page into the column's cells ([#runPage]).
/// - **Publishing** takes the completed cells at the head of the column's publish order into its
///   [BatchExchange], without waiting. A cell that finds no room stays at the head, and the
///   consumer publishes it when it takes a batch ([BatchExchange#setRoomListener]).
///
/// A column without repetition places its pages into cells on its fetch steps, from their metadata
/// ([ScheduledFlatColumn]); a column decoded through levels places them on their tasks, once
/// decoded and measured ([ScheduledNestedColumn]).
///
/// The column lock guards the placement, the queue of pages for the scheduler and the publish
/// order. It is held for bookkeeping only: no decoding, filling or I/O runs under it.
///
/// @param <B> the batch type
public abstract class ScheduledColumn<B> implements ColumnPipeline {

    /// Sentinel for the `maxRows` / row-limit contract meaning "no limit".
    static final long UNLIMITED = 0L;

    /// Pages a column keeps decoded and not yet filled into its cells, at most: the unplaced pages
    /// of a nested column, and the size of a flat binary column's scratch pool. Overridable via the
    /// `hardwood.internal.maxOutstanding` system property.
    static final int MAX_INFLIGHT_PAGES = Integer.getInteger("hardwood.internal.maxOutstanding", 8);

    /// Pages the fetch steps keep queued for the scheduler.
    static final int FETCH_AHEAD = 2;

    /// Stands in the publish order for the end of the read.
    static final Object END = new Object();

    /// Told of every fetch step that stops for room, on its thread, before it releases its claim;
    /// for tests of the wake protocol. `null` for none.
    static volatile Consumer<ScheduledColumn<?>> roomWaitObserver;

    final ColumnSchema column;
    final PageSource pageSource;
    final BatchExchange<B> exchange;
    final DecompressorFactory decompressorFactory;
    final int maxDefinitionLevel;
    private final ReadScheduler scheduler;

    // === Guarded by `lock` ===
    final ReentrantLock lock = new ReentrantLock();
    /// Pages queued and not yet submitted, in page order. A column also queues a [SealedFillTask]
    /// here, which stands for no page and reports as a boundary marker.
    final ArrayDeque<PageTask> ready = new ArrayDeque<>();
    /// Cells in plan order, then [#END] or the failure that ends the read.
    final ArrayDeque<Object> publishOrder = new ArrayDeque<>();

    // === Read by the scheduler ===
    /// The read's row the column's next page starts at or after.
    volatile long nextRow;
    /// The read's row of the column's first record not yet published: the first row of the cell
    /// at the head of the publish order, or [#nextRow] when every cell placed is published.
    private volatile long publishedRow;
    /// Set once the fetch steps queue no more pages.
    volatile boolean fetchEnded;

    // === Owned by the scheduler, read and written only by the thread submitting ===
    /// Raised when the column's state changed since the scheduler last indexed it.
    final AtomicBoolean changePending = new AtomicBoolean();
    /// The column's position among the read's columns, which orders columns with equal keys.
    int scheduleId;
    /// Where the scheduler indexed the column: [ReadScheduler] keeps it in at most one of its
    /// queued and pending sets, by the key last read here.
    boolean indexedQueued;
    boolean indexedPending;
    boolean indexedPublished;
    long indexedHeadRow;
    long indexedNextRow;
    long indexedPublishedRow;

    // === Pipeline control ===
    /// Set when the column stops: closed, its last batch published, or failed.
    volatile boolean done;
    /// Page tasks and fetch steps submitted and not finished, which [#close()] waits for.
    private final AtomicInteger tasks = new AtomicInteger();
    /// Raised when a publish found no room, so the consumer publishes when it takes a batch.
    private final AtomicBoolean publishWanted = new AtomicBoolean();
    /// A fetch step is queued or running; stays set once the fetch has ended, so that no step
    /// follows it.
    private final AtomicBoolean fetchScheduled = new AtomicBoolean();
    /// Raised by every [#scheduleFetch()]: the running step looks again before it returns.
    private volatile boolean fetchWanted;
    /// The thread that claimed the close and waits for the page tasks; the last task unparks it.
    private volatile Thread closingThread;
    /// Claimed by the one thread that closes the column ([#close()]).
    private final AtomicBoolean closeClaimed = new AtomicBoolean();
    /// Released once the claiming thread has closed the column, for every other closer.
    private final CountDownLatch closed = new CountDownLatch(1);

    // === Fetch steps only ===
    private PageDecoder pageDecoder;

    /// Creates a scheduled column and adds it to `scheduler`, which starts it.
    ScheduledColumn(ReadScheduler scheduler, PageSource pageSource, BatchExchange<B> exchange, ColumnSchema column,
                    DecompressorFactory decompressorFactory) {
        this.scheduler = scheduler;
        this.pageSource = pageSource;
        this.exchange = exchange;
        this.column = column;
        this.decompressorFactory = decompressorFactory;
        this.maxDefinitionLevel = column.maxDefinitionLevel();
        exchange.setRoomListener(this::roomPossible);
        if (scheduler.queue() != null) {
            exchange.setQueue(scheduler.queue());
        }
        scheduler.add(this);
    }

    /// Queues the column's first fetch step. Called by the scheduler.
    void start() {
        scheduleFetch();
    }

    /// Stops every column of the read ([ReadScheduler#stop()]) and waits until this one is
    /// quiescent: every fetch step and page task submitted for it has finished, so that the
    /// caller can release what the `InputFile` owns. Nothing is interrupted.
    ///
    /// A read's columns are closed together, one after another. Stopping them all on the first
    /// close lets the waits of every column overlap: a column still running while an earlier one
    /// is awaited would go on fetching, submitting and decoding pages.
    ///
    /// A reader and its parent can close a read on two threads at once. One thread claims the
    /// close and waits for the column; every other closer returns once that close has completed.
    ///
    /// On a context without a pool, the closing thread runs the read's queue while it waits, as
    /// nothing else runs the column's tasks; each returns at once on a stopped column.
    @Override
    public void close() {
        if (!closeClaimed.compareAndSet(false, true)) {
            awaitClosed();
            return;
        }
        boolean interrupted = false;
        try {
            scheduler.stop();
            // Written before the count is read, as a task reads it after the count: either this
            // sees the last task's decrement or that task sees this thread and unparks it.
            closingThread = Thread.currentThread();
            ReadQueue queue = scheduler.queue();
            if (queue != null) {
                queue.runUntil(() -> tasks.get() == 0);
            }
            while (tasks.get() > 0) {
                LockSupport.park(this);
                // A park returns at once while the interrupt flag is set: cleared here and
                // restored on the way out, so an interrupted close waits instead of spinning.
                interrupted |= Thread.interrupted();
            }
        }
        finally {
            closed.countDown();
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /// Waits until the thread that claimed the close has completed it, carrying on through
    /// interrupts and restoring them.
    private void awaitClosed() {
        boolean interrupted = false;
        while (true) {
            try {
                closed.await();
                break;
            }
            catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /// Stops the column without waiting for it: no publish runs after it, its consumer sees the
    /// end of the stream, and a fetch step queued for it ends the fetch. Called by
    /// [ReadScheduler#stop()], on the thread that closes the read.
    void signalStop() {
        stop();
        exchange.finish();
        scheduleFetch();
    }

    /// Sets `done` under the column lock, so that no publish runs after it.
    private void stop() {
        lock.lock();
        try {
            done = true;
        }
        finally {
            lock.unlock();
        }
    }

    boolean isDone() {
        return done;
    }

    // ==================== Scheduler side ====================

    /// The column's next queued page, `null` if none is queued.
    PageTask peekReady() {
        lock.lock();
        try {
            return ready.peekFirst();
        }
        finally {
            lock.unlock();
        }
    }

    long nextRow() {
        return nextRow;
    }

    long publishedRow() {
        return publishedRow;
    }

    /// The column's page tasks submitted and not finished.
    int tasksInFlight() {
        return tasks.get();
    }

    /// Takes the column's next queued page off the queue and counts it as a task. The task is
    /// counted before `done` is read, so [#close()] either sees the count or this sees `done`.
    ///
    /// @return the page, or `null` if the column is done
    PageTask takeReady() {
        tasks.incrementAndGet();
        PageTask page;
        lock.lock();
        try {
            page = done ? null : ready.pollFirst();
        }
        finally {
            lock.unlock();
        }
        if (page == null) {
            // Not submitted, so not counted by the scheduler.
            uncountTask();
        }
        scheduleFetch();
        return page;
    }

    /// Ends the read with the decode pool's refusal of a page task, which a closed context gives.
    void rejected(RejectedExecutionException e) {
        taskEnded();
        signalError(ScheduledColumn.contextClosed(e));
    }

    // ==================== Fetch steps ====================

    /// Queues the column's next fetch step on the pool, unless one is queued or running, which
    /// then looks again before it returns, or the fetch has ended. Called on every change that
    /// may let the column queue a page or end its fetch: a page taken off its queue, a page
    /// placed, a stop, the end of the read.
    void scheduleFetch() {
        if (fetchEnded) {
            return;
        }
        // Raised before the claim is tried: a step that releases the claim reads it after
        fetchWanted = true;
        if (!fetchScheduled.compareAndSet(false, true)) {
            return;
        }
        tasks.incrementAndGet();
        try {
            scheduler.executeFetch(this::runFetchStep);
        }
        catch (RejectedExecutionException e) {
            uncountTask();
            endFetch();
            signalError(ScheduledColumn.contextClosed(e));
        }
    }

    /// Runs fetch steps until the column has no room and no change arrived since the last one
    /// looked, its fetch ends, or it needs bytes or plans that are not ready. A step that stops on
    /// bytes issues their read as demand; one that stops on a work item's planning leaves it to the
    /// virtual thread that plans it. Either keeps the column's claim and its count of tasks, and
    /// the need's completion queues the next step ([#resumeFetch]): no step waits for I/O, and a
    /// close waits for the read and the planning. On a context without a pool, the read's queue
    /// counts the step as outstanding until then ([ReadQueue#yielded()]).
    private void runFetchStep() {
        Need pending = null;
        try {
            while (true) {
                fetchWanted = false;
                FetchOutcome outcome = fetch();
                if (outcome == FetchOutcome.ENDED) {
                    return;
                }
                if (outcome == FetchOutcome.PENDING) {
                    pending = pageSource.pending();
                    return;
                }
                Consumer<ScheduledColumn<?>> observer = roomWaitObserver;
                if (observer != null) {
                    observer.accept(this);
                }
                fetchScheduled.set(false);
                // Released before the flag is read: a change after the step looked either
                // raised the flag before this read, or finds the claim free and queues a step
                if (!fetchWanted || !fetchScheduled.compareAndSet(false, true)) {
                    return;
                }
            }
        }
        finally {
            if (pending == null) {
                uncountTask();
            }
            else {
                // Last: the completion may queue the next step at once, which another thread of
                // the pool runs
                ReadQueue queue = scheduler.queue();
                if (queue != null) {
                    queue.yielded();
                }
                pending.whenMet(this::resumeFetch);
            }
        }
    }

    /// Queues the step that a need stopped, once the need is met, on the thread that met it. The
    /// step holds the column's claim and its count of tasks.
    private void resumeFetch() {
        try {
            scheduler.executeFetch(this::runFetchStep);
        }
        catch (RejectedExecutionException e) {
            uncountTask();
            endFetch();
            signalError(ScheduledColumn.contextClosed(e));
        }
        finally {
            // After the step is queued, so that the read's queue is never seen idle in between
            ReadQueue queue = scheduler.queue();
            if (queue != null) {
                queue.resumed();
            }
        }
    }

    /// How a fetch step's [#fetch] stopped.
    private enum FetchOutcome {
        /// The column's fetch has ended.
        ENDED,
        /// The column's queue is full.
        NO_ROOM,
        /// The next page needs bytes that are not present, or a work item that is not yet planned.
        PENDING
    }

    /// Fetches and queues pages while the column may queue them and their bytes are present.
    ///
    /// @return how the fetch stopped
    private FetchOutcome fetch() {
        FetchOutcome outcome = FetchOutcome.ENDED;
        try {
            while (!done && !fetchStopped()) {
                if (!mayQueue()) {
                    outcome = FetchOutcome.NO_ROOM;
                    return outcome;
                }
                PageInfo info = pageSource.next();
                if (info == null) {
                    if (pageSource.pending() != null) {
                        outcome = FetchOutcome.PENDING;
                        return outcome;
                    }
                    pagesEnded();
                    break;
                }
                if (!info.isBoundaryMarker()
                        && (pageDecoder == null || !pageDecoder.decodesChunk(info.columnMetaData()))) {
                    pageDecoder = newPageDecoder(info);
                }
                if (!queue(info, pageDecoder)) {
                    break;
                }
                scheduler.submit(this);
            }
        }
        catch (Exception e) {
            fetchFailed(ScheduledColumn.placeFailure(e, column, pageSource.footersSupplied(),
                    pageSource.getCurrentFileName(), pageSource.getCurrentRowGroupIndex(),
                    pageSource.getCurrentPageIndex()), pageSource.isBetweenWorkItems());
        }
        catch (Error err) {
            signalError(err);
            throw err;
        }
        finally {
            if (outcome == FetchOutcome.ENDED) {
                endFetch();
            }
        }
        return outcome;
    }

    /// Ends the column's fetch: no step follows, and the scheduler stops waiting for a page of it.
    private void endFetch() {
        fetchEnded = true;
        lock.lock();
        try {
            nextRow = Long.MAX_VALUE;
            updatePublishedRow();
        }
        finally {
            lock.unlock();
        }
        scheduler.submit(this);
    }

    /// A decoder for the column chunk `info` starts.
    PageDecoder newPageDecoder(PageInfo info) {
        return new PageDecoder(info.columnMetaData(), info.columnSchema(), decompressorFactory);
    }

    /// A page of the column, for its queue: where it came from, and the decoder of its chunk.
    PageTask newPageTask(PageInfo info, PageDecoder decoder, long firstRow) {
        return new PageTask(info, pageSource.getCurrentFileName(), pageSource.getCurrentRowGroupIndex(),
                pageSource.getCurrentPageIndex(), pageSource.getCurrentBatches(), decoder, firstRow);
    }

    /// Queues the fetch step's next page for the scheduler, unless it has no records to decode.
    ///
    /// @return whether the column goes on
    abstract boolean queue(PageInfo info, PageDecoder decoder);

    /// Ends the column's stream of pages: the fetch steps have queued every page.
    abstract void pagesEnded();

    /// Ends the stream with a fetch step's failure, raised once every cell before it is published.
    ///
    /// @param betweenWorkItems whether the read failed between two work items, where every
    ///        column's batch ends at the same row
    abstract void fetchFailed(Exception failure, boolean betweenWorkItems);

    /// Whether the column fetches no further page: the column's placement ended before its
    /// read did.
    boolean fetchStopped() {
        return false;
    }

    /// Whether a fetch step may queue another page.
    boolean mayQueue() {
        return queued() < FETCH_AHEAD;
    }

    int queued() {
        lock.lock();
        try {
            return ready.size();
        }
        finally {
            lock.unlock();
        }
    }

    /// Stops publishing past `failure`, which is raised once every cell before it is published. A
    /// failure between work items publishes the open cell's partial batch first, as every column's
    /// batch ends there; one inside a work item abandons it (lock held).
    ///
    /// @param abandoned the open cell of a failure inside a work item, `null` if none; it is never
    ///        published
    void endWithFailure(Throwable failure, ScheduledCell<B> abandoned) {
        if (abandoned != null) {
            publishOrder.remove(abandoned);
        }
        publishOrder.addLast(failure);
        publishReady();
    }

    // ==================== Page task ====================

    /// Decodes a page into the column's cells, with the buffers of the pool task running it.
    abstract void runPage(PageTask page, DecodeBuffers buffers);

    /// Ends the read with a failure met on `page`'s task, placed with the page; a segment's
    /// failure ([SegmentFillException]) is placed with the segment's own page.
    void pageFailed(Exception e, PageTask page) {
        if (e instanceof SegmentFillException segment) {
            failed(segment.failure(), segment.page);
        }
        else {
            failed(e, page);
        }
    }

    private void failed(Exception e, PageTask page) {
        signalError(ScheduledColumn.placeFailure(e, column, pageSource.footersSupplied(), page.fileName,
                page.rowGroup, page.pageIndex));
    }

    /// Marks a cell completed and publishes what its completion lets the column publish.
    ///
    /// @param task the buffers of the pool task completing the cell, which only a task of the
    ///        context's pool holds: a cell completes on the pool, as its fill, its validity and
    ///        its filter fragment run there
    void completed(ScheduledCell<B> cell, DecodeBuffers task) {
        lock.lock();
        try {
            cell.markCompleted();
            publishReady();
        }
        finally {
            lock.unlock();
        }
    }

    // ==================== Publish (column lock) ====================

    /// Publishes the completed cells at the head of the publish order, and raises the item ending
    /// the read once every cell before it is published.
    void publishReady() {
        Object head;
        while (!done && (head = publishOrder.peekFirst()) != null) {
            if (head == END) {
                publishOrder.pollFirst();
                finishRead();
                return;
            }
            if (head instanceof Throwable failure) {
                publishOrder.pollFirst();
                signalError(failure);
                return;
            }
            @SuppressWarnings("unchecked")
            ScheduledCell<B> cell = (ScheduledCell<B>) head;
            if (!cell.isCompleted()) {
                break;
            }
            if (!exchange.tryPublish(cell.batch())) {
                publishWanted.set(true);
                // Room may have come free between the try and the flag: either the retry sees it,
                // or the consumer's next take sees the flag.
                if (!exchange.tryPublish(cell.batch())) {
                    break;
                }
            }
            publishOrder.pollFirst();
            cell.release();
        }
        updatePublishedRow();
        scheduler.submit(this);
    }

    /// Run by the consumer after it took a batch: publishes the cells that waited for room.
    /// A publish under the lock that left room continues its own loop.
    private void roomPossible() {
        if (!lock.isHeldByCurrentThread() && publishWanted.compareAndSet(true, false)) {
            lock.lock();
            try {
                publishReady();
            }
            finally {
                lock.unlock();
            }
        }
    }

    /// Recomputes [#publishedRow] after the publish order or [#nextRow] changed (lock held).
    void updatePublishedRow() {
        Object head = publishOrder.peekFirst();
        if (head instanceof ScheduledCell<?> cell) {
            publishedRow = cell.firstRow();
        }
        else {
            publishedRow = Math.min(nextRow, unplacedRow());
        }
    }

    /// The first row of the column's first page queued and not yet placed into a cell,
    /// [Long#MAX_VALUE] when there is none (lock held). Such a page's records are not published
    /// yet.
    long unplacedRow() {
        return Long.MAX_VALUE;
    }

    private void finishRead() {
        done = true;
        exchange.finish();
        scheduleFetch();
        scheduler.submit(this);
    }

    /// Ends the column's read with `t`: the scheduler failed.
    void fail(Throwable t) {
        signalError(t);
    }

    void signalError(Throwable t) {
        done = true;
        exchange.signalError(t);
        scheduleFetch();
        scheduler.submit(this);
    }

    void taskEnded() {
        uncountTask();
        scheduler.pageEnded(this);
    }

    private void uncountTask() {
        if (tasks.decrementAndGet() == 0) {
            Thread closing = closingThread;
            if (closing != null) {
                LockSupport.unpark(closing);
            }
        }
    }

    void unparkScheduler() {
        scheduler.submit(this);
    }

    /// A failure met reading `column`, typed ([#asReadFailure]) and placed with the file, row group
    /// and page it came from. Where a [dev.hardwood.MetadataSource] supplied the footers
    /// (`footersSupplied`), a read failure also says so
    /// ([ExceptionContext#addSuppliedFooterHint]): the checks a supplied footer passes on open do
    /// not catch every file rewritten since the footer was read.
    static Exception placeFailure(Exception e, ColumnSchema column, boolean footersSupplied,
                                  String fileName, int rowGroup, int page) {
        Exception typed = asReadFailure(e);
        if (fileName == null || fileName.isEmpty()) {
            return typed;
        }
        String columnPath = column.fieldPath().toString();
        if (typed instanceof RuntimeException re) {
            RuntimeException placed = ExceptionContext.addReadContext(fileName, rowGroup, columnPath, page, re);
            return footersSupplied ? ExceptionContext.addSuppliedFooterHint(placed) : placed;
        }
        if (typed instanceof IOException ioe) {
            // Stays checked. The pipeline carries a failure across its thread boundary as a
            // `Throwable`, so nothing between here and the reader needs it wrapped, and the
            // reader's own signature can declare it.
            return ExceptionContext.addReadContext(fileName, rowGroup, columnPath, page, ioe);
        }
        return typed;
    }

    /// The failure of a read whose decode pool refused a task: its context was closed while it
    /// was reading. It is not the file's, so it is not placed.
    static IllegalStateException contextClosed(RejectedExecutionException e) {
        return new IllegalStateException("The HardwoodContext was closed while a reader using it was reading", e);
    }

    /// Delegates runtime-failure classification; checked transport failures pass through unchanged.
    static Exception asReadFailure(Exception e) {
        return e instanceof RuntimeException runtime
                ? ExceptionContext.asReadFailure(runtime)
                : e;
    }
}
