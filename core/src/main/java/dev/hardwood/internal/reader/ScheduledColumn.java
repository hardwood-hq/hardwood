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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.compression.DecompressorFactory;
import dev.hardwood.schema.ColumnSchema;

/// A column whose pages a [ReadScheduler] submits to the decode pool, in row order with the
/// read's other scheduled columns, and whose page tasks decode straight into the column's batches.
///
/// - The **fetcher**, a virtual thread, pulls the column's pages from its [PageSource], which does
///   every `InputFile` access, and queues them for the scheduler ([#queue]). It keeps at most
///   [#FETCH_AHEAD] pages queued.
/// - A **page task**, on the decode pool, decodes its page into the column's cells ([#runPage]).
/// - **Publishing** takes the completed cells at the head of the column's publish order into its
///   [BatchExchange], without waiting. A cell that finds no room stays at the head, and the
///   consumer publishes it when it takes a batch ([BatchExchange#setRoomListener]).
///
/// A column without repetition places its pages into cells on the fetcher, from their metadata
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
    /// of a nested column, and the size of a column's scratch pool. Overridable via the
    /// `hardwood.internal.maxOutstanding` system property.
    static final int MAX_INFLIGHT_PAGES = Integer.getInteger("hardwood.internal.maxOutstanding", 8);

    /// How long a fetcher waiting for room, and a close waiting for the column's page tasks, park
    /// before they re-check rather than waiting to be told.
    ///
    /// The conditions they wait on are monotone, so re-checking is always safe. The waits are
    /// timed because the runtime can drop an unpark: on JDK 25 through 25.0.2, and 26 before
    /// 26.0.1, an untimed park on a virtual thread that recently did a timed park can be stranded
    /// by the earlier park's stale timeout task (JDK-8369227, fixed in 25.0.3, 26.0.1 and 27).
    static final long WAKE_CHECK_NANOS = 10L * 1_000_000L;


    /// Pages the fetcher keeps queued for the scheduler.
    static final int FETCH_AHEAD = 2;

    /// Stands in the publish order for the end of the read.
    static final Object END = new Object();

    final ColumnSchema column;
    final PageSource pageSource;
    final BatchExchange<B> exchange;
    final DecompressorFactory decompressorFactory;
    final int maxDefinitionLevel;
    private final ReadScheduler scheduler;

    // === Guarded by `lock` ===
    final ReentrantLock lock = new ReentrantLock();
    /// Pages queued and not yet submitted, in page order.
    final ArrayDeque<PageTask> ready = new ArrayDeque<>();
    /// Cells in plan order, then [#END] or the failure that ends the read.
    final ArrayDeque<Object> publishOrder = new ArrayDeque<>();

    // === Read by the scheduler ===
    /// The read's row the column's next page starts at or after.
    volatile long nextRow;
    /// The read's row of the column's first record not yet published: the first row of the cell
    /// at the head of the publish order, or [#nextRow] when every cell placed is published.
    private volatile long publishedRow;
    /// The end rows ([PageTask#endRow]) of the pages submitted whose rows are not all published,
    /// in page order.
    private final ArrayDeque<long[]> submittedPages = new ArrayDeque<>();
    /// Set once the fetcher queues no more pages.
    volatile boolean fetchEnded;

    // === Pipeline control ===
    /// Set when the column stops: closed, its last batch published, or failed.
    volatile boolean done;
    /// Page tasks submitted and not finished, which [#close()] waits for.
    private final AtomicInteger tasks = new AtomicInteger();
    /// Raised when a publish found no room, so the consumer publishes when it takes a batch.
    private final AtomicBoolean publishWanted = new AtomicBoolean();
    private volatile Thread fetcherThread;
    private volatile Thread closingThread;

    // === Fetcher only ===
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
        scheduler.add(this);
    }

    /// Starts the fetcher. Called by the scheduler.
    void start() {
        fetcherThread = Thread.ofVirtual().unstarted(this::runFetcher);
        fetcherThread.start();
    }

    /// Stops every column of the read ([ReadScheduler#stop()]) and waits until this one is
    /// quiescent: its fetcher has exited and every page task submitted for it has finished, so
    /// that the caller can release what the `InputFile` owns. Nothing is interrupted.
    ///
    /// A read's columns are closed together, one after another. Stopping them all on the first
    /// close lets the waits of every column overlap: a column still running while an earlier one
    /// is awaited would go on submitting and decoding pages, and its fetcher would only be woken
    /// once its own close is reached.
    @Override
    public void close() {
        scheduler.stop();
        Thread fetcher = fetcherThread;
        boolean interrupted = fetcher != null && ScheduledColumn.joinUninterruptibly(fetcher);
        // Bounded wait, re-checked: the last task to finish unparks this thread.
        while (tasks.get() > 0) {
            LockSupport.parkNanos(this, ScheduledColumn.WAKE_CHECK_NANOS);
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /// Stops the column without waiting for it: no publish runs after it, its consumer sees the
    /// end of the stream, its fetcher is woken to exit, and the calling thread is the one its last
    /// page task wakes. Called by [ReadScheduler#stop()], on the thread that closes the read.
    void signalStop() {
        closingThread = Thread.currentThread();
        stop();
        exchange.finish();
        unparkFetcher();
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

    /// The column's pages submitted whose rows are not all published: the pages its read-ahead
    /// holds.
    int pagesAhead() {
        lock.lock();
        try {
            dropPublishedPages();
            return submittedPages.size();
        }
        finally {
            lock.unlock();
        }
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
            if (page != null) {
                submittedPages.addLast(page.endRow);
            }
        }
        finally {
            lock.unlock();
        }
        if (page == null) {
            // Not submitted, so not counted by the scheduler.
            uncountTask();
        }
        unparkFetcher();
        return page;
    }

    /// Ends the read with the decode pool's refusal of a page task, which a closed context gives.
    void rejected(RejectedExecutionException e) {
        taskEnded();
        signalError(ScheduledColumn.contextClosed(e));
    }

    // ==================== Fetcher ====================

    private void runFetcher() {
        try {
            while (!done && !fetchStopped()) {
                PageInfo info = pageSource.next();
                if (info == null) {
                    pagesEnded();
                    break;
                }
                if (!info.isBoundaryMarker()
                        && (pageDecoder == null || !pageDecoder.isCompatibleWith(info.columnMetaData()))) {
                    pageDecoder = newPageDecoder(info);
                }
                if (!queue(info, pageDecoder)) {
                    break;
                }
                scheduler.submit();
                while (!done && !fetchStopped() && !mayQueue()) {
                    LockSupport.parkNanos(this, ScheduledColumn.WAKE_CHECK_NANOS);
                }
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
            fetchEnded = true;
            lock.lock();
            try {
                nextRow = Long.MAX_VALUE;
                updatePublishedRow();
            }
            finally {
                lock.unlock();
            }
            scheduler.submit();
        }
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

    /// Queues the fetcher's next page for the scheduler, unless it has no records to decode.
    ///
    /// @return whether the column goes on
    abstract boolean queue(PageInfo info, PageDecoder decoder);

    /// Ends the column's stream of pages: the fetcher has queued every page.
    abstract void pagesEnded();

    /// Ends the stream with the fetcher's failure, raised once every cell before it is published.
    ///
    /// @param betweenWorkItems whether the read failed between two work items, where every
    ///        column's batch ends at the same row
    abstract void fetchFailed(Exception failure, boolean betweenWorkItems);

    /// Whether the fetcher fetches no further page: the column's placement ended before its
    /// read did.
    boolean fetchStopped() {
        return false;
    }

    /// Whether the fetcher may queue another page.
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

    /// Decodes a page into the column's cells.
    abstract void runPage(PageTask page);

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
    void completed(ScheduledCell<B> cell) {
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
        scheduler.submit();
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

    /// Drops the submitted pages whose rows are all published (lock held).
    private void dropPublishedPages() {
        long published = publishedRow;
        while (!submittedPages.isEmpty() && submittedPages.peekFirst()[0] <= published) {
            submittedPages.pollFirst();
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
        dropPublishedPages();
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
        unparkFetcher();
        scheduler.submit();
    }

    /// Ends the column's read with `t`: the scheduler failed.
    void fail(Throwable t) {
        signalError(t);
    }

    void signalError(Throwable t) {
        done = true;
        exchange.signalError(t);
        unparkFetcher();
        scheduler.submit();
    }

    void taskEnded() {
        uncountTask();
        scheduler.pageEnded();
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
        scheduler.submit();
    }

    void unparkFetcher() {
        Thread t = fetcherThread;
        if (t != null) {
            LockSupport.unpark(t);
        }
    }
    /// Waits for `thread` to exit, carrying on through interrupts.
    ///
    /// @return whether the calling thread was interrupted while waiting
    static boolean joinUninterruptibly(Thread thread) {
        boolean interrupted = false;
        while (true) {
            try {
                thread.join();
                return interrupted;
            }
            catch (InterruptedException e) {
                interrupted = true;
            }
        }
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
