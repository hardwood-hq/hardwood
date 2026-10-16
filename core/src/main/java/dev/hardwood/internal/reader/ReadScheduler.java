/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;

/// Submits the pages of a read's [ScheduledColumn]s to the decode pool, in row order across the
/// columns.
///
/// The pool runs tasks in the order they arrive. One scheduler per read, a virtual thread, takes
/// the queued page that starts at the lowest row of the read, so the tasks of the batch the
/// consumer waits for are ahead of every read-ahead task in the pool's queue, whatever the number
/// of columns and their page sizes. It does no I/O and no decoding: each column's fetcher
/// fetches and places the column's pages, and queues them.
///
/// Before it submits a page it waits for:
///
/// - every column whose queue is empty and whose next page could start before that page, so that
///   no page is submitted ahead of an earlier one;
/// - fewer than [#PAGES_IN_FLIGHT] pages submitted and not finished, across the read;
/// - the page to start fewer than [#WINDOW_BATCHES] batches of rows past the first row any column
///   has not published. A page may extend past that window, so a page spanning many batches is
///   not held back by its own span. The window is at least one batch, so that every page holding
///   rows of the batch the slowest column publishes next is within it.
///
/// While the batch the slowest column publishes next waits for decoding and a decode thread has no
/// task, counted over every read sharing the context, the window counts its batches as the rows
/// of the read's batch byte budget ([BatchSizing#budgetRows]) instead, when those are more.
/// [BatchSizing#MAX_BATCH] cuts the batches of a narrow read short of its budget, and a narrow
/// read of pages spanning several batches would otherwise keep fewer pages in flight than the
/// pool has threads. A read waiting for its consumer, or a pool kept busy by this read or others,
/// gains nothing from reading further ahead.
public final class ReadScheduler {

    /// Read-ahead bounds: pages in flight per decode thread, and batches past the slowest
    /// column's first unpublished row.
    private static final int PAGES_PER_THREAD = atLeastOne("hardwood.internal.pagesPerThread", 2);
    private static final int WINDOW_BATCHES = atLeastOne("hardwood.internal.windowBatches", 8);

    /// Told of every page as it is submitted, with the pages in flight then; for tests of the
    /// submission order and the read-ahead bounds. `null` for none.
    static volatile BiConsumer<PageTask, Integer> submissionObserver;

    private final HardwoodContextImpl context;
    private final int pagesInFlight;
    private final long windowRows;
    /// The window while a decode thread has no task.
    private final long idleWindowRows;
    private final List<ScheduledColumn> columns = new ArrayList<>();
    private final AtomicInteger submitted = new AtomicInteger();
    private final AtomicInteger closed = new AtomicInteger();
    private volatile Thread thread;

    /// @param context the context whose decode pool runs the page tasks
    /// @param batchCapacity the read's batch capacity
    /// @param budgetRows the rows of the read's batch byte budget ([BatchSizing#budgetRows])
    public ReadScheduler(HardwoodContextImpl context, int batchCapacity, long budgetRows) {
        this.context = context;
        this.pagesInFlight = PAGES_PER_THREAD * context.threads();
        this.windowRows = (long) WINDOW_BATCHES * batchCapacity;
        this.idleWindowRows = WINDOW_BATCHES * Math.max(batchCapacity, budgetRows);
    }

    private static int atLeastOne(String property, int defaultValue) {
        int value = Integer.getInteger(property, defaultValue);
        if (value < 1) {
            throw new IllegalArgumentException(property + " must be at least 1, but was " + value);
        }
        return value;
    }

    /// Adds a column; called by its constructor, before [#start()].
    void add(ScheduledColumn column) {
        if (thread != null) {
            throw new IllegalStateException("The scheduler has started");
        }
        columns.add(column);
    }

    /// Whether any column was added.
    public boolean hasColumns() {
        return !columns.isEmpty();
    }

    /// Starts every column's fetcher and the scheduler. Must be called once, after every column
    /// was added.
    public void start() {
        thread = Thread.ofVirtual().unstarted(this::run);
        for (ScheduledColumn column : columns) {
            column.start();
        }
        thread.start();
    }

    /// Wakes the scheduler: a page was queued, a task ended, a batch was published or a column
    /// ended.
    void unpark() {
        Thread t = thread;
        if (t != null) {
            LockSupport.unpark(t);
        }
    }

    void pageEnded() {
        submitted.decrementAndGet();
        context.pageTaskEnded();
        unpark();
    }

    /// Counts a closed column; the scheduler exits once every column is closed or done.
    void columnClosed() {
        closed.incrementAndGet();
        unpark();
    }

    private void run() {
        try {
            schedule();
        }
        catch (Throwable t) {
            // Nothing else submits the columns' pages: every column ends with the failure, an
            // `Error` included, which is rethrown as it was raised.
            for (ScheduledColumn column : columns) {
                column.fail(t);
            }
            throw t;
        }
    }

    private void schedule() {
        while (closed.get() < columns.size()) {
            ScheduledColumn next = next();
            if (next == null) {
                if (allDone()) {
                    return;
                }
                LockSupport.parkNanos(this, ColumnWorker.WAKE_CHECK_NANOS);
                continue;
            }
            PageTask page = next.takeReady();
            if (page == null) {
                continue;
            }
            int inFlight = submitted.incrementAndGet();
            context.pageTaskSubmitted();
            BiConsumer<PageTask, Integer> observer = submissionObserver;
            if (observer != null) {
                observer.accept(page, inFlight);
            }
            try {
                context.executor().execute(() -> next.runPage(page));
            }
            catch (RejectedExecutionException e) {
                next.rejected(e);
            }
        }
    }

    /// The column whose queued page is to be submitted now, `null` if none is.
    private ScheduledColumn next() {
        ScheduledColumn best = null;
        long bestRow = Long.MAX_VALUE;
        long publishedRow = Long.MAX_VALUE;
        // Whether the column whose first unpublished row is lowest waits for decoding
        boolean waitsForDecode = false;
        // The lowest row a column with nothing queued may yet queue a page at. Its next row is
        // read before its queue, and a page queued later starts at or after it.
        long pendingRow = Long.MAX_VALUE;
        for (int i = 0; i < columns.size(); i++) {
            ScheduledColumn column = columns.get(i);
            if (column.isDone()) {
                continue;
            }
            long columnPublished = column.publishedRow();
            if (columnPublished < publishedRow) {
                publishedRow = columnPublished;
                waitsForDecode = column.waitsForDecode();
            }
            else if (columnPublished == publishedRow) {
                waitsForDecode |= column.waitsForDecode();
            }
            long nextRow = column.nextRow();
            PageTask head = column.peekReady();
            if (head == null) {
                pendingRow = Math.min(pendingRow, nextRow);
            }
            else if (head.firstRow < bestRow) {
                best = column;
                bestRow = head.firstRow;
            }
        }
        if (best == null || pendingRow < bestRow || submitted.get() >= pagesInFlight
                || !withinWindow(bestRow - publishedRow, waitsForDecode)) {
            return null;
        }
        return best;
    }

    /// Whether a page starting `ahead` rows past the first unpublished row is within the window.
    ///
    /// @param waitsForDecode whether the batch the slowest column publishes next waits for decoding
    private boolean withinWindow(long ahead, boolean waitsForDecode) {
        return ahead < windowRows
                || (ahead < idleWindowRows && waitsForDecode && context.hasIdleDecodeThread());
    }

    private boolean allDone() {
        for (int i = 0; i < columns.size(); i++) {
            ScheduledColumn column = columns.get(i);
            if (!column.isDone() && !(column.fetchEnded() && column.peekReady() == null)) {
                return false;
            }
        }
        return true;
    }
}
