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
import java.util.function.BiConsumer;

/// Submits the pages of a read's [ScheduledColumn]s to the decode pool, in row order across the
/// columns.
///
/// The pool runs tasks in the order they arrive. The scheduler submits the queued page that
/// starts at the lowest row of the read, so the tasks of the batch the consumer waits for are
/// ahead of every read-ahead task in the pool's queue, whatever the number of columns and their
/// page sizes. It has no thread of its own and does no I/O and no decoding: it runs on whichever
/// thread changes what may be submitted (a fetcher queuing a page, a page task ending, a column
/// publishing a batch or ending), one thread at a time, and a call made while another thread
/// submits makes that thread look again ([#submit()]).
///
/// A page is submitted once:
///
/// - every column whose queue is empty and whose next page could start before that page has
///   queued it, so that no page is submitted ahead of an earlier one;
/// - fewer than [#PAGES_PER_THREAD] pages per decode thread are submitted and not finished,
///   across the read;
/// - it holds rows of the batch its column publishes next, or it is within the column's
///   read-ahead depth: among the column's pages submitted and not published fewer than the pool
///   has threads, or starting fewer batches past that batch than the pool has threads, and in
///   either case within [#CAP_BUDGETS] batch byte budgets ([BatchSizing#budgetRows]) of it.
///
/// The depth gives every decode thread work on the column alone, in whichever unit runs out
/// first: a page, where pages are large against batches, or a batch, where many small pages
/// make up one and none of them stops counting until its whole batch is published. Beyond that
/// it decodes no further ahead than its threads need. The budgets cap the bytes a column
/// decodes ahead where its pages are large against the budget.
public final class ReadScheduler {

    /// Pages in flight per decode thread, across the read.
    private static final int PAGES_PER_THREAD = atLeastOne("hardwood.internal.pagesPerThread", 2);
    /// Batch byte budgets a column decodes at most ahead of the batch it publishes next.
    private static final int CAP_BUDGETS = 8;

    /// Whether flat columns are read by a scheduler; `false` reads every column through a
    /// [ColumnWorker], for tests comparing the two.
    public static final boolean ENABLED =
            !"false".equalsIgnoreCase(System.getProperty("hardwood.internal.scheduled"));

    /// Told of every page as it is submitted, with the pages in flight then; for tests of the
    /// submission order and the read-ahead bounds. `null` for none.
    static volatile BiConsumer<PageTask, Integer> submissionObserver;

    private final HardwoodContextImpl context;
    private final int pagesInFlight;
    private final int batchCapacity;
    private final long capRows;
    private final List<ScheduledColumn<?>> columns = new ArrayList<>();
    private final AtomicInteger submitted = new AtomicInteger();
    /// Calls to [#submit()] not yet served; the caller that raises it from zero submits.
    private final AtomicInteger submitting = new AtomicInteger();
    private volatile boolean started;

    /// @param context the context whose decode pool runs the page tasks
    /// @param batchCapacity the read's batch capacity
    /// @param budgetRows the rows of the read's batch byte budget ([BatchSizing#budgetRows])
    public ReadScheduler(HardwoodContextImpl context, int batchCapacity, long budgetRows) {
        this.context = context;
        this.pagesInFlight = PAGES_PER_THREAD * context.threads();
        this.batchCapacity = batchCapacity;
        this.capRows = CAP_BUDGETS * Math.max(batchCapacity, budgetRows);
    }

    private static int atLeastOne(String property, int defaultValue) {
        int value = Integer.getInteger(property, defaultValue);
        if (value < 1) {
            throw new IllegalArgumentException(property + " must be at least 1, but was " + value);
        }
        return value;
    }

    /// Adds a column; called by its constructor, before [#start()].
    void add(ScheduledColumn<?> column) {
        if (started) {
            throw new IllegalStateException("The scheduler has started");
        }
        columns.add(column);
    }

    /// Whether any column was added.
    public boolean hasColumns() {
        return !columns.isEmpty();
    }

    /// Starts every column's fetcher. Must be called once, after every column was added.
    public void start() {
        started = true;
        for (ScheduledColumn<?> column : columns) {
            column.start();
        }
    }

    /// Submits every page that may be submitted now. Called whenever that may have changed; the
    /// call that finds no other thread submitting does the submitting, and goes on until no call
    /// arrived meanwhile.
    void submit() {
        if (submitting.getAndIncrement() != 0) {
            return;
        }
        int calls = 1;
        do {
            try {
                submitReady();
            }
            catch (RuntimeException | Error e) {
                // Nothing else submits the columns' pages: every column ends with the failure
                for (ScheduledColumn<?> column : columns) {
                    column.fail(e);
                }
                throw e;
            }
            calls = submitting.addAndGet(-calls);
        } while (calls != 0);
    }

    void pageEnded() {
        submitted.decrementAndGet();
        submit();
    }

    private void submitReady() {
        ScheduledColumn<?> next;
        while ((next = next()) != null) {
            PageTask page = next.takeReady();
            if (page == null) {
                continue;
            }
            int inFlight = submitted.incrementAndGet();
            BiConsumer<PageTask, Integer> observer = submissionObserver;
            if (observer != null) {
                observer.accept(page, inFlight);
            }
            ScheduledColumn<?> column = next;
            try {
                context.executor().execute(() -> column.runPage(page));
            }
            catch (RejectedExecutionException e) {
                column.rejected(e);
            }
        }
    }

    /// The column whose queued page is to be submitted now, `null` if none is.
    private ScheduledColumn<?> next() {
        ScheduledColumn<?> best = null;
        long bestRow = Long.MAX_VALUE;
        // The lowest row a column with nothing queued may yet queue a page at. Its next row is
        // read before its queue, and a page queued later starts at or after it.
        long pendingRow = Long.MAX_VALUE;
        // The read's first row a column has not published: the slowest column's
        long readPublished = Long.MAX_VALUE;
        for (int i = 0; i < columns.size(); i++) {
            ScheduledColumn<?> column = columns.get(i);
            if (column.isDone()) {
                continue;
            }
            readPublished = Math.min(readPublished, column.publishedRow());
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
                || !withinDepth(best, bestRow, readPublished)) {
            return null;
        }
        return best;
    }

    /// Whether a page of `column` starting at the read's row `row` is within the read-ahead depth.
    /// A page starting at most one batch capacity past the column's first unpublished row always is:
    /// a column with repetition seals its open cell only on the page holding the next record start.
    private boolean withinDepth(ScheduledColumn<?> column, long row, long readPublished) {
        if (row - column.publishedRow() <= batchCapacity) {
            return true;
        }
        long ahead = row - readPublished;
        int threads = context.threads();
        return ahead < capRows && (column.pagesAhead() < threads || ahead < (long) threads * batchCapacity);
    }
}
