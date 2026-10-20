/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import dev.hardwood.internal.DecodeExecutor;

/// Submits the pages of a read's [ScheduledColumn]s to the decode pool, in row order across the
/// columns.
///
/// The pool runs page tasks in the order they arrive ([DecodeExecutor] puts only a decode a thread
/// waits for ahead of them). The scheduler submits the queued page that
/// starts at the lowest row of the read, so the tasks of the batch the consumer waits for are
/// ahead of every read-ahead task in the pool's queue, whatever the number of columns and their
/// page sizes. While that page is past the cap, every page below it is submitted; if fewer of
/// the read's pages are in flight than the pool has threads, the scheduler then submits the next
/// page of the first column past it whose page holds rows of the batch that column publishes
/// next, such as a filter-only column that has published far ahead of the read.
///
/// The scheduler has no thread of its own and does no I/O and no decoding: it runs on whichever
/// thread changes what may be submitted (a fetcher queuing a page, a page task ending, a column
/// publishing a batch or ending), one thread at a time, and a call made while another thread
/// submits makes that thread look again ([#submit()]).
///
/// A page is submitted once:
///
/// - every column whose queue is empty and whose next page could start before that page has
///   queued it, so that no page is submitted ahead of an earlier one not yet queued;
/// - fewer than [#PAGES_PER_THREAD] pages per decode thread are submitted and not finished,
///   across the read;
/// - it holds rows of the batch its column publishes next, or it starts within the cap: fewer
///   than [#CAP_BUDGETS] batch byte budgets ([BatchSizing#readAheadBudgetRows]) past the read's
///   first unpublished row, the slowest column's.
///
/// The scheduler keeps the columns ordered by the keys these conditions compare (the first row of
/// a column's next queued page, the next row of a column with nothing queued, the first row a
/// column has not published), and re-reads a column's keys only when the column reports a
/// change. Finding the page to submit costs a logarithm of the column count, not a pass over the
/// columns, so a read of thousands of columns is not quadratic in them; past a page beyond the
/// cap, the scheduler looks at no more columns than the pool has threads.
///
/// The cap bounds the bytes a read decodes ahead of its consumer, and is counted from the
/// slowest column, so every column reads ahead to the same row and a fast one does not spend it
/// running ahead of a slow one. The pages it admits keep the pool busy and give each column its
/// next page while the consumer reads the current one; the pages in flight per thread bound how
/// many of them decode at once.
public final class ReadScheduler {

    /// Pages in flight per decode thread, across the read.
    private static final int PAGES_PER_THREAD = atLeastOne("hardwood.internal.pagesPerThread", 2);
    /// Batch byte budgets a read decodes at most ahead of its first unpublished row.
    private static final int CAP_BUDGETS = 8;

    /// Told of every page as it is submitted; for tests of the submission order and the
    /// read-ahead bounds. `null` for none.
    static volatile SubmissionObserver submissionObserver;

    /// Sees each page the scheduler submits, on the submitting thread, after the page was taken
    /// off its column's queue and before its task is handed to the pool.
    interface SubmissionObserver {

        /// @param column the page's column
        /// @param page the page
        /// @param inFlight the pages in flight across the read, this one included
        /// @param lowestRow the first row of the lowest page queued across the read when the page
        ///     was chosen, the page's own row when it is that page
        /// @param readPublishedRow the read's first unpublished row the choice was made against
        void submitted(ScheduledColumn<?> column, PageTask page, int inFlight, long lowestRow, long readPublishedRow);
    }

    private final HardwoodContextImpl context;
    private final int pagesInFlight;
    private final int batchCapacity;
    private final long capRows;
    private final List<ScheduledColumn<?>> columns = new ArrayList<>();
    /// Columns whose state changed since they were last indexed, each at most once
    /// ([ScheduledColumn#changePending]).
    private final ConcurrentLinkedQueue<ScheduledColumn<?>> changed = new ConcurrentLinkedQueue<>();
    // The index, touched only by the thread submitting.
    /// Columns with a page queued, by the first row of their next queued page.
    private final TreeSet<ScheduledColumn<?>> queued = new TreeSet<>(
            Comparator.comparingLong((ScheduledColumn<?> c) -> c.indexedHeadRow).thenComparingInt(c -> c.scheduleId));
    /// Columns not done with nothing queued, by the row their next page starts at or after.
    private final TreeSet<ScheduledColumn<?>> pending = new TreeSet<>(
            Comparator.comparingLong((ScheduledColumn<?> c) -> c.indexedNextRow).thenComparingInt(c -> c.scheduleId));
    /// Columns not done, by their first row not published.
    private final TreeSet<ScheduledColumn<?>> unpublished = new TreeSet<>(
            Comparator.comparingLong((ScheduledColumn<?> c) -> c.indexedPublishedRow).thenComparingInt(c -> c.scheduleId));
    /// The lowest queued row and the read's first unpublished row of the last choice, for the
    /// [SubmissionObserver].
    private long chosenLowestRow;
    private long chosenReadPublished;
    private final AtomicInteger submitted = new AtomicInteger();
    /// Calls to [#submit()] not yet served; the caller that raises it from zero submits.
    private final AtomicInteger submitting = new AtomicInteger();
    private volatile boolean started;
    /// Set by the first [#stop()].
    private final AtomicBoolean stopped = new AtomicBoolean();

    /// @param context the context whose decode pool runs the page tasks
    /// @param batchCapacity the read's batch capacity
    /// @param budgetRows the rows of the read's batch byte budget ([BatchSizing#readAheadBudgetRows])
    public ReadScheduler(HardwoodContextImpl context, int batchCapacity, long budgetRows) {
        this.context = context;
        this.pagesInFlight = PAGES_PER_THREAD * context.threads();
        this.batchCapacity = batchCapacity;
        this.capRows = CAP_BUDGETS * budgetRows;
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
        column.scheduleId = columns.size();
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
            markChanged(column);
        }
        for (ScheduledColumn<?> column : columns) {
            column.start();
        }
    }

    /// Stops every column of the read without waiting for any ([ScheduledColumn#signalStop()]).
    /// Called by each column's close, on the thread closing the read; the first call stops them
    /// all and later ones do nothing.
    void stop() {
        if (!stopped.compareAndSet(false, true)) {
            return;
        }
        for (ScheduledColumn<?> column : columns) {
            column.signalStop();
        }
    }

    /// Submits every page that may be submitted now, after `column`'s state changed. Every change
    /// of a column's queue, next row, published rows or end is followed by this call, from the
    /// thread that made it; the call that finds no other thread submitting does the submitting,
    /// and goes on until no call arrived meanwhile.
    void submit(ScheduledColumn<?> column) {
        markChanged(column);
        submit();
    }

    private void markChanged(ScheduledColumn<?> column) {
        if (column.changePending.compareAndSet(false, true)) {
            changed.add(column);
        }
    }

    private void submit() {
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

    void pageEnded(ScheduledColumn<?> column) {
        submitted.decrementAndGet();
        submit(column);
    }

    private void submitReady() {
        ScheduledColumn<?> next;
        while ((next = next()) != null) {
            PageTask page = next.takeReady();
            index(next);
            if (page == null) {
                continue;
            }
            int inFlight = submitted.incrementAndGet();
            SubmissionObserver observer = submissionObserver;
            if (observer != null) {
                observer.submitted(next, page, inFlight, chosenLowestRow, chosenReadPublished);
            }
            ScheduledColumn<?> column = next;
            try {
                context.execute(buffers -> column.runPage(page, buffers));
            }
            catch (RejectedExecutionException e) {
                column.rejected(e);
            }
        }
    }

    /// The column whose queued page is to be submitted now, `null` if none is.
    private ScheduledColumn<?> next() {
        ScheduledColumn<?> column;
        while ((column = changed.poll()) != null) {
            // Cleared before the keys are read: a change after this marks the column again
            column.changePending.set(false);
            index(column);
        }
        if (queued.isEmpty()) {
            return null;
        }
        ScheduledColumn<?> best = queued.first();
        long bestRow = best.indexedHeadRow;
        // The lowest row a column with nothing queued may yet queue a page at. Its next row is
        // read before its queue, and a page queued later starts at or after it.
        long pendingRow = pending.isEmpty() ? Long.MAX_VALUE : pending.first().indexedNextRow;
        // The read's first row a column has not published: the slowest column's
        long readPublished = unpublished.isEmpty() ? Long.MAX_VALUE : unpublished.first().indexedPublishedRow;
        if (pendingRow < bestRow || submitted.get() >= pagesInFlight) {
            return null;
        }
        chosenLowestRow = bestRow;
        chosenReadPublished = readPublished;
        if (admitted(best, bestRow, readPublished)) {
            return best;
        }
        // The page at the lowest row is past the cap, so every page below it is submitted and
        // every later page is past the cap too. A later page is still admitted where it holds
        // rows of the batch its own column publishes next, as a column that has published far
        // ahead of the read does; such a page is worth submitting only to a thread the read
        // leaves idle. The search stops at a page a column with nothing queued may yet start
        // below, and passes at most as many columns as the pool has threads, so that it does not
        // grow with the column count.
        int threads = context.threads();
        if (submitted.get() >= threads) {
            return null;
        }
        int passed = 0;
        for (ScheduledColumn<?> candidate : queued.tailSet(best, false)) {
            if (candidate.indexedHeadRow > pendingRow || passed++ == threads) {
                return null;
            }
            if (admitted(candidate, candidate.indexedHeadRow, readPublished)) {
                return candidate;
            }
        }
        return null;
    }

    /// Re-reads `column`'s keys and moves it to where they place it in the index.
    private void index(ScheduledColumn<?> column) {
        if (column.indexedQueued) {
            queued.remove(column);
            column.indexedQueued = false;
        }
        if (column.indexedPending) {
            pending.remove(column);
            column.indexedPending = false;
        }
        if (column.indexedPublished) {
            unpublished.remove(column);
            column.indexedPublished = false;
        }
        if (column.isDone()) {
            return;
        }
        column.indexedPublishedRow = column.publishedRow();
        unpublished.add(column);
        column.indexedPublished = true;
        long nextRow = column.nextRow();
        PageTask head = column.peekReady();
        if (head == null) {
            column.indexedNextRow = nextRow;
            pending.add(column);
            column.indexedPending = true;
        }
        else {
            column.indexedHeadRow = head.firstRow;
            queued.add(column);
            column.indexedQueued = true;
        }
    }

    /// Whether a page of `column` starting at the read's row `row` may be submitted: it starts within
    /// the cap past the read's first unpublished row `readPublished`, or at most one batch capacity
    /// past its own column's first unpublished row. The latter keeps the read live: a column with
    /// repetition seals its open cell only on the page holding the next record start.
    private boolean admitted(ScheduledColumn<?> column, long row, long readPublished) {
        return row - column.publishedRow() <= batchCapacity || row - readPublished < capRows;
    }
}
