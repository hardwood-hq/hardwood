/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;

import dev.hardwood.internal.ExceptionContext;

/// Per-column iterator that yields [PageInfo] objects across all row groups and files.
///
/// For each row group, obtains a [FetchPlan] from [RowGroupIterator#getColumnPlan]
/// and drains its [PageWalk]. `PageSource` is agnostic of whether pages are
/// pre-computed (OffsetIndex) or lazily discovered (sequential scan) — both are
/// hidden behind the walk.
///
/// This is the only interface a [ScheduledColumn]'s fetch step sees: [#next] returns the next
/// page, or `null` with [#pending] naming what it needs (bytes, or the planning of the next work
/// item), or `null` at the end. A walk that stops on bytes keeps its place, and a column that
/// stops on a planning keeps its cursor, so the next call resumes either.
public class PageSource {

    private final RowGroupIterator rowGroupIterator;
    private final int projectedColumnIndex;

    // Current position in the work list
    /// How far this column has walked the work list. Each step may plan another
    /// file, so the list can grow behind the cursor.
    private int workItemCursor;

    // Current row group's page walk
    private PageWalk currentPlan;

    /// What the last [#next] stopped on.
    private Need pending;

    // Work item the current plan was built from. Tracked so that we can call
    // RowGroupIterator#releaseWorkItem when this column advances past it,
    // letting the iterator evict cached chunk bytes once all columns are done.
    private RowGroupIterator.WorkItem currentWorkItem;

    // Batches of the current work item's row group, from the read's batch plan.
    private BatchPlan.RowGroupBatches currentBatches;

    /// Creates a PageSource for the given column.
    ///
    /// @param rowGroupIterator shared iterator providing work items and metadata
    /// @param projectedColumnIndex the projected column index
    public PageSource(RowGroupIterator rowGroupIterator, int projectedColumnIndex) {
        this.rowGroupIterator = rowGroupIterator;
        this.projectedColumnIndex = projectedColumnIndex;
        // No work items taken here: asking for them plans the read, and a page
        // source is built per projected column when the reader is. The cursor
        // pulls them one at a time as this column advances. See #1107.
    }

    /// Returns the name of the file currently being read, or `null` if no work item
    /// is active. Only valid on the fetcher.
    public String getCurrentFileName() {
        return currentWorkItem != null ? currentWorkItem.inputFile().name() : null;
    }

    /// Index of the row group currently being read, or
    /// [dev.hardwood.internal.ExceptionContext#UNKNOWN_ROW_GROUP] if no work item is
    /// active. Only valid on the fetcher.
    public int getCurrentRowGroupIndex() {
        return currentWorkItem != null
                ? currentWorkItem.rowGroupIndex()
                : ExceptionContext.UNKNOWN_ROW_GROUP;
    }

    /// The batches of the current work item's row group, or `null` if no work item is active.
    /// Only valid on the fetcher.
    BatchPlan.RowGroupBatches getCurrentBatches() {
        return currentBatches;
    }

    /// Which page of the current column chunk is being read, or
    /// [dev.hardwood.internal.ExceptionContext#UNKNOWN_PAGE] when no plan is walking one.
    /// Only valid on the fetcher.
    public int getCurrentPageIndex() {
        return currentPlan != null ? currentPlan.currentPage() : ExceptionContext.UNKNOWN_PAGE;
    }

    /// Whether no work item is active: the column has not started, or has finished one and
    /// not yet been handed the next. Only valid on the fetcher.
    public boolean isBetweenWorkItems() {
        return currentWorkItem == null;
    }

    /// Whether a [dev.hardwood.MetadataSource] supplied the footers of the files this source
    /// reads. Fixed for the read, so valid on any thread.
    public boolean footersSupplied() {
        return rowGroupIterator.footersSupplied();
    }

    /// The next page, or `null` when there is none to return: the column's pages have ended, or
    /// the next page needs bytes that are not present or a work item that is not yet planned,
    /// which [#pending] then returns.
    public PageInfo next() throws IOException {
        pending = null;
        while (true) {
            if (currentPlan != null) {
                PageInfo page = currentPlan.advance();
                if (page != null) {
                    return page;
                }
                pending = currentPlan.pending();
                if (pending != null) {
                    return null;
                }
            }

            // currentPlan is exhausted (or null) — this column is done with the
            // previous work item. Release our reference so the iterator can
            // evict its caches once every column has advanced past it.
            if (currentWorkItem != null) {
                rowGroupIterator.releaseWorkItem(currentWorkItem);
                currentWorkItem = null;
                currentBatches = null;
            }

            WorkItemPlanning planning = rowGroupIterator.planning(workItemCursor);
            if (!planning.isDone()) {
                pending = planning;
                return null;
            }
            if (planning.failed()) {
                // A failure planning the row group is reported against its work item, one planning
                // the file against none. Taken from the planning: asking the iterator would take
                // the planning lock on the pool, which a planning holds while it waits for the pool
                currentWorkItem = planning.failedWorkItem();
                currentPlan = null;
            }
            RowGroupIterator.WorkItem workItem = planning.workItem();
            if (workItem == null) {
                return null;
            }
            workItemCursor++;
            // The planning computed the work item's plans, so taking this column's reads
            // nothing. The old plan goes: left in place, it would answer "which page" with the
            // last page of the row group before this one.
            currentWorkItem = workItem;
            currentPlan = null;
            FetchPlan plan = rowGroupIterator.getColumnPlan(workItem, projectedColumnIndex);
            currentBatches = rowGroupIterator.getBatches(workItem);
            currentPlan = plan.walk();
        }
    }

    /// What the last [#next] stopped on, bytes or a work item's planning, or `null` when it
    /// returned a page or ended.
    public Need pending() {
        return pending;
    }
}
