/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.plan;

import java.io.IOException;

import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.reader.decode.PageInfo;
import dev.hardwood.internal.reader.fetch.FetchPlan;
import dev.hardwood.internal.reader.fetch.PageWalk;
import dev.hardwood.internal.reader.memory.WorkingSet;
import dev.hardwood.internal.reader.step.Need;
import dev.hardwood.schema.ColumnSchema;

/// Per-column iterator that yields [PageInfo] objects across all row groups and files.
///
/// For each row group, takes its [FetchPlan] from the planned work item's plans
/// ([RowGroupIterator#plansOf]) and drains its [PageWalk]. `PageSource` is agnostic of whether pages are
/// pre-computed (OffsetIndex) or lazily discovered (sequential scan) — both are
/// hidden behind the walk.
///
/// This is the only interface a [dev.hardwood.internal.reader.pipeline.ScheduledColumn]'s fetch
/// step sees: [#next] returns the next page, or `null` with [#pending] naming what it needs (bytes,
/// or the planning of the next work item), or `null` at the end. A walk that stops on bytes keeps
/// its place, and a column that stops on a planning keeps its cursor, so the next call resumes
/// either.
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

    /// The working set of the read this column is part of.
    public WorkingSet readWorkingSet() {
        return rowGroupIterator.workingSet();
    }

    /// The read's working-set handle for this column, whose chunk's schema is `schema`.
    public WorkingSet.Column workingSet(ColumnSchema schema) {
        return rowGroupIterator.workingSet().column(projectedColumnIndex, schema);
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
    public BatchPlan.RowGroupBatches getCurrentBatches() {
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

            if (!rowGroupIterator.isPlanned(workItemCursor)) {
                pending = rowGroupIterator.whenPlanned(workItemCursor);
                return null;
            }
            // A failure planning the row group is reported against its work item, one planning the
            // file against none
            currentWorkItem = rowGroupIterator.failedWorkItem(workItemCursor);
            currentPlan = null;
            RowGroupIterator.WorkItem workItem = rowGroupIterator.plannedWorkItem(workItemCursor);
            if (workItem == null) {
                return null;
            }
            workItemCursor++;
            // The planning computed the work item's plans, so taking this column's reads
            // nothing. The old plan goes: left in place, it would answer "which page" with the
            // last page of the row group before this one.
            currentWorkItem = workItem;
            currentPlan = null;
            RowGroupIterator.RowGroupPlans plans = rowGroupIterator.plansOf(workItem);
            FetchPlan plan = plans.columns()[projectedColumnIndex];
            currentBatches = plans.batches();
            rowGroupIterator.workingSet().enter(projectedColumnIndex, currentBatches.batchStartRow(),
                    currentBatches.readRowsBefore() + currentBatches.plannedRows());
            currentPlan = plan.walk();
        }
    }

    /// What the last [#next] stopped on, bytes or a work item's planning, or `null` when it
    /// returned a page or ended.
    public Need pending() {
        return pending;
    }
}
