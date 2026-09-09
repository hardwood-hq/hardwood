/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;

/// Per-column iterator that yields [PageInfo] objects across all row groups and files.
///
/// For each row group, obtains a [FetchPlan] from [RowGroupIterator#getColumnPlan]
/// and drains its page iterator. `PageSource` is agnostic of whether pages are
/// pre-computed (OffsetIndex) or lazily discovered (sequential scan) — both are
/// hidden behind the [FetchPlan] iterator.
///
/// This is the only interface the [ColumnWorker] sees: [#nextWorkItem] to step to the
/// next file and row group, [#nextPage] to walk that one's pages.
public class PageSource {

    private final RowGroupIterator rowGroupIterator;
    private final int projectedColumnIndex;

    // Current position in the work list
    /// How far this column has walked the work list. Each step may plan another
    /// file, so the list can grow behind the cursor.
    private int workItemCursor;

    // Current row group's page iterator, null when the work item has no pages for this
    // column or has not been planned yet — `planned` tells the two apart.
    private PageIterator currentPlan;
    private boolean planned;

    // Work item the current plan was built from. Tracked so that we can call
    // RowGroupIterator#releaseWorkItem when this column advances past it,
    // letting the iterator evict cached chunk bytes once all columns are done.
    private RowGroupIterator.WorkItem currentWorkItem;

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

    /// Whether a filter predicate is installed on the underlying iterator, so that the
    /// rows this source yields can outnumber the rows a reader returns.
    public boolean isFilterActive() {
        return rowGroupIterator.hasFilter();
    }

    /// Advances to the next work item this column has to read, or `null` once it has
    /// read them all.
    ///
    /// The work item is the file and row group a caller enters before asking for pages:
    /// which one is being read holds for every page of it, so it is named once, by whoever
    /// walks the list, rather than restated by each page.
    ///
    /// @return the work item now being read, or `null` when there are no more
    public RowGroupIterator.WorkItem nextWorkItem() throws IOException {
        // This column is done with the previous work item. Release our reference so the
        // iterator can evict its caches once every column has advanced past it.
        if (currentWorkItem != null) {
            rowGroupIterator.releaseWorkItem(currentWorkItem);
            currentWorkItem = null;
        }
        currentPlan = null;
        planned = false;

        RowGroupIterator.WorkItem workItem = rowGroupIterator.workItemAt(workItemCursor);
        if (workItem == null) {
            return null;
        }
        workItemCursor++;
        currentWorkItem = workItem;
        return workItem;
    }

    /// The next page of the work item [#nextWorkItem] last returned, or `null` once that
    /// work item has no more.
    ///
    /// The plan is built on the first page asked for rather than when the work item is
    /// taken, so that planning — the page index, the dictionary, the chunk it sits in —
    /// happens inside the caller's scope for that work item.
    ///
    /// @return the next page, or `null` when this work item is exhausted
    public PageInfo nextPage() throws IOException {
        if (!planned) {
            planned = true;
            FetchPlan plan = rowGroupIterator.getColumnPlan(currentWorkItem, projectedColumnIndex);
            currentPlan = plan.isEmpty() ? null : plan.pages();
        }
        return currentPlan != null && currentPlan.hasNext() ? currentPlan.next() : null;
    }
}
