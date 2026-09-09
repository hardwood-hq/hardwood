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
import dev.hardwood.internal.ReadScope;
import dev.hardwood.metadata.FieldPath;

/// Per-column iterator that yields [PageInfo] objects across all row groups and files.
///
/// For each row group, obtains a [FetchPlan] from [RowGroupIterator#getColumnPlan]
/// and drains its page iterator. `PageSource` is agnostic of whether pages are
/// pre-computed (OffsetIndex) or lazily discovered (sequential scan) — both are
/// hidden behind the [FetchPlan] iterator.
///
/// This is the only interface the [ColumnWorker] sees — a simple
/// `PageInfo next()` iterator.
public class PageSource {

    private final RowGroupIterator rowGroupIterator;
    private final int projectedColumnIndex;

    // Current position in the work list
    /// How far this column has walked the work list. Each step may plan another
    /// file, so the list can grow behind the cursor.
    private int workItemCursor;

    // Current row group's page iterator
    private PageIterator currentPlan;

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

    /// The column this source yields pages for, by path.
    private FieldPath columnPath() {
        return rowGroupIterator.projectedSchema()
                .getProjectedColumn(projectedColumnIndex).fieldPath();
    }

    /// Returns the name of the file currently being read, or `null` if no work item
    /// is active. Only valid on the retriever thread.
    public String getCurrentFileName() {
        return currentWorkItem != null ? currentWorkItem.inputFile().name() : null;
    }

    /// Index of the row group currently being read, or
    /// [dev.hardwood.internal.ExceptionContext.ReadContext#UNKNOWN_ROW_GROUP]
    /// if no work item is active. Only valid on the retriever thread.
    public int getCurrentRowGroupIndex() {
        return currentWorkItem != null
                ? currentWorkItem.rowGroupIndex()
                : ExceptionContext.ReadContext.UNKNOWN_ROW_GROUP;
    }

    /// Whether statistics proved the current work item's row group matches the filter
    /// predicate in full. Only valid on the retriever thread.
    public boolean isCurrentFilterAlwaysMatches() {
        return currentWorkItem != null && currentWorkItem.filterAlwaysMatches();
    }

    /// Whether a filter predicate is installed on the underlying iterator, so that the
    /// rows this source yields can outnumber the rows a reader returns.
    public boolean isFilterActive() {
        return rowGroupIterator.hasFilter();
    }

    public PageInfo next() throws IOException {
        while (true) {
            if (currentPlan != null) {
                // Everything getting a page does — the plan, the filters, the
                // index, the fetch, the header — happens under this column, so
                // it is named once here rather than by each of them. `hasNext`
                // is inside it because deciding whether a page is coming reads
                // the headers that say so.
                try (ReadScope.Scope scope = ReadScope.file(getCurrentFileName())
                        .rowGroup(getCurrentRowGroupIndex())
                        .column(columnPath())) {
                    if (currentPlan.hasNext()) {
                        return currentPlan.next();
                    }
                }
            }

            // currentPlan is exhausted (or null) — this column is done with the
            // previous work item. Release our reference so the iterator can
            // evict its caches once every column has advanced past it.
            if (currentWorkItem != null) {
                rowGroupIterator.releaseWorkItem(currentWorkItem);
                currentWorkItem = null;
            }

            RowGroupIterator.WorkItem workItem = rowGroupIterator.workItemAt(workItemCursor);
            if (workItem == null) {
                return null;
            }
            workItemCursor++;
            FetchPlan plan;
            // Planning reads too — the page index, the dictionary, the chunk it
            // sits in — and the work item names the file and row group that
            // `getCurrentRowGroupIndex` cannot until the plan is in hand.
            try (ReadScope.Scope scope = ReadScope.file(workItem.inputFile().name())
                    .rowGroup(workItem.rowGroupIndex())
                    .column(columnPath())) {
                plan = rowGroupIterator.getColumnPlan(workItem, projectedColumnIndex);
            }
            currentPlan = plan.isEmpty() ? null : plan.pages();
            currentWorkItem = workItem;
        }
    }
}
