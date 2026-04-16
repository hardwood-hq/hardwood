/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.Iterator;

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
    private final Iterator<RowGroupIterator.WorkItem> workItemIterator;

    // Current row group's page iterator
    private Iterator<PageInfo> currentPlan;

    /// Creates a PageSource for the given column.
    ///
    /// @param rowGroupIterator shared iterator providing work items and metadata
    /// @param projectedColumnIndex the projected column index
    public PageSource(RowGroupIterator rowGroupIterator, int projectedColumnIndex) {
        this.rowGroupIterator = rowGroupIterator;
        this.projectedColumnIndex = projectedColumnIndex;
        this.workItemIterator = rowGroupIterator.getWorkItems().iterator();
    }

    public PageInfo next() {
        while (true) {
            if (currentPlan != null && currentPlan.hasNext()) {
                return currentPlan.next();
            }

            if (!workItemIterator.hasNext()) {
                return null;
            }

            RowGroupIterator.WorkItem workItem = workItemIterator.next();
            FetchPlan plan = rowGroupIterator.getColumnPlan(workItem, projectedColumnIndex);
            currentPlan = plan.isEmpty() ? null : plan.pages();
        }
    }
}
