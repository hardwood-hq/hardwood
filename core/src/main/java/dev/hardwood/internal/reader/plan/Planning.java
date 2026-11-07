/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.plan;

import java.util.concurrent.atomic.AtomicBoolean;

import dev.hardwood.internal.reader.fetch.CoalescingPolicy.Extent;
import dev.hardwood.internal.reader.fetch.Sections;

/// One work item's state: what its stages have decided so far and the bytes they read, and
/// whether the read has entered it and reported its drop.
final class Planning {

    final WorkItem workItem;
    /// The bytes its stages read outside a window: Bloom filters without a length, dictionary
    /// pages and page-format probes.
    final PlanningReads reads;
    final RowGroupPruning.Probe probe = new RowGroupPruning.Probe();
    RowGroupPruning.Arrival arrival;
    SharedRowGroupMetadata shared;
    MaskCapability mask;
    RowGroupPlans plans;
    /// The validated chunk extents of the decoded columns, taken before any stage reads a
    /// chunk (`Planner.chunkExtents`).
    Extent[] extents;
    final AtomicBoolean entered = new AtomicBoolean();
    final AtomicBoolean reported = new AtomicBoolean();

    Planning(WorkItem workItem, Sections sections) {
        this.workItem = workItem;
        this.reads = new PlanningReads(sections, workItem.inputFile(), "rg=" + workItem.rowGroupIndex(), 1);
    }
}
