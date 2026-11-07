/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.plan;

import java.util.List;

import dev.hardwood.internal.reader.fetch.FetchPlan;
import dev.hardwood.jfr.PageFilterEvent;

/// The fetch plans of one row group, one per decoded column, its batches, `null` when the
/// read plans no batches, and the [PageFilterEvent]s the read commits once it reaches the
/// row group.
record RowGroupPlans(FetchPlan[] columns, BatchPlan.RowGroupBatches batches,
                     List<PageFilterEvent> pageFilterEvents) {}
