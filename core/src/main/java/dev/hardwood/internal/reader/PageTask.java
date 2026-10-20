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

/// One page of a scheduled column on its way into batches: the page, where it came from, its
/// place in the read's row order, the decoder its task decodes it with, and the [Segment]s the
/// column's fetcher placed it as. Its subclass [SealedFillTask] stands for no page: it carries
/// [PageInfo#BOUNDARY_MARKER], so it reports as a boundary marker.
///
/// The fetcher creates and places it, and the read's scheduler hands it to its task through the
/// executor, which orders the fetcher's writes before the task's reads.
class PageTask {

    final PageInfo info;
    /// The file, row group and page ordinal the page came from, which a failure met while
    /// decoding or filling it is placed with.
    final String fileName;
    final int rowGroup;
    final int pageIndex;
    /// The batches of the page's row group, from the read's batch plan.
    final BatchPlan.RowGroupBatches batches;
    /// The decoder of the page's column chunk.
    final PageDecoder decoder;
    /// The read's row, counted in kept rows over every row group before, of the page's first kept
    /// record. Pages reach the decode pool in this order across the read's columns.
    final long firstRow;
    /// The segments the page was placed as, in page order.
    final List<Segment> segments = new ArrayList<>(2);

    PageTask(PageInfo info, String fileName, int rowGroup, int pageIndex, BatchPlan.RowGroupBatches batches,
             PageDecoder decoder, long firstRow) {
        this.info = info;
        this.fileName = fileName;
        this.rowGroup = rowGroup;
        this.pageIndex = pageIndex;
        this.batches = batches;
        this.decoder = decoder;
        this.firstRow = firstRow;
    }

    /// Whether the page stands for a row group the column is not read in.
    boolean isBoundaryMarker() {
        return info.isBoundaryMarker();
    }

    /// The records the page contributes after its mask.
    int records() {
        return info.records();
    }
}
