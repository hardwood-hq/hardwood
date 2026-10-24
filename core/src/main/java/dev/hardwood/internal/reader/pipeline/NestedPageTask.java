/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.pipeline;

import java.util.concurrent.atomic.AtomicInteger;

import dev.hardwood.internal.reader.decode.PageDecoder;
import dev.hardwood.internal.reader.decode.PageInfo;
import dev.hardwood.internal.reader.plan.BatchPlan;

/// A page of a [ScheduledNestedColumn]: its task decompresses it into scratch of its own (or leaves
/// values stored uncompressed in the fetched page), decodes its levels and measures it, and the
/// page keeps the scratch until every cell it is placed into is filled.
final class NestedPageTask extends PageTask {

    /// The page's levels, record starts and byte prefix, which outlive its task.
    final PageDecoder.LevelScratch scratch;
    /// Segments placed from the page whose cells are not filled yet; the page's scratch goes back
    /// to its column's pool once its placement has ended and the count is back at zero.
    final AtomicInteger unfilledSegments = new AtomicInteger();
    /// The page read in two passes: its levels, kept to its mask's records, and its values in
    /// place in its scratch's bytes or the fetched page; written by its task before it is measured.
    PageDecoder.InPlaceNestedPage inPlace;
    /// The page's measure; written by its task before [#measured] is set.
    PageMeasure measure;
    /// Guarded by the column lock: whether the page is measured, so that it can be placed.
    boolean measured;

    NestedPageTask(PageInfo info, String fileName, int rowGroup, int pageIndex, BatchPlan.RowGroupBatches batches,
                   PageDecoder decoder, long firstRow, PageDecoder.LevelScratch scratch) {
        super(info, fileName, rowGroup, pageIndex, batches, decoder, firstRow);
        this.scratch = scratch;
    }
}
