/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

/// A page of a [ScheduledNestedColumn]: its task decodes it into page arrays of its own and
/// measures it, and the page keeps them until every cell it is placed into is filled.
final class NestedPageTask extends PageTask {

    /// The page's levels, record starts and byte prefix, which outlive its task.
    final PageDecoder.LevelScratch scratch = new PageDecoder.LevelScratch();
    /// The decoded page, trimmed to its kept records; written by its task before it is measured.
    Page page;
    /// The page's measure; written by its task before [#measured] is set.
    PageMeasure measure;
    /// Guarded by the column lock: whether the page is measured, so that it can be placed.
    boolean measured;

    NestedPageTask(PageInfo info, String fileName, int rowGroup, int pageIndex, BatchPlan.RowGroupBatches batches,
                   PageDecoder decoder, long firstRow) {
        super(info, fileName, rowGroup, pageIndex, batches, decoder, firstRow);
    }
}
