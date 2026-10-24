/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.pipeline;

import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.reader.decode.PageInfo;

/// A task of a [ScheduledColumn] that decodes no page: it fills ([ScheduledNestedColumn]) or
/// completes ([ScheduledFlatColumn]) the cells the column's fetcher sealed, which no page task may
/// be left to fill or complete. The column queues it ahead of its queued pages at the first row of
/// the first such cell, so the scheduler submits it in row order with every other task of the
/// read. It stands for no page.
final class SealedFillTask extends PageTask {

    SealedFillTask(long firstRow) {
        super(PageInfo.BOUNDARY_MARKER, null, ExceptionContext.UNKNOWN_ROW_GROUP,
                ExceptionContext.UNKNOWN_PAGE, null, null, firstRow);
    }
}
