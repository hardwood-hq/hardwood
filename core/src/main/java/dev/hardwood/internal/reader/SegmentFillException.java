/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

/// The failure of a segment's fill, carrying the page the segment reads, for a fill that runs on
/// another page's task: a nested cell's fill ([NestedFill]). [ScheduledColumn#pageFailed] unwraps it, so the
/// failure names the segment's page; it never leaves the column.
final class SegmentFillException extends RuntimeException {

    final PageTask page;

    SegmentFillException(PageTask page, RuntimeException cause) {
        super(cause.getMessage(), cause, false, false);
        this.page = page;
    }

    /// The failure the segment's fill raised.
    RuntimeException failure() {
        return (RuntimeException) getCause();
    }
}
