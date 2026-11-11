/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.pipeline;

/// The failure of a segment's fill, carrying the page the segment reads, for a fill that runs on
/// another page's task: a nested cell's fill ([NestedFill]). The fill's job unwraps it, so the
/// failure names the segment's page; it never leaves the read.
public final class SegmentFillException extends RuntimeException {

    final PageTask page;

    SegmentFillException(PageTask page, RuntimeException cause) {
        super(cause.getMessage(), cause, false, false);
        this.page = page;
    }

    /// The page the failed segment reads.
    public PageTask page() {
        return page;
    }

    /// The failure the segment's fill raised.
    public RuntimeException failure() {
        return (RuntimeException) getCause();
    }
}
