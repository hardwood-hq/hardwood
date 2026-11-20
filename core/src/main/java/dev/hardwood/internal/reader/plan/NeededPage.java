/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.plan;

import dev.hardwood.internal.reader.select.PageRowMask;
import dev.hardwood.metadata.PageLocation;

/// A page a column reads, paired with the [PageRowMask] selecting which of its records the
/// column keeps: [PageRowMask#ALL] without a filter or where the page lies entirely inside the
/// matching rows. `pageIndex` is the page's ordinal in the whole column chunk, as the offset
/// index numbers it, not its position among the pages kept; `rows` is its row span in the
/// offset index, before the mask.
public record NeededPage(PageLocation location, PageRowMask mask, int pageIndex, int rows) {

    /// A chunk's dictionary page, the `length` bytes at `offset`, which holds no row.
    public static NeededPage dictionary(long offset, int length) {
        return new NeededPage(new PageLocation(offset, length, 0), PageRowMask.ALL, -1, 0);
    }
}
