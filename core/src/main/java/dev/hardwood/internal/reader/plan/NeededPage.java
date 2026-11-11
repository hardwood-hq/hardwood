/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.plan;

import java.util.ArrayList;
import java.util.List;

import dev.hardwood.internal.reader.select.PageRowMask;
import dev.hardwood.internal.reader.select.RowRanges;
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

    /// The pages of `allPages` that hold a row of `matchingRows`, each with its mask.
    public static List<NeededPage> select(List<PageLocation> allPages, RowRanges matchingRows, long rowGroupRowCount) {
        List<NeededPage> needed = new ArrayList<>(matchingRows.isAll() ? allPages.size() : 8);
        for (int i = 0; i < allPages.size(); i++) {
            long pageFirstRow = allPages.get(i).firstRowIndex();
            int rows = pageRows(allPages, i, rowGroupRowCount);
            PageRowMask mask = matchingRows.isAll() ? PageRowMask.ALL
                    : matchingRows.maskForPage(pageFirstRow, pageFirstRow + rows);
            if (mask != null) {
                needed.add(new NeededPage(allPages.get(i), mask, i, rows));
            }
        }
        return needed;
    }

    /// The rows of page `i` by the offset index: up to the next page's first row, or to the end
    /// of the row group for the last page.
    private static int pageRows(List<PageLocation> allPages, int i, long rowGroupRowCount) {
        long pageLastRow = i + 1 < allPages.size() ? allPages.get(i + 1).firstRowIndex() : rowGroupRowCount;
        return Math.toIntExact(pageLastRow - allPages.get(i).firstRowIndex());
    }
}
