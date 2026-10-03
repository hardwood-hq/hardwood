/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

/// What one data page contributes to its chunk's `ColumnIndex`.
///
/// @param min the page's lower bound, `null` where no value extended it
/// @param max the page's upper bound, `null` where no value extended it
/// @param valueCount the page's present values
/// @param nullCount the page's absent slots
/// @param nanCount the page's `NaN` values, or [StatisticsCollector#NO_NAN_COUNT]
record PageBounds(byte[] min, byte[] max, int valueCount, long nullCount, long nanCount) {

    /// The bounds of a page whose values `page` has collected, merging it into `chunk`.
    static <C extends StatisticsCollector<C>> PageBounds finish(C chunk, C page, int valueCount, long nullCount) {
        page.addNulls(nullCount);
        chunk.merge(page);
        boolean bounded = page.hasValues();
        return new PageBounds(bounded ? page.indexMin() : null, bounded ? page.indexMax() : null, valueCount,
                nullCount, page.nanCount());
    }
}
