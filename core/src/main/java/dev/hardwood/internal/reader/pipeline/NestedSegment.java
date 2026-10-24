/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.pipeline;

/// The part of one measured page that goes into one [NestedCell]: a range of the page's records,
/// the range of its values those records hold, and where both land in the cell.
///
/// A page splits into one segment per batch it touches, in page order. A segment of no records
/// carries the values a page begins with before its first record start: they continue the last
/// record of the cell they go into. The cell offsets are running sums over the cell's segments in
/// page order, assigned when the segment is placed.
final class NestedSegment {

    final NestedPageTask page;
    /// The page's records `[recordStart, recordEnd)`.
    final int recordStart;
    final int recordEnd;
    /// The page's values `[valueStart, valueEnd)`.
    final int valueStart;
    final int valueEnd;
    /// Where the segment's first record and value land in the cell.
    final int cellRecordOffset;
    final int cellValueOffset;

    NestedSegment(NestedPageTask page, int recordStart, int recordEnd, int valueStart, int valueEnd,
                  int cellRecordOffset, int cellValueOffset) {
        this.page = page;
        this.recordStart = recordStart;
        this.recordEnd = recordEnd;
        this.valueStart = valueStart;
        this.valueEnd = valueEnd;
        this.cellRecordOffset = cellRecordOffset;
        this.cellValueOffset = cellValueOffset;
    }

    int values() {
        return valueEnd - valueStart;
    }
}
