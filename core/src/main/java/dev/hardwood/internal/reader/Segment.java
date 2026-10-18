/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

/// The part of one page that goes into one [Cell]: a range of the page's kept records, and where
/// it lands in the cell.
///
/// A page splits into one segment per batch it touches, in page order. The cell offset is a
/// running sum over the cell's segments in page order, assigned when the segment is placed. Every
/// segment of a cell writes a disjoint range of the cell's values, and only the words at the
/// edges of its validity bits are shared with a neighbour.
final class Segment {

    final Cell cell;
    final PageTask page;
    /// The page's kept records `[recordStart, recordEnd)`.
    final int recordStart;
    final int recordEnd;
    /// Where the segment's first record lands in the cell.
    final int cellOffset;
    /// Whether the fill wrote the segment's validity bits, which it does only for a segment
    /// holding a null; the cell's completion sets the bits of every other segment.
    boolean wroteValidity;

    // === Binary columns ([BinaryFill]) ===
    /// The bytes of the segment's plain values, from its page's measure.
    int bytes;
    /// Where the segment's bytes start in the cell's byte buffer, assigned when the cell is sized.
    int byteOffset;
    /// Whether the fill wrote the segment's entry ids; the cell's completion marks every other
    /// segment's values as having none.
    boolean wroteIds;
    /// The segment's bytes while it waits for its cell to be sized ([Cell#park]).
    BinaryPageValues parkedValues;

    Segment(Cell cell, PageTask page, int recordStart, int recordEnd, int cellOffset) {
        this.cell = cell;
        this.page = page;
        this.recordStart = recordStart;
        this.recordEnd = recordEnd;
        this.cellOffset = cellOffset;
    }

    int records() {
        return recordEnd - recordStart;
    }
}
