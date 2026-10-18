/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.Arrays;
import java.util.List;

/// Fills a [Segment] of a flat binary column's cell from its page's [BinaryPageValues], and sizes
/// and finishes the cell's [BinaryBatchValues].
///
/// A cell's batch has two kinds of arrays:
///
/// - the entry ids, the views and the validity, sized by the plan's records: whichever segment
///   writes one first installs it, so a fill never waits for them;
/// - the byte buffer of the plain values, sized by the sum of its segments' bytes, which their
///   pages' measures give ([#size]).
///
/// A segment of dictionary ids writes its ids, and the empty views of its nulls; its values'
/// views are built when a reader needs them. A segment of plain values writes its views and its
/// bytes, at the offset the sizing gave it. One whose cell is not sized yet writes nothing and
/// parks with a view of its page ([Cell#park]), whose bytes the page keeps; the sizing writes its
/// views, bytes and validity straight from the page ([#unpark]). Every value is written once.
final class BinaryFill implements FlatFill.Run {

    private final BinaryPageValues values;
    private final FlatFill.ValidityWriter validity;
    private final int[] starts;
    private final int[] ends;
    /// The ids the segment writes into, `null` for plain values.
    private final int[] ids;
    /// Where the segment's bytes go, and the next one.
    private final byte[] dest;
    private int destByte;

    private BinaryFill(BinaryPageValues values, FlatFill.ValidityWriter validity, int[] starts, int[] ends,
                       int[] ids, byte[] dest, int destByte) {
        this.values = values;
        this.validity = validity;
        this.starts = starts;
        this.ends = ends;
        this.ids = ids;
        this.dest = dest;
        this.destByte = destByte;
    }

    /// Fills `segment` from `values` into the cell's batch `out`.
    ///
    /// @param maxDefinitionLevel the column's, below which a value is null
    /// @return whether the segment is filled; `false` when it parked, and the sizing completes it
    static boolean fill(Segment segment, BinaryPageValues values, BinaryBatchValues out, int maxDefinitionLevel) {
        FlatFill.ValidityWriter validity = new FlatFill.ValidityWriter(segment, maxDefinitionLevel);
        Dictionary.ByteArrayDictionary dictionary = values.dictionary();
        if (dictionary != null) {
            int[] ids = out.installDictionary(dictionary);
            // Only a null writes a view: an all-present page leaves the views to be built
            boolean nulls = !values.allPresent();
            if (nulls) {
                out.installViews();
            }
            FlatFill.forEachRun(segment, new BinaryFill(values, validity, nulls ? out.fillStarts() : null,
                    nulls ? out.fillEnds() : null, ids, null, 0));
            segment.wroteIds = true;
            return true;
        }
        out.installViews();
        Cell cell = segment.cell;
        if (segment.bytes > 0 && !cell.isSized() && cell.park(segment, values.view())) {
            return false;
        }
        writePlain(segment, values, out, validity);
        return true;
    }

    /// Writes a segment's plain values' views and bytes at its offset in the sized byte buffer.
    private static void writePlain(Segment segment, BinaryPageValues values, BinaryBatchValues out,
                                   FlatFill.ValidityWriter validity) {
        FlatFill.forEachRun(segment, new BinaryFill(values, validity, out.fillStarts(), out.fillEnds(), null,
                out.fillBytes(), segment.bytes > 0 ? segment.byteOffset : 0));
    }

    /// The bytes of the segment's plain values: `0` for ids.
    static int measure(Segment segment, BinaryPageValues values) {
        int[] bytes = new int[1];
        FlatFill.forEachRun(segment, (pageRow, destPos, length) -> bytes[0] += values.bytes(pageRow, pageRow + length));
        return bytes[0];
    }

    @Override
    public void write(int pageRow, int destPos, int length) {
        if (ids != null) {
            values.copyIds(pageRow, length, ids, starts, ends, destPos);
        }
        else {
            destByte = values.copyPlain(pageRow, length, starts, ends, destPos, dest, destByte);
        }
        validity.mark(values.definitionLevels(), pageRow, destPos, length);
    }

    /// Sizes a measured cell once it is sealed and every segment is measured: gives each segment
    /// its offset in the byte buffer, and the buffer the sum of their bytes.
    ///
    /// @return the segments parked before the sizing, whose bytes [#unpark] writes; `null` if none
    /// @throws IllegalStateException if the cell's bytes would exceed 2 GB
    static List<Segment> size(Cell cell, BinaryBatchValues out) {
        long total = 0;
        for (Segment segment : cell.segments()) {
            segment.byteOffset = (int) total;
            total += segment.bytes;
            if (total > Integer.MAX_VALUE) {
                throw BinaryBatchValues.bufferOverflow(segment.cellOffset);
            }
        }
        out.prepareBytes((int) total);
        return cell.markSized();
    }

    /// Fills a parked segment of a sized cell straight from its page: its views, bytes and validity.
    ///
    /// @param maxDefinitionLevel the column's, below which a value is null
    static void unpark(Segment segment, BinaryBatchValues out, int maxDefinitionLevel) {
        BinaryPageValues values = segment.parkedValues;
        segment.parkedValues = null;
        writePlain(segment, values, out, new FlatFill.ValidityWriter(segment, maxDefinitionLevel));
    }

    /// Finishes a completed cell's batch: in a batch with entry ids, the values of the segments
    /// that wrote none have none.
    static void finish(Cell cell, BinaryBatchValues out, BinaryBatchValues.ViewDemand demand) {
        if (out.dictionary != null) {
            for (Segment segment : cell.segments()) {
                if (!segment.wroteIds) {
                    Arrays.fill(out.dictIndices, segment.cellOffset, segment.cellOffset + segment.records(), -1);
                }
            }
        }
        out.finishFill(cell.records(), demand);
    }
}
