/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.pipeline;

import java.util.Arrays;

import dev.hardwood.internal.reader.batch.BinaryBatchValues;
import dev.hardwood.internal.reader.batch.Dictionary;
import dev.hardwood.internal.reader.batch.PlainBytes;
import dev.hardwood.internal.reader.decode.BinaryPageValues;

/// Fills a [Segment] of a flat binary column's cell from its page's [BinaryPageValues], and
/// finishes the cell's [BinaryBatchValues].
///
/// The entry ids, the views and the validity are sized by the plan's records: whichever segment
/// writes one first installs it, so a fill never waits for them. A segment of dictionary ids writes
/// its ids, and the empty views of its nulls; its values' views are built when a reader needs them.
/// A segment of plain values writes its views and copies its bytes into chunks of its own
/// ([PlainBytes]). Every value is written once.
final class BinaryFill implements FlatFill.Run {

    private final BinaryPageValues values;
    private final FlatFill.ValidityWriter validity;
    private final int[] starts;
    private final int[] ends;
    private final int[] bufferIds;
    /// The ids the segment writes into, `null` for plain values.
    private final int[] ids;
    /// Where the segment's bytes go, `null` for ids.
    private final PlainBytes dest;

    private BinaryFill(BinaryPageValues values, FlatFill.ValidityWriter validity, int[] starts, int[] ends,
                       int[] bufferIds, int[] ids, PlainBytes dest) {
        this.values = values;
        this.validity = validity;
        this.starts = starts;
        this.ends = ends;
        this.bufferIds = bufferIds;
        this.ids = ids;
        this.dest = dest;
    }

    /// Fills `segment` from `values` into the cell's batch `out`.
    ///
    /// @param maxDefinitionLevel the column's, below which a value is null
    static void fill(Segment segment, BinaryPageValues values, BinaryBatchValues out, int maxDefinitionLevel) {
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
                    nulls ? out.fillEnds() : null, nulls ? out.fillBufferIds() : null, ids, null));
            segment.wroteIds = true;
            return;
        }
        out.installViews();
        FlatFill.forEachRun(segment, new BinaryFill(values, validity, out.fillStarts(), out.fillEnds(),
                out.fillBufferIds(), null, new PlainBytes(out, measure(segment, values))));
    }

    /// The bytes of the segment's plain values.
    private static long measure(Segment segment, BinaryPageValues values) {
        long[] bytes = new long[1];
        FlatFill.forEachRun(segment, (pageRow, destPos, length) -> bytes[0] += values.bytes(pageRow, pageRow + length));
        return bytes[0];
    }

    @Override
    public void write(int pageRow, int destPos, int length) {
        if (ids != null) {
            values.copyIds(pageRow, length, ids, starts, ends, bufferIds, destPos);
        }
        else {
            values.copyPlain(pageRow, length, starts, ends, bufferIds, destPos, dest);
        }
        validity.mark(values.definitionLevels(), pageRow, destPos, length);
    }

    /// Finishes a completed cell's batch: in a batch with entry ids, the values of the segments
    /// that wrote none have none, and the ids are complete when every segment wrote them.
    static void finish(Cell cell, BinaryBatchValues out, BinaryBatchValues.ViewDemand demand) {
        if (out.dictionary != null) {
            boolean complete = true;
            for (Segment segment : cell.segments()) {
                if (!segment.wroteIds) {
                    Arrays.fill(out.dictIndices, segment.cellOffset, segment.cellOffset + segment.records(), -1);
                    complete = false;
                }
            }
            out.idsComplete = complete;
        }
        out.finishFill(cell.records(), demand);
    }
}
