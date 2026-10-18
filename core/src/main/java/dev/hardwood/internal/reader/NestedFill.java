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

import dev.hardwood.schema.ColumnSchema;

/// Fills a sealed [NestedCell]: allocates its batch's arrays at their final size from its
/// segments' measures, writes each segment from its page, and computes the batch's index
/// ([NestedIndex]).
///
/// Each segment's values are one contiguous run of its page's kept values, read in place from the
/// page's bytes through a view of its own, and through its kept runs for a masked page. A segment
/// writes:
///
/// - values: decoded straight into the cell for primitive values; for binary values each value's
///   bytes at the segment's place in the cell's plain bytes, or its dictionary entry id;
/// - definition levels: copied, or filled with the column's maximum for an all-present page;
/// - repetition levels: copied, or filled with `0` for a column without repetition;
/// - the record offsets of the records it starts.
///
/// A fixed-size-list page writes no levels into a cell whose every segment is from a page of the
/// same `k`: the batch is fixed-width, its boundaries implicit. In any other cell it writes the
/// levels it omits: every element present, `0` at each record start and `1` for the record's
/// other `k - 1` elements.
///
/// The batch keeps its levels unless its index mode drops them (`REAL_VIEW`), which builds the
/// real-items view from levels written to the filling thread's scratch instead, and only the
/// levels the view reads: the repetition levels of a column with repetition, and the definition
/// levels of a batch that is not all present.
final class NestedFill {

    private static final ThreadLocal<PageDecoder.LevelScratch> VIEW_LEVELS =
            ThreadLocal.withInitial(PageDecoder.LevelScratch::new);

    private NestedFill() {
    }

    /// Fills `cell` into a batch taken from `exchange`.
    ///
    /// @throws IllegalStateException if the cell's values draw on two dictionaries, or its bytes
    ///         would exceed 2 GB
    static NestedBatch fill(NestedCell cell, BatchExchange<NestedBatch> exchange, ColumnSchema column,
                            NestedLevelComputer.Layers layers, NestedIndexMode mode,
                            BinaryBatchValues.ViewDemand viewDemand) {
        List<NestedSegment> segments = cell.segments();
        int values = cell.values();
        int records = cell.records();
        int maxDefinitionLevel = column.maxDefinitionLevel();
        int k = -1;
        boolean allPresent = true;
        for (int i = 0; i < segments.size(); i++) {
            PageMeasure measure = segments.get(i).page.measure;
            k = k < 0 || k == measure.fixedListK() ? measure.fixedListK() : 0;
            allPresent &= measure.allPresent();
        }
        boolean fixedWidth = k > 0;
        boolean keepLevels = mode != NestedIndexMode.REAL_VIEW;

        int[] definitionLevels = null;
        int[] repetitionLevels = null;
        if (!fixedWidth && keepLevels) {
            definitionLevels = new int[values];
            repetitionLevels = new int[values];
        }
        else if (!fixedWidth) {
            PageDecoder.LevelScratch scratch = VIEW_LEVELS.get();
            definitionLevels = allPresent ? null : scratch.definitionLevels(values);
            repetitionLevels = column.maxRepetitionLevel() > 0 ? scratch.repetitionLevels(values) : null;
        }

        Object out = BatchExchange.allocateArray(column, values);
        int[] recordOffsets = new int[records];
        if (out instanceof BinaryBatchValues binary) {
            fillBinary(segments, values, binary, viewDemand);
        }
        for (int i = 0; i < segments.size(); i++) {
            NestedSegment segment = segments.get(i);
            if (!(out instanceof BinaryBatchValues)) {
                try {
                    copyInPlace(segment, out);
                }
                catch (RuntimeException e) {
                    throw new SegmentFillException(segment.page, e);
                }
            }
            copyLevels(segment, definitionLevels, repetitionLevels, maxDefinitionLevel);
            PageMeasure measure = segment.page.measure;
            int shift = segment.cellValueOffset - segment.valueStart;
            for (int r = segment.recordStart; r < segment.recordEnd; r++) {
                recordOffsets[segment.cellRecordOffset + r - segment.recordStart] = measure.recordStart(r) + shift;
            }
        }

        NestedBatch batch = exchange.newBatch();
        batch.values = out;
        batch.recordCount = records;
        batch.valueCount = values;
        batch.recordOffsets = recordOffsets;
        batch.definitionLevels = keepLevels ? definitionLevels : null;
        batch.repetitionLevels = keepLevels ? repetitionLevels : null;
        batch.fixedListK = fixedWidth ? k : 0;
        batch.allPresent = allPresent;
        batch.fileName = cell.fileName;
        batch.filterAlwaysMatches = cell.alwaysMatches;
        NestedIndex.compute(batch, definitionLevels, repetitionLevels, maxDefinitionLevel, layers, mode);
        return batch;
    }

    /// Writes every segment's binary values: the plain values' bytes, each segment's at the sum of
    /// the bytes before it, and their views; the dictionary values' entry ids, their views built
    /// when a reader first needs them.
    private static void fillBinary(List<NestedSegment> segments, int valueCount, BinaryBatchValues out,
                                   BinaryBatchValues.ViewDemand viewDemand) {
        long plainBytes = 0;
        for (int i = 0; i < segments.size(); i++) {
            NestedSegment segment = segments.get(i);
            plainBytes += segment.page.measure.bytes(segment.valueStart, segment.valueEnd);
            if (plainBytes > Integer.MAX_VALUE) {
                throw BinaryBatchValues.bufferOverflow(segment.cellValueOffset);
            }
        }
        out.prepareBytes((int) plainBytes);
        int destByte = 0;
        boolean[] wroteIds = new boolean[segments.size()];
        for (int i = 0; i < segments.size(); i++) {
            NestedSegment segment = segments.get(i);
            try {
                destByte = fillBinary(segment, out, destByte, wroteIds, i);
            }
            catch (RuntimeException e) {
                throw new SegmentFillException(segment.page, e);
            }
        }
        if (out.dictionary != null) {
            for (int i = 0; i < segments.size(); i++) {
                if (!wroteIds[i]) {
                    NestedSegment segment = segments.get(i);
                    Arrays.fill(out.dictIndices, segment.cellValueOffset, segment.cellValueOffset + segment.values(), -1);
                }
            }
        }
        out.finishFill(valueCount, viewDemand);
        if (viewDemand.requested) {
            out.ensureViews();
        }
    }

    /// Writes one segment's binary values: its plain values' bytes from `destByte` on and their
    /// views, or its entry ids, recording in `wroteIds[index]` that it wrote ids.
    ///
    /// @return the byte after the segment's plain bytes
    private static int fillBinary(NestedSegment segment, BinaryBatchValues out, int destByte, boolean[] wroteIds,
                                  int index) {
        // Each fill reads through a view of its own: in-place values keep a cursor, and two cells
        // sharing the page may be filled from it at once
        BinaryPageValues values = segment.page.measure.binary().view();
        KeptRuns runs = segment.page.inPlace.runs();
        Dictionary.ByteArrayDictionary dictionary = values.dictionary();
        int[] ids = null;
        if (dictionary != null) {
            ids = out.installDictionary(dictionary);
            if (!values.allPresent()) {
                out.installViews();
            }
            wroteIds[index] = true;
        }
        else {
            out.installViews();
        }
        int kept = segment.valueStart;
        int end = segment.valueEnd;
        while (kept < end) {
            int run = runs == null ? 0 : runs.runOf(kept);
            int n = runs == null ? end - kept : Math.min(end - kept, runs.remaining(run, kept));
            int pagePos = runs == null ? kept : runs.pagePosition(run, kept);
            int destPos = segment.cellValueOffset + kept - segment.valueStart;
            if (ids != null) {
                values.copyIds(pagePos, n, ids, out.fillStarts(), out.fillEnds(), destPos);
            }
            else {
                destByte = values.copyPlain(pagePos, n, out.fillStarts(), out.fillEnds(), destPos, out.fillBytes(),
                        destByte);
            }
            kept += n;
        }
        return destByte;
    }

    /// Writes a segment of a page read in place straight from the page's bytes, through the kept
    /// runs of a masked page.
    private static void copyInPlace(NestedSegment segment, Object out) {
        PageDecoder.InPlaceNestedPage inPlace = segment.page.inPlace;
        FlatPageValues values = inPlace.flat().view();
        KeptRuns runs = inPlace.runs();
        if (runs == null) {
            values.copy(segment.valueStart, out, segment.cellValueOffset, segment.values());
            return;
        }
        int kept = segment.valueStart;
        while (kept < segment.valueEnd) {
            int run = runs.runOf(kept);
            int n = Math.min(segment.valueEnd - kept, runs.remaining(run, kept));
            values.copy(runs.pagePosition(run, kept), out, segment.cellValueOffset + kept - segment.valueStart, n);
            kept += n;
        }
    }

    /// Writes the segment's levels into the arrays given, each `null` when it is not written.
    private static void copyLevels(NestedSegment segment, int[] definitionLevels, int[] repetitionLevels,
                                   int maxDefinitionLevel) {
        PageDecoder.InPlaceNestedPage inPlace = segment.page.inPlace;
        int from = segment.valueStart;
        int dest = segment.cellValueOffset;
        int length = segment.values();
        int k = inPlace.fixedListK();
        if (definitionLevels != null) {
            int[] pageDefinitionLevels = inPlace.definitionLevels();
            if (k == 0 && pageDefinitionLevels != null) {
                System.arraycopy(pageDefinitionLevels, from, definitionLevels, dest, length);
            }
            else {
                Arrays.fill(definitionLevels, dest, dest + length, maxDefinitionLevel);
            }
        }
        if (repetitionLevels != null) {
            int[] pageRepetitionLevels = inPlace.repetitionLevels();
            if (k > 0) {
                // A fixed-size-list page's records are k-value runs from its first value
                Arrays.fill(repetitionLevels, dest, dest + length, 1);
                for (int v = 0; v < length; v += k) {
                    repetitionLevels[dest + v] = 0;
                }
            }
            else if (pageRepetitionLevels != null) {
                System.arraycopy(pageRepetitionLevels, from, repetitionLevels, dest, length);
            }
            else {
                Arrays.fill(repetitionLevels, dest, dest + length, 0);
            }
        }
    }
}
