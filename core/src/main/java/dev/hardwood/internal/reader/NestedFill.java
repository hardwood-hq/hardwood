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

import dev.hardwood.internal.DecodeBuffers;
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
/// - the record offsets of the records it starts, unless the index mode reads none (`REAL_VIEW`).
///
/// A fixed-size-list page writes no levels into a cell whose every segment is from a page of the
/// same `k`: the batch is fixed-width, its boundaries implicit. In any other cell it writes the
/// levels it omits: every element present, `0` at each record start and `1` for the record's
/// other `k - 1` elements.
///
/// The batch keeps its levels unless its index mode drops them (`REAL_VIEW`), which builds the
/// real-items view from levels written to the filling task's scratch instead, and only the
/// levels the view reads: the repetition levels of a column with repetition, and the definition
/// levels of a batch that is not all present. Every segment writes its levels before any writes
/// its values, so the view is built first; where the batch has phantom positions (null or empty
/// parents), its values are allocated at the real leaf count and each segment writes only its
/// real leaves, each straight to its real position.
final class NestedFill {

    private static final DecodeBuffers.Slot<PageDecoder.LevelScratch> VIEW_LEVELS = DecodeBuffers.slot();

    private NestedFill() {
    }

    /// Fills `cell` into a batch taken from `exchange`.
    ///
    /// @throws IllegalStateException if the cell's values draw on two dictionaries, or its bytes
    ///         would exceed 2 GB
    static NestedBatch fill(NestedCell cell, BatchExchange<NestedBatch> exchange, ColumnSchema column,
                            NestedLevelComputer.Layers layers, NestedIndexMode mode,
                            BinaryBatchValues.ViewDemand viewDemand, DecodeBuffers buffers) {
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
        PageDecoder.LevelScratch viewLevels = null;
        if (!fixedWidth && keepLevels) {
            definitionLevels = new int[values];
            repetitionLevels = new int[values];
        }
        else if (!fixedWidth) {
            viewLevels = buffers.get(VIEW_LEVELS);
            if (viewLevels == null) {
                viewLevels = new PageDecoder.LevelScratch();
                buffers.set(VIEW_LEVELS, viewLevels);
            }
            definitionLevels = allPresent ? null : viewLevels.definitionLevels(values);
            repetitionLevels = column.maxRepetitionLevel() > 0 ? viewLevels.repetitionLevels(values) : null;
        }

        for (int i = 0; i < segments.size(); i++) {
            copyLevels(segments.get(i), definitionLevels, repetitionLevels, maxDefinitionLevel);
        }
        NestedLevelComputer.RealView realView = keepLevels ? null
                : NestedIndex.realView(definitionLevels, repetitionLevels, values, records, fixedWidth ? k : 0,
                        allPresent, maxDefinitionLevel, layers);
        // A row below the leaf's item threshold is a phantom position, which takes no value slot.
        // The leaf's item repetition threshold is the number of REPEATED layers, which is the
        // column's maximum repetition level, so the definition level alone tells a phantom; each
        // write is checked against the view's count all the same.
        int slots = realView != null ? realView.valueCount() : values;
        int slotLevel = slots != values ? layers.itemDefThresholds()[layers.count()] : 0;

        Object out = BatchExchange.allocateArray(column, slots);
        if (out instanceof BinaryBatchValues binary) {
            fillBinary(segments, slots, binary, viewDemand, slotLevel);
        }
        else {
            int dest = 0;
            for (int i = 0; i < segments.size(); i++) {
                NestedSegment segment = segments.get(i);
                try {
                    dest += copyInPlace(segment, out, dest, slotLevel);
                }
                catch (RuntimeException e) {
                    throw new SegmentFillException(segment.page, e);
                }
            }
            requireAllSlots(dest, slots);
        }

        NestedBatch batch = exchange.newBatch();
        batch.values = out;
        batch.recordCount = records;
        batch.valueCount = values;
        batch.recordOffsets = keepLevels ? recordOffsets(segments, records) : null;
        batch.definitionLevels = keepLevels ? definitionLevels : null;
        batch.repetitionLevels = keepLevels ? repetitionLevels : null;
        batch.fixedListK = fixedWidth ? k : 0;
        batch.allPresent = allPresent;
        batch.fileName = cell.fileName;
        batch.filterAlwaysMatches = cell.alwaysMatches;
        batch.realView = realView;
        batch.realValues = realView != null ? out : null;
        NestedIndex.compute(batch, definitionLevels, repetitionLevels, maxDefinitionLevel, layers, mode);
        if (viewLevels != null) {
            // A cell of very long lists holds more values than a batch has rows
            viewLevels.trim();
        }
        return batch;
    }

    /// Checks that the segments wrote a value to every one of the batch's `slots`.
    private static void requireAllSlots(int written, int slots) {
        if (written != slots) {
            throw new IllegalStateException("A cell's segments wrote " + written + " leaf values where the batch has "
                    + slots);
        }
    }

    /// The cell value offset of each record the segments start.
    private static int[] recordOffsets(List<NestedSegment> segments, int records) {
        int[] recordOffsets = new int[records];
        for (int i = 0; i < segments.size(); i++) {
            NestedSegment segment = segments.get(i);
            PageMeasure measure = segment.page.measure;
            int shift = segment.cellValueOffset - segment.valueStart;
            for (int r = segment.recordStart; r < segment.recordEnd; r++) {
                recordOffsets[segment.cellRecordOffset + r - segment.recordStart] = measure.recordStart(r) + shift;
            }
        }
        return recordOffsets;
    }

    /// Writes every segment's binary values: the plain values' bytes, copied into the batch's
    /// chunks in segment order, and their views; the dictionary values' entry ids, their views built
    /// when a reader first needs them.
    private static void fillBinary(List<NestedSegment> segments, int valueCount, BinaryBatchValues out,
                                   BinaryBatchValues.ViewDemand viewDemand, int slotLevel) {
        long plainBytes = 0;
        for (int i = 0; i < segments.size(); i++) {
            NestedSegment segment = segments.get(i);
            plainBytes += segment.page.measure.bytes(segment.valueStart, segment.valueEnd);
        }
        PlainBytes dest = new PlainBytes(out, plainBytes);
        boolean[] wroteIds = new boolean[segments.size()];
        // Where each segment's values start among the batch's, its last entry the value count
        int[] valueStarts = new int[segments.size() + 1];
        for (int i = 0; i < segments.size(); i++) {
            NestedSegment segment = segments.get(i);
            try {
                valueStarts[i + 1] = valueStarts[i] + fillBinary(segment, out, dest, wroteIds, i, valueStarts[i], slotLevel);
            }
            catch (RuntimeException e) {
                throw new SegmentFillException(segment.page, e);
            }
        }
        requireAllSlots(valueStarts[segments.size()], valueCount);
        if (out.dictionary != null) {
            boolean complete = true;
            for (int i = 0; i < segments.size(); i++) {
                if (!wroteIds[i]) {
                    Arrays.fill(out.dictIndices, valueStarts[i], valueStarts[i + 1], -1);
                    complete = false;
                }
            }
            out.idsComplete = complete;
        }
        out.finishFill(valueCount, viewDemand);
        if (viewDemand.requested) {
            out.ensureViews();
        }
    }

    /// Writes one segment's binary values from value `valueStart`: its plain values' bytes to
    /// `dest` and their views, or its entry ids, recording in `wroteIds[index]` that it wrote ids.
    /// A row whose definition level is below `slotLevel` takes no value slot.
    ///
    /// @return the value slots written
    private static int fillBinary(NestedSegment segment, BinaryBatchValues out, PlainBytes dest,
                                  boolean[] wroteIds, int index, int valueStart, int slotLevel) {
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
        int destPos = valueStart;
        while (kept < end) {
            int run = runs == null ? 0 : runs.runOf(kept);
            int n = runs == null ? end - kept : Math.min(end - kept, runs.remaining(run, kept));
            int pagePos = runs == null ? kept : runs.pagePosition(run, kept);
            destPos += ids != null
                    ? values.copyIds(pagePos, n, ids, out.fillStarts(), out.fillEnds(), out.fillBufferIds(), destPos,
                            slotLevel)
                    : values.copyPlain(pagePos, n, out.fillStarts(), out.fillEnds(), out.fillBufferIds(), destPos,
                            dest, slotLevel);
            kept += n;
        }
        return destPos - valueStart;
    }

    /// Writes a segment of a page read in place straight from the page's bytes, through the kept
    /// runs of a masked page, from value `valueStart`. A row whose definition level is below
    /// `slotLevel` takes no value slot.
    ///
    /// @return the value slots written
    private static int copyInPlace(NestedSegment segment, Object out, int valueStart, int slotLevel) {
        PageDecoder.InPlaceNestedPage inPlace = segment.page.inPlace;
        FlatPageValues values = inPlace.flat().view();
        KeptRuns runs = inPlace.runs();
        if (runs == null) {
            return values.copy(segment.valueStart, out, valueStart, segment.values(), slotLevel);
        }
        int kept = segment.valueStart;
        int destPos = valueStart;
        while (kept < segment.valueEnd) {
            int run = runs.runOf(kept);
            int n = Math.min(segment.valueEnd - kept, runs.remaining(run, kept));
            destPos += values.copy(runs.pagePosition(run, kept), out, destPos, n, slotLevel);
            kept += n;
        }
        return destPos - valueStart;
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
