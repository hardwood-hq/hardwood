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
/// segments' measures, copies each segment from its decoded page, and computes the batch's index
/// ([NestedIndex]).
///
/// Each segment's values are one contiguous run of its page, which was trimmed to its kept
/// records. A segment writes:
///
/// - values: one `System.arraycopy` for primitive values; for binary values each value's bytes at
///   the segment's place in the cell's plain bytes, or its dictionary entry id;
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
                            NestedLevelComputer.Layers layers, NestedColumnWorker.IndexMode mode,
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
        boolean keepLevels = mode != NestedColumnWorker.IndexMode.REAL_VIEW;

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
                copyValues(segment.page.page, segment.valueStart, out, segment.cellValueOffset, segment.values());
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
            // A decoded page's values (arrays or row ids) are read by position, with no cursor, so
            // two cells sharing the page may be filled from it at once
            BinaryPageValues values = segment.page.measure.binary();
            Dictionary.ByteArrayDictionary dictionary = values.dictionary();
            if (dictionary != null) {
                int[] ids = out.installDictionary(dictionary);
                if (!values.allPresent()) {
                    out.installViews();
                }
                values.copyIds(segment.valueStart, segment.values(), ids, out.fillStarts(), out.fillEnds(),
                        segment.cellValueOffset);
                wroteIds[i] = true;
            }
            else {
                out.installViews();
                destByte = values.copyPlain(segment.valueStart, segment.values(), out.fillStarts(), out.fillEnds(),
                        segment.cellValueOffset, out.fillBytes(), destByte);
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

    private static void copyValues(Page page, int srcPos, Object values, int destPos, int length) {
        switch (page) {
            case Page.IntPage p -> System.arraycopy(p.values(), srcPos, (int[]) values, destPos, length);
            case Page.LongPage p -> System.arraycopy(p.values(), srcPos, (long[]) values, destPos, length);
            case Page.FloatPage p -> System.arraycopy(p.values(), srcPos, (float[]) values, destPos, length);
            case Page.DoublePage p -> System.arraycopy(p.values(), srcPos, (double[]) values, destPos, length);
            case Page.BooleanPage p -> System.arraycopy(p.values(), srcPos, (boolean[]) values, destPos, length);
            case Page.BinaryPage p -> throw new IllegalStateException("Binary values are filled by fillBinary");
        }
    }

    /// Writes the segment's levels into the arrays given, each `null` when it is not written.
    private static void copyLevels(NestedSegment segment, int[] definitionLevels, int[] repetitionLevels,
                                   int maxDefinitionLevel) {
        Page page = segment.page.page;
        int from = segment.valueStart;
        int dest = segment.cellValueOffset;
        int length = segment.values();
        int k = page.fixedListK();
        if (definitionLevels != null) {
            int[] pageDefinitionLevels = page.definitionLevels();
            if (k == 0 && pageDefinitionLevels != null) {
                System.arraycopy(pageDefinitionLevels, from, definitionLevels, dest, length);
            }
            else {
                Arrays.fill(definitionLevels, dest, dest + length, maxDefinitionLevel);
            }
        }
        if (repetitionLevels != null) {
            int[] pageRepetitionLevels = page.repetitionLevels();
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
