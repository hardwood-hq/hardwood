/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.pipeline;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import dev.hardwood.internal.reader.batch.BitmapWords;
import dev.hardwood.internal.reader.decode.FlatPageValues;
import dev.hardwood.internal.reader.select.PageRowMask;

/// Fills a [Segment] of a flat fixed-width column's cell from its page's values in place
/// ([FlatPageValues]).
///
/// A flat page keeps its mask: the segment's kept records `[recordStart, recordEnd)` are walked
/// through the mask's intervals to the page rows they sit at, in runs, in page order. Each run's
/// values are written to the segment's place in the cell's array, and its validity is recorded in
/// the cell's bitmap.
///
/// Segments of one cell are filled concurrently, each by its own page's task. They write disjoint
/// ranges of the values, and of the validity bits, but a word of the bitmap holds the bits of 64
/// values and two neighbouring segments can share the word at their common edge. A segment
/// therefore writes the words at its edges, where its range does not start or end on a word
/// boundary, with an atomic OR, and every other word plainly: only it touches those.
///
/// Validity is a set bit per present value. The cell's bitmap is installed by the first segment
/// that meets a null ([Cell#installValidity]); a segment without a null writes no bit, and the
/// cell's completion sets the bits of every such segment once the bitmap exists
/// ([#backfillValidity]). A cell that meets no null publishes `null`, the representation of
/// "every value present".
final class FlatFill {

    private static final VarHandle WORDS = MethodHandles.arrayElementVarHandle(long[].class);

    private FlatFill() {
    }

    /// Fills `segment` from its page's `values`, read in place, into the cell's array `out`.
    ///
    /// @param maxDefinitionLevel the column's, below which a value is null
    static void fill(Segment segment, FlatPageValues values, Object out, int maxDefinitionLevel) {
        ValidityWriter validity = new ValidityWriter(segment, maxDefinitionLevel);
        forEachRun(segment, (pageRow, destPos, length) -> {
            values.copy(pageRow, out, destPos, length);
            validity.mark(values.definitionLevels(), pageRow, destPos, length);
        });
    }

    /// Writes a run of a segment's values: page rows `[pageRow, pageRow + length)` to the cell's
    /// values `[destPos, destPos + length)`.
    @FunctionalInterface
    interface Run {
        void write(int pageRow, int destPos, int length);
    }

    /// Walks the segment's kept records `[recordStart, recordEnd)` through its page's mask, in
    /// page order, as runs of consecutive page rows.
    static void forEachRun(Segment segment, Run run) {
        PageRowMask mask = segment.page.info.mask();
        int from = segment.recordStart;
        int to = segment.recordEnd;
        int dest = segment.cellOffset;
        if (mask.isAll()) {
            run.write(from, dest, to - from);
            return;
        }
        int kept = 0;
        for (int i = 0; i < mask.intervalCount() && kept < to; i++) {
            int start = mask.start(i);
            int length = mask.end(i) - start;
            int lo = Math.max(from, kept);
            int hi = Math.min(to, kept + length);
            if (lo < hi) {
                run.write(start + (lo - kept), dest + (lo - from), hi - lo);
            }
            kept += length;
        }
    }

    /// Sets the validity bits of every segment of `cell` that wrote none, as it held no null, once
    /// a segment installed the cell's bitmap. Runs on the task that completes the cell, after every
    /// fill of it.
    static void backfillValidity(Cell cell) {
        long[] validity = cell.validity();
        if (validity == null) {
            return;
        }
        for (Segment segment : cell.segments()) {
            if (!segment.wroteValidity) {
                BitmapWords.setRange(validity, segment.cellOffset, segment.cellOffset + segment.records());
            }
        }
    }

    /// Writes one segment's validity bits, run by run, into its cell's bitmap.
    static final class ValidityWriter {

        private final Segment segment;
        private final int maxDefinitionLevel;
        /// The segment's first and last word, and whether a neighbour may share each.
        private final int firstWord;
        private final int lastWord;
        private final boolean firstShared;
        private final boolean lastShared;
        /// The cell's bitmap, `null` until the segment meets its first null.
        private long[] words;

        ValidityWriter(Segment segment, int maxDefinitionLevel) {
            this.segment = segment;
            this.maxDefinitionLevel = maxDefinitionLevel;
            int start = segment.cellOffset;
            int end = start + segment.records();
            this.firstWord = start >>> 6;
            this.lastWord = (end - 1) >>> 6;
            this.firstShared = (start & 63) != 0;
            this.lastShared = (end & 63) != 0;
        }

        /// Records the validity of a run of values just written. A page without definition levels
        /// has no null, and writes no bit until the segment met one.
        void mark(int[] defLevels, int srcPos, int destPos, int length) {
            if (defLevels == null) {
                if (words != null) {
                    setRange(destPos, destPos + length);
                }
                return;
            }
            for (int i = 0; i < length; i++) {
                int bit = destPos + i;
                if (defLevels[srcPos + i] < maxDefinitionLevel) {
                    if (words == null) {
                        install(bit);
                    }
                }
                else if (words != null) {
                    or(bit >>> 6, 1L << bit);
                }
            }
        }

        /// Takes the cell's bitmap at the segment's first null, `bit`, and sets the bits of the
        /// segment's values before it, all present.
        private void install(int bit) {
            words = segment.cell.installValidity();
            segment.wroteValidity = true;
            setRange(segment.cellOffset, bit);
        }

        private void setRange(int from, int to) {
            if (from >= to) {
                return;
            }
            int first = from >>> 6;
            int last = (to - 1) >>> 6;
            long firstMask = ~0L << from;
            long lastMask = ~0L >>> -to;
            if (first == last) {
                or(first, firstMask & lastMask);
                return;
            }
            or(first, firstMask);
            for (int w = first + 1; w < last; w++) {
                or(w, ~0L);
            }
            or(last, lastMask);
        }

        /// ORs `bits` into word `word`, atomically where a neighbouring segment can share it.
        private void or(int word, long bits) {
            if ((word == firstWord && firstShared) || (word == lastWord && lastShared)) {
                WORDS.getAndBitwiseOr(words, word, bits);
            }
            else {
                words[word] |= bits;
            }
        }
    }
}
