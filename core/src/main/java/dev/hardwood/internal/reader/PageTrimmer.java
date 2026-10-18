/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import dev.hardwood.reader.ParquetReadException;

/// Trims a decoded page to the records its [PageRowMask] keeps, so the page reaches
/// assembly unmasked. A nested column trims every masked page on its page's task
/// ([ScheduledNestedColumn]), and then keeps every value it is given. Flat columns are not
/// trimmed: their fills copy each kept interval.
///
/// Keeping masks out of assembly matters for nested columns: a masked page there would put a
/// second, rarely taken shape into the JIT profile of the per-value loop that every other
/// page runs through, and one masked row group at the start of a read measurably slows the
/// unmasked pages assembled after it.
///
/// A page's values, definition levels, repetition levels and dictionary indices are indexed
/// by leaf position, so trimming moves the kept positions to the front of each array, in
/// place, and shortens the page's [Page#size()]. The arrays belong to the page being
/// trimmed: values are allocated per page, and the level arrays are the scratch the page was
/// decoded with.
///
/// Record `0` of the mask is the record the page's first value starts, so a masked page must
/// begin a record.
final class PageTrimmer {

    private PageTrimmer() {
    }

    /// Returns `page` holding only the values of the records `mask` keeps.
    ///
    /// @throws IllegalArgumentException if `mask` is [PageRowMask#ALL], which keeps the page as is
    /// @throws ParquetReadException if the page's first repetition level is not `0`, or the
    ///         mask selects records past the last one the page holds
    static Page trim(Page page, PageRowMask mask) {
        if (mask.isAll()) {
            throw new IllegalArgumentException("PageRowMask.ALL keeps the page as is; nothing to trim");
        }
        int kept;
        if (page.fixedListK() > 0) {
            kept = compactFixedWidth(page, mask, page.fixedListK());
        }
        else if (page.repetitionLevels() == null) {
            // A column with no repeated ancestor: every value is a record of its own.
            kept = compactFixedWidth(page, mask, 1);
        }
        else {
            kept = compactRegular(page, mask);
        }
        return Page.withSize(page, kept);
    }

    /// Returns the view of a page read in place that holds only the values of the records `mask`
    /// keeps: their levels, copied into `scratch`'s kept-level arrays, and where they lie in the
    /// page ([KeptRuns]). The page's own levels and values stay as they are, for its fill.
    ///
    /// @throws IllegalArgumentException if `mask` is [PageRowMask#ALL]
    /// @throws ParquetReadException as [#trim(Page, PageRowMask)] does
    static PageDecoder.InPlaceNestedPage trim(PageDecoder.InPlaceNestedPage page, PageRowMask mask,
                                              PageDecoder.LevelScratch scratch) {
        if (mask.isAll()) {
            throw new IllegalArgumentException("PageRowMask.ALL keeps the page as is; nothing to trim");
        }
        int[] starts = new int[mask.intervalCount()];
        int[] lengths = new int[mask.intervalCount()];
        int count;
        int k = page.fixedListK() > 0 ? page.fixedListK() : page.repetitionLevels() == null ? 1 : 0;
        if (k > 0) {
            checkMaskFits(mask, page.size() / k);
            for (int i = 0; i < mask.intervalCount(); i++) {
                starts[i] = mask.start(i) * k;
                lengths[i] = (mask.end(i) - mask.start(i)) * k;
            }
            count = mask.intervalCount();
        }
        else {
            count = regularRuns(page.size(), page.repetitionLevels(), mask, starts, lengths);
        }
        KeptRuns runs = KeptRuns.of(starts, lengths, count);
        int kept = 0;
        for (int i = 0; i < count; i++) {
            kept += lengths[i];
        }
        int[] definitionLevels = keep(page.definitionLevels(), starts, lengths, count, kept,
                page.definitionLevels() == null ? null : scratch.keptDefinitionLevels(kept));
        int[] repetitionLevels = keep(page.repetitionLevels(), starts, lengths, count, kept,
                page.repetitionLevels() == null ? null : scratch.keptRepetitionLevels(kept));
        return new PageDecoder.InPlaceNestedPage(kept, definitionLevels, repetitionLevels, page.fixedListK(),
                page.flat(), page.binary(), runs);
    }

    /// Copies the levels of the kept runs into `dest`; `null` for a page without such levels.
    private static int[] keep(int[] levels, int[] starts, int[] lengths, int count, int kept, int[] dest) {
        if (levels == null) {
            return null;
        }
        int write = 0;
        for (int i = 0; i < count; i++) {
            System.arraycopy(levels, starts[i], dest, write, lengths[i]);
            write += lengths[i];
        }
        return dest;
    }

    /// The runs of the values of the records `mask` keeps of a page whose record boundaries are its
    /// repetition levels of `0`, into `starts` and `lengths`; returns how many. Adjacent kept
    /// records make one run, so there are at most as many runs as the mask has intervals.
    private static int regularRuns(int size, int[] repetitionLevels, PageRowMask mask, int[] starts,
                                   int[] lengths) {
        if (size > 0 && repetitionLevels[0] != 0) {
            throw new ParquetReadException(
                    "Invalid column chunk: first repetition level must be 0 but was "
                    + repetitionLevels[0]);
        }
        int intervalCount = mask.intervalCount();
        int intervalCursor = 0;
        int recordIndex = -1;
        int count = 0;
        int runStart = -1;
        for (int i = 0; i < size; i++) {
            if (repetitionLevels[i] == 0) {
                recordIndex++;
                while (intervalCursor < intervalCount && recordIndex >= mask.end(intervalCursor)) {
                    intervalCursor++;
                }
            }
            boolean keep = intervalCursor < intervalCount && recordIndex >= mask.start(intervalCursor);
            if (keep && runStart < 0) {
                runStart = i;
            }
            else if (!keep && runStart >= 0) {
                starts[count] = runStart;
                lengths[count++] = i - runStart;
                runStart = -1;
            }
        }
        checkMaskFits(mask, recordIndex + 1);
        if (runStart >= 0) {
            starts[count] = runStart;
            lengths[count++] = size - runStart;
        }
        return count;
    }

    /// Compacts a page whose records are the runs of `k` values starting at multiples
    /// of `k`: a fixed-width fixed-size-list page, or with `k == 1` a page of a column
    /// with no repeated ancestor. Returns the number of values kept.
    private static int compactFixedWidth(Page page, PageRowMask mask, int k) {
        checkMaskFits(mask, page.size() / k);
        int write = 0;
        for (int i = 0; i < mask.intervalCount(); i++) {
            int start = mask.start(i);
            int end = mask.end(i);
            move(page, start * k, write, (end - start) * k);
            write += (end - start) * k;
        }
        return write;
    }

    /// Compacts a page whose record boundaries are its repetition levels of `0`.
    /// Returns the number of values kept.
    private static int compactRegular(Page page, PageRowMask mask) {
        int size = page.size();
        int[] repetitionLevels = page.repetitionLevels();
        if (size > 0 && repetitionLevels[0] != 0) {
            throw new ParquetReadException(
                    "Invalid column chunk: first repetition level must be 0 but was "
                    + repetitionLevels[0]);
        }
        int intervalCount = mask.intervalCount();
        int intervalCursor = 0;
        int recordIndex = -1;
        int write = 0;
        int runStart = -1;
        for (int i = 0; i < size; i++) {
            if (repetitionLevels[i] == 0) {
                recordIndex++;
                while (intervalCursor < intervalCount && recordIndex >= mask.end(intervalCursor)) {
                    intervalCursor++;
                }
            }
            boolean keep = intervalCursor < intervalCount && recordIndex >= mask.start(intervalCursor);
            if (keep && runStart < 0) {
                runStart = i;
            }
            else if (!keep && runStart >= 0) {
                move(page, runStart, write, i - runStart);
                write += i - runStart;
                runStart = -1;
            }
        }
        checkMaskFits(mask, recordIndex + 1);
        if (runStart >= 0) {
            move(page, runStart, write, size - runStart);
            write += size - runStart;
        }
        return write;
    }

    /// Fails unless every record `mask` selects lies within the page's `pageRecords`
    /// records. A mask is derived from the page's row count in the offset index or its
    /// header, so a mask reaching past the page means those disagree with the page's
    /// values, and trimming to fewer records would misalign the column with its siblings.
    private static void checkMaskFits(PageRowMask mask, int pageRecords) {
        int maskEnd = mask.end(mask.intervalCount() - 1);
        if (maskEnd > pageRecords) {
            throw new ParquetReadException("Invalid column chunk: page row mask selects records up to "
                    + maskEnd + " but the page holds " + pageRecords + " records");
        }
    }

    /// Moves the `length` positions starting at `from` to start at `to`, in every array
    /// indexed by position. `to` never exceeds `from`, and [System#arraycopy] handles the
    /// overlap of a move to the left.
    private static void move(Page page, int from, int to, int length) {
        if (from == to) {
            return;
        }
        if (page.definitionLevels() != null) {
            System.arraycopy(page.definitionLevels(), from, page.definitionLevels(), to, length);
        }
        if (page.repetitionLevels() != null) {
            System.arraycopy(page.repetitionLevels(), from, page.repetitionLevels(), to, length);
        }
        switch (page) {
            case Page.BooleanPage p -> System.arraycopy(p.values(), from, p.values(), to, length);
            case Page.IntPage p -> System.arraycopy(p.values(), from, p.values(), to, length);
            case Page.LongPage p -> System.arraycopy(p.values(), from, p.values(), to, length);
            case Page.FloatPage p -> System.arraycopy(p.values(), from, p.values(), to, length);
            case Page.DoublePage p -> System.arraycopy(p.values(), from, p.values(), to, length);
            case Page.ByteArrayPage p -> System.arraycopy(p.values(), from, p.values(), to, length);
            case Page.DictionaryByteArrayPage p ->
                    System.arraycopy(p.dictIndices(), from, p.dictIndices(), to, length);
        }
    }
}
