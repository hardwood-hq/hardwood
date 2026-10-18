/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import dev.hardwood.reader.ParquetReadException;

/// Trims a page read in place to the records its [PageRowMask] keeps, so the page reaches
/// its measure and fill unmasked. A nested column trims every masked page on its page's task
/// ([ScheduledNestedColumn]), and then keeps every value it is given. Flat columns are not
/// trimmed: their fills copy each kept interval.
///
/// The trimmed page carries the kept values' levels, copied into the page's scratch, and where
/// the kept values lie in the page ([KeptRuns]); its values stay where they are, for the fill to
/// read through the runs.
///
/// Record `0` of the mask is the record the page's first value starts, so a masked page must
/// begin a record.
final class PageTrimmer {

    private PageTrimmer() {
    }

    /// Returns the view of a page read in place that holds only the values of the records `mask`
    /// keeps: their levels, copied into `scratch`'s kept-level arrays, and where they lie in the
    /// page ([KeptRuns]). The page's own levels and values stay as they are, for its fill.
    ///
    /// @throws IllegalArgumentException if `mask` is [PageRowMask#ALL]
    /// @throws ParquetReadException if the page's first repetition level is not `0`, or the
    ///         mask selects records past the last one the page holds
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
}
