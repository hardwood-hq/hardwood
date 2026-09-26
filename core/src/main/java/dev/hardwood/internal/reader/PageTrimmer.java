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
/// assembly unmasked. [NestedColumnWorker] trims every masked page on the decode thread,
/// and its assembly then keeps every value it is given. Flat columns are not trimmed:
/// [FlatColumnWorker] copies each kept interval during assembly.
///
/// Keeping masks out of assembly matters for nested columns: a masked page there would put a
/// second, rarely taken shape into the JIT profile of the per-value loop that every other
/// page runs through, and one masked row group at the start of a read measurably slows the
/// unmasked pages assembled after it.
///
/// A page's values, definition levels, repetition levels and dictionary indices are indexed
/// by leaf position, so trimming moves the kept positions to the front of each array, in
/// place, and shortens the page's [Page#size()]. The arrays belong to the page being
/// trimmed: values are allocated per page, and the level arrays are the decode slot's
/// scratch, which is not reused before the page is drained.
///
/// Record `0` of the mask is the record the page's first value starts, so a masked page must
/// begin a record.
final class PageTrimmer {

    private PageTrimmer() {
    }

    /// Returns `page` holding only the values of the records `mask` keeps.
    ///
    /// @throws IllegalArgumentException if `mask` is [PageRowMask#ALL], which keeps the page as is
    /// @throws ParquetReadException if the page's first repetition level is not `0`
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

    /// Compacts a page whose records are the runs of `k` values starting at multiples
    /// of `k`: a fixed-width fixed-size-list page, or with `k == 1` a page of a column
    /// with no repeated ancestor. Returns the number of values kept.
    private static int compactFixedWidth(Page page, PageRowMask mask, int k) {
        int pageRecords = page.size() / k;
        int write = 0;
        for (int i = 0; i < mask.intervalCount(); i++) {
            int start = Math.min(mask.start(i), pageRecords);
            int end = Math.min(mask.end(i), pageRecords);
            if (start < end) {
                move(page, start * k, write, (end - start) * k);
                write += (end - start) * k;
            }
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
        if (runStart >= 0) {
            move(page, runStart, write, size - runStart);
            write += size - runStart;
        }
        return write;
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
            case Page.ByteArrayPage p -> {
                System.arraycopy(p.values(), from, p.values(), to, length);
                if (p.dictIndices() != null) {
                    System.arraycopy(p.dictIndices(), from, p.dictIndices(), to, length);
                }
            }
        }
    }
}
