/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import dev.hardwood.InputFile;

/// The bytes the planning of a read holds of one file outside its data: a window's page index or
/// Bloom filters, a work item's dictionary pages, Bloom filters without a length and page-format
/// probes. Each range is an entry of the read's [FetchSequence], held by the work items it serves
/// and released once each has released it; a range inside the file's tail while the tail is held,
/// or of a file in memory, is sliced in place and needs none. A window's request inside the tail
/// is sliced as it is inserted and kept: the tail is the reader's, and another read may drop it
/// before this one reads the window.
///
/// A parse reads through it as a [ByteSource]: a range it holds is returned, one it does not is
/// `null`, and the first such range since [#clearMissing] is kept ([#missing]). The planning then
/// demands an entry for that range ([#need]) and parses again once it has arrived. A window
/// inserts its ranges up front ([#insert]) and starts them together ([#start]).
///
/// The planning chain alone inserts and reads; a release may come from any column, once the
/// chain is done with the work items it releases.
final class PlanningReads implements ByteSource {

    private final FetchSequence sequence;
    private final InputFile inputFile;
    private final String reason;
    /// The work items each entry serves, which release it.
    private final int holders;
    private final List<FetchSequence.Entry> entries = new ArrayList<>();
    /// The requests inserted inside the file's tail, sliced from it as they were inserted.
    private final List<Sliced> inTail = new ArrayList<>();
    private Parsed<?> missing;

    private record Sliced(long offset, ByteBuffer bytes) {

        boolean holds(long from, int length) {
            return offset <= from && from + length <= offset + bytes.limit();
        }
    }

    PlanningReads(FetchSequence sequence, InputFile inputFile, String reason, int holders) {
        this.sequence = sequence;
        this.inputFile = inputFile;
        this.reason = reason;
        this.holders = holders;
    }

    @Override
    public ByteBuffer read(long offset, int length) throws IOException {
        if (RangeRead.inMemory(inputFile)) {
            return inputFile.readRange(offset, length);
        }
        for (Sliced sliced : inTail) {
            if (sliced.holds(offset, length)) {
                return sliced.bytes().slice(Math.toIntExact(offset - sliced.offset()), length);
            }
        }
        ByteBuffer tailSlice = TailedInputFile.sliceTail(inputFile, offset, length);
        if (tailSlice != null) {
            return tailSlice;
        }
        FetchSequence.Entry entry = holding(offset, length);
        ByteBuffer bytes = entry != null ? entry.poll() : null;
        if (bytes == null) {
            if (missing == null) {
                missing = Parsed.need(offset, length);
            }
            return null;
        }
        return entry.slice(bytes, offset, length);
    }

    /// The first range a parse read that this did not hold since [#clearMissing], `null` if none.
    Parsed<?> missing() {
        return missing;
    }

    void clearMissing() {
        missing = null;
    }

    /// What the planning waits for before it parses again: the entry holding the range `parsed`
    /// needs, inserted if none does, and demanded.
    Need need(Parsed<?> parsed) {
        FetchSequence.Entry entry = holding(parsed.needOffset(), parsed.needLength());
        if (entry == null) {
            entry = insert(parsed.needOffset(), parsed.needLength());
        }
        entry.issue();
        return entry;
    }

    /// Inserts an entry for each request of `ranges`, which [#start] starts, but for a request
    /// inside the file's tail, which it slices from the tail now, and a file in memory.
    void insert(CoalescedRanges ranges) {
        if (RangeRead.inMemory(inputFile)) {
            return;
        }
        for (int r = 0; r < ranges.requestCount(); r++) {
            long start = ranges.requestStart(r);
            int length = Math.toIntExact(ranges.requestEnd(r) - start);
            ByteBuffer sliced = TailedInputFile.sliceTail(inputFile, start, length);
            if (sliced != null) {
                inTail.add(new Sliced(start, sliced));
            }
            else {
                insert(start, length);
            }
        }
    }

    /// Starts every entry not present, so they are read together, and returns the first of them,
    /// `null` when all are present.
    Need start() throws IOException {
        Need first = null;
        for (FetchSequence.Entry entry : entries) {
            if (entry.poll() == null) {
                entry.issue();
                if (first == null) {
                    first = entry;
                }
            }
        }
        return first;
    }

    /// Whether every entry is present.
    boolean present() throws IOException {
        for (FetchSequence.Entry entry : entries) {
            if (entry.poll() == null) {
                return false;
            }
        }
        return true;
    }

    /// Releases the entries for one of their holders.
    void release() {
        for (FetchSequence.Entry entry : entries) {
            entry.release();
        }
    }

    /// Releases the entries whatever their holders, as the read closes.
    void discard() {
        for (FetchSequence.Entry entry : entries) {
            entry.discard();
        }
    }

    private FetchSequence.Entry insert(long offset, int length) {
        FetchSequence.Entry entry = sequence.plannedEntry(inputFile, offset, length, reason, holders);
        entries.add(entry);
        return entry;
    }

    private FetchSequence.Entry holding(long offset, int length) {
        for (FetchSequence.Entry entry : entries) {
            if (entry.offset() <= offset && offset + length <= entry.end()) {
                return entry;
            }
        }
        return null;
    }
}
