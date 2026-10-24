/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.plan;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import dev.hardwood.InputFile;
import dev.hardwood.internal.reader.fetch.CoalescedRanges;
import dev.hardwood.internal.reader.fetch.FetchSequence;
import dev.hardwood.internal.reader.fetch.SectionRequest;
import dev.hardwood.internal.reader.step.ByteSource;
import dev.hardwood.internal.reader.step.Need;
import dev.hardwood.internal.reader.step.Parsed;

/// The bytes the planning of a read holds of one file outside its data: a window's page index or
/// Bloom filters, a work item's dictionary pages, Bloom filters without a length and page-format
/// probes. Each range is a section request of the read's [FetchSequence], held by the work items it
/// serves and released once each has released it. A range inside the file's tail while the tail is
/// held, or of a file in memory, is a section request sliced in place, which needs no read; one
/// inside the tail is sliced as it is inserted and kept, since the tail is the reader's and another
/// read may drop it before this one reads the range.
///
/// A parse reads through it as a [ByteSource]: a range it holds is returned, one it does not is
/// `null`, and the first such range since [#clearMissing] is kept ([#missing]). The planning then
/// demands a section request for that range ([#need]) and parses again once it has arrived. A
/// window inserts its ranges up front ([#insert]) and starts them together ([#start]).
///
/// The planning chain alone inserts and reads; a release may come from any column, once the
/// chain is done with the work items it releases.
final class PlanningReads implements ByteSource {

    private final FetchSequence sequence;
    private final InputFile inputFile;
    private final String reason;
    /// The work items each section request serves, which release it.
    private final int holders;
    private final List<SectionRequest> sections = new ArrayList<>();
    private Parsed<?> missing;

    PlanningReads(FetchSequence sequence, InputFile inputFile, String reason, int holders) {
        this.sequence = sequence;
        this.inputFile = inputFile;
        this.reason = reason;
        this.holders = holders;
    }

    @Override
    public ByteBuffer read(long offset, int length) throws IOException {
        SectionRequest section = holding(offset, length);
        if (section == null) {
            section = FetchSequence.inPlace(FetchSequence.PLANNING, inputFile,
                    new FetchSequence.Span(offset, length, reason, holders));
            if (section != null) {
                sections.add(section);
            }
        }
        ByteBuffer bytes = section != null ? section.poll() : null;
        if (bytes == null) {
            if (missing == null) {
                missing = Parsed.need(offset, length);
            }
            return null;
        }
        return section.slice(bytes, offset, length);
    }

    /// The first range a parse read that this did not hold since [#clearMissing], `null` if none.
    Parsed<?> missing() {
        return missing;
    }

    void clearMissing() {
        missing = null;
    }

    /// What the planning waits for before it parses again: the section request holding the range
    /// `parsed` needs, inserted if none does, and demanded.
    Need need(Parsed<?> parsed) {
        SectionRequest section = holding(parsed.needOffset(), parsed.needLength());
        if (section == null) {
            section = insert(parsed.needOffset(), parsed.needLength());
        }
        section.issue();
        return section;
    }

    /// Inserts a section request for each request of `ranges`, which [#start] starts.
    void insert(CoalescedRanges ranges) {
        for (int r = 0; r < ranges.requestCount(); r++) {
            long start = ranges.requestStart(r);
            insert(start, Math.toIntExact(ranges.requestEnd(r) - start));
        }
    }

    /// Starts every section request not present, so they are read together, and returns the first
    /// of them, `null` when all are present.
    Need start() throws IOException {
        Need first = null;
        for (SectionRequest section : sections) {
            if (section.poll() == null) {
                section.issue();
                if (first == null) {
                    first = section;
                }
            }
        }
        return first;
    }

    /// Whether every section request is present.
    boolean present() throws IOException {
        for (SectionRequest section : sections) {
            if (section.poll() == null) {
                return false;
            }
        }
        return true;
    }

    /// Releases the section requests for one of their holders.
    void release() {
        for (SectionRequest section : sections) {
            section.release();
        }
    }

    /// Releases the section requests whatever their holders, as the read closes.
    void discard() {
        for (SectionRequest section : sections) {
            section.discard();
        }
    }

    private SectionRequest insert(long offset, int length) {
        SectionRequest section = sequence.plannedSection(inputFile, offset, length, reason, holders);
        sections.add(section);
        return section;
    }

    private SectionRequest holding(long offset, int length) {
        for (SectionRequest section : sections) {
            if (section.offset() <= offset && offset + length <= section.end()) {
                return section;
            }
        }
        return null;
    }
}
