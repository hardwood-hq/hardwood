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
import dev.hardwood.internal.reader.fetch.Section;
import dev.hardwood.internal.reader.fetch.Sections;
import dev.hardwood.internal.reader.fetch.Span;
import dev.hardwood.internal.reader.step.ByteSource;
import dev.hardwood.internal.reader.step.Need;
import dev.hardwood.internal.reader.step.Parsed;

/// The bytes the planning of a read holds of one file outside its data: a window's page index or
/// Bloom filters, a work item's dictionary pages, Bloom filters without a length and page-format
/// probes. Each range is a section of the read ([Sections]), held by the work items it
/// serves and released once each has released it. A range inside the file's tail while the tail is
/// held, or of a file in memory, is a request sliced in place, which needs no read; one
/// inside the tail is sliced as it is inserted and kept, since the tail is the reader's and another
/// read may drop it before this one reads the range.
///
/// A parse reads through it as a [ByteSource]: a range it holds is returned, one it does not is
/// `null`, and the first such range since [#clearMissing] is kept ([#missing]). The planning then
/// demands a request for that range ([#need]) and parses again once it has arrived. A
/// window inserts its ranges up front ([#insert]) and starts them together ([#start]). Each
/// request is read under the label of the work items it serves and its purpose ([#PRUNING],
/// [#INDEXES]), the fetch reason S3 logs and the I/O budget name it by.
///
/// The read's planning alone inserts and reads; a release may come from any column, once the
/// planning is done with the work items it releases.
final class PlanningReads implements ByteSource {

    /// The purpose of Bloom filters and dictionary pages, read to prune row groups.
    static final String PRUNING = "pruning";
    /// The purpose of page indexes and page-format probes.
    static final String INDEXES = "indexes";

    private final Sections readSections;
    private final InputFile inputFile;
    /// The work items the requests serve, such as `rg=3` or `rg=0-4`.
    private final String label;
    /// The work items each request serves, which release it.
    private final int holders;
    private final List<Section> sections = new ArrayList<>();
    private Parsed<?> missing;

    PlanningReads(Sections readSections, InputFile inputFile, String label, int holders) {
        this.readSections = readSections;
        this.inputFile = inputFile;
        this.label = label;
        this.holders = holders;
    }

    @Override
    public ByteBuffer read(long offset, int length) throws IOException {
        Section section = holding(offset, length);
        if (section == null) {
            section = readSections.heldInPlace(inputFile, new Span(offset, length, label, holders));
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

    /// Whether a request holds `[offset, offset + length)`, present or not.
    boolean holds(long offset, int length) {
        return holding(offset, length) != null;
    }

    /// What the planning waits for before it parses again: the request holding the range
    /// `parsed` needs, inserted for `purpose` if none does, and demanded.
    Need need(Parsed<?> parsed, String purpose) {
        Section section = holding(parsed.needOffset(), parsed.needLength());
        if (section == null) {
            section = insert(parsed.needOffset(), parsed.needLength(), purpose);
        }
        section.demand();
        return section;
    }

    /// Inserts a request for `purpose` for each of the coalesced requests of `ranges`, which
    /// [#start] starts.
    void insert(CoalescedRanges ranges, String purpose) {
        for (int r = 0; r < ranges.requestCount(); r++) {
            long start = ranges.requestStart(r);
            insert(start, Math.toIntExact(ranges.requestEnd(r) - start), purpose);
        }
    }

    /// Starts every request not present, so they are read together, and returns the first
    /// of them, `null` when all are present.
    Need start() throws IOException {
        Need first = null;
        for (Section section : sections) {
            if (section.poll() == null) {
                section.demand();
                if (first == null) {
                    first = section;
                }
            }
        }
        return first;
    }

    /// Whether every request is present.
    boolean present() throws IOException {
        for (Section section : sections) {
            if (section.poll() == null) {
                return false;
            }
        }
        return true;
    }

    /// Releases the requests for one of their holders.
    void release() {
        for (Section section : sections) {
            section.release();
        }
    }

    /// Releases the requests whatever their holders, as the read closes.
    void discard() {
        for (Section section : sections) {
            section.discard();
        }
    }

    private Section insert(long offset, int length, String purpose) {
        Section section = readSections.planned(inputFile, offset, length, label + " " + purpose, holders);
        sections.add(section);
        return section;
    }

    private Section holding(long offset, int length) {
        for (Section section : sections) {
            if (section.offset() <= offset && offset + length <= section.end()) {
                return section;
            }
        }
        return null;
    }
}
