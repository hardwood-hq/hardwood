/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.fetch;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import dev.hardwood.InputFile;
import dev.hardwood.internal.reader.step.ByteSource;

/// One walk's view of the [FetchSequence] section requests that overlap its column chunk, in offset
/// order, fixed when the row group was planned. [#read] takes a range's bytes without waiting: a
/// slice of the section request holding it, or a copy of the parts of the section requests a range
/// straddles, once every one of them is present; otherwise it returns `null` and [#pending] names
/// the first section request missing. The walk reads forward, so a section request that ends before
/// where it reads next is never read again, and the walk passes it ([#releaseBefore]).
///
/// A column without an offset index under a row limit holds section requests for only the first
/// part of its chunk; a read past them inserts the rest, up to the next section request or the
/// chunk's end, as a demanded section request ([Rest]).
///
/// As a [ByteSource], it serves the parsers that read file structure through one, such as a page
/// header's peek ([dev.hardwood.internal.reader.decode.PageFormatProbe#readHeader]).
public final class SectionCursor implements ByteSource {

    /// Inserts the section request for the part of the chunk a row limit left out that a walk has
    /// reached.
    @FunctionalInterface
    public interface Rest {

        /// A section request starting at `start`, at most up to `limit`, the next section request
        /// or the chunk's end.
        SectionRequest insert(long start, long limit);
    }

    private final List<SectionRequest> sections;
    private final Rest rest;
    /// The first section request the walk has not passed.
    private int held;
    private SectionRequest pending;

    /// @param sections the section requests overlapping the column chunk, in offset order
    /// @param rest inserts the rest of the chunk, or `null` when the section requests cover every
    ///        range the walk reads
    SectionCursor(List<SectionRequest> sections, Rest rest) {
        this.sections = new ArrayList<>(sections);
        this.rest = rest;
    }

    /// The section request the last [#read] stopped on, `null` when it returned bytes.
    SectionRequest pending() {
        return pending;
    }

    /// The `length` bytes at absolute `start`, or `null` when a section request holding some of
    /// them is not present, which [#pending] then names.
    ///
    /// @throws IOException if the demand read of a section request failed
    @Override
    public ByteBuffer read(long start, int length) throws IOException {
        pending = null;
        long end = start + length;
        int first = covering(start);
        SectionRequest section = sections.get(first);
        if (end <= section.end()) {
            ByteBuffer bytes = section.poll();
            if (bytes == null) {
                pending = section;
                return null;
            }
            return section.slice(bytes, start, length);
        }
        // Straddles a cut: copied once every section request holding a byte of it is present, so a
        // copy is never left part done
        List<ByteBuffer> parts = new ArrayList<>();
        for (int i = first; ; i = covering(sections.get(i).end())) {
            SectionRequest part = sections.get(i);
            ByteBuffer bytes = part.poll();
            if (bytes == null) {
                pending = part;
                return null;
            }
            parts.add(bytes);
            if (part.end() >= end) {
                break;
            }
        }
        // Direct, so that FFM-based decompressors (libdeflate) take it as a native segment
        ByteBuffer combined = ByteBuffer.allocateDirect(length);
        long position = start;
        for (int p = 0; p < parts.size(); p++) {
            SectionRequest part = sections.get(first + p);
            int take = Math.toIntExact(Math.min(part.end(), end) - position);
            combined.put(part.slice(parts.get(p), position, take));
            position += take;
        }
        return combined.flip();
    }

    /// The index of the section request holding absolute offset `position`, inserting the rest of
    /// the chunk where no section request holds it.
    private int covering(long position) {
        int i = held;
        while (i < sections.size() && sections.get(i).end() <= position) {
            i++;
        }
        if (i < sections.size() && sections.get(i).offset() <= position) {
            return i;
        }
        if (rest == null) {
            throw new IllegalStateException("No section request holds offset " + position);
        }
        long limit = i < sections.size() ? sections.get(i).offset() : Long.MAX_VALUE;
        sections.add(i, rest.insert(position, limit));
        return i;
    }

    /// Passes the section requests that end at or before absolute offset `position`, from which on
    /// the walk reads.
    void releaseBefore(long position) {
        while (held < sections.size() && sections.get(held).end() <= position) {
            sections.get(held++).release();
        }
    }

    /// Passes every section request the walk holds, once it has ended.
    void releaseAll() {
        while (held < sections.size()) {
            sections.get(held++).release();
        }
    }

    /// Inserts, as a demanded section request, the part of a column chunk ending at `chunkEnd` that
    /// a walk reaches past its section requests: from where it reads to the next section request or
    /// the chunk's end, at most `maxSpan` bytes.
    public static Rest rest(FetchSequence sequence, int workItemIndex, InputFile inputFile, long chunkEnd,
                     String reason, int maxSpan) {
        return (start, limit) -> {
            long end = Math.min(Math.min(limit, chunkEnd), start + maxSpan);
            if (end <= start) {
                throw new IllegalStateException("No byte of the column chunk ending at " + chunkEnd
                        + " is left to read at offset " + start);
            }
            return sequence.insertDemanded(workItemIndex, inputFile, start, Math.toIntExact(end - start), reason);
        };
    }
}
