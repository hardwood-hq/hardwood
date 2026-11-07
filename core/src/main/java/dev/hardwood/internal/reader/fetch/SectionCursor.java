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

/// One walk's view of the [Section]s that overlap its column chunk, in offset
/// order, fixed when the row group was planned. [#read] takes a range's bytes without waiting: a
/// slice of the request holding it, or a copy of the parts of the requests a range
/// straddles, once every one of them is present; otherwise it returns `null` and [#pending] names
/// the first request missing. The walk reads forward, so a request that ends before
/// where it reads next is never read again, and the walk passes it ([#releaseBefore]).
///
/// A column without an offset index under a row limit holds requests for only the first
/// part of its chunk; a read past them inserts the rest, up to the next request or the
/// chunk's end, as a demanded request ([Rest]).
///
/// As a [ByteSource], it serves the parsers that read file structure through one, such as a page
/// header's peek ([dev.hardwood.internal.reader.decode.PageFormatProbe#readHeader]).
public final class SectionCursor implements ByteSource {

    /// Inserts the request for the part of the chunk a row limit left out that a walk has
    /// reached.
    @FunctionalInterface
    public interface Rest {

        /// A request starting at `start`, at most up to `limit`, the next request
        /// or the chunk's end.
        Section insert(long start, long limit);
    }

    private final List<Section> sections;
    private final Rest rest;
    /// The first request the walk has not passed.
    private int held;
    private Section pending;

    /// @param sections the requests overlapping the column chunk, in offset order
    /// @param rest inserts the rest of the chunk, or `null` when the requests cover every
    ///        range the walk reads
    SectionCursor(List<? extends Section> sections, Rest rest) {
        this.sections = new ArrayList<>(sections);
        this.rest = rest;
    }

    /// The request the last [#read] stopped on, `null` when it returned bytes.
    Section pending() {
        return pending;
    }

    /// The `length` bytes at absolute `start`, or `null` when a request holding some of
    /// them is not present, which [#pending] then names.
    ///
    /// @throws IOException if the demand read of a request failed
    @Override
    public ByteBuffer read(long start, int length) throws IOException {
        pending = null;
        long end = start + length;
        int first = covering(start);
        Section section = sections.get(first);
        if (end <= section.end()) {
            ByteBuffer bytes = section.poll();
            if (bytes == null) {
                pending = section;
                return null;
            }
            return section.slice(bytes, start, length);
        }
        // Straddles a cut: copied once every request holding a byte of it is present, so a
        // copy is never left part done
        List<ByteBuffer> parts = new ArrayList<>();
        for (int i = first; ; i = covering(sections.get(i).end())) {
            Section part = sections.get(i);
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
            Section part = sections.get(first + p);
            int take = Math.toIntExact(Math.min(part.end(), end) - position);
            combined.put(part.slice(parts.get(p), position, take));
            position += take;
        }
        return combined.flip();
    }

    /// The index of the request holding absolute offset `position`, inserting the rest of
    /// the chunk where no request holds it.
    private int covering(long position) {
        int i = held;
        while (i < sections.size() && sections.get(i).end() <= position) {
            i++;
        }
        if (i < sections.size() && sections.get(i).offset() <= position) {
            return i;
        }
        if (rest == null) {
            throw new IllegalStateException("No request holds offset " + position);
        }
        long limit = i < sections.size() ? sections.get(i).offset() : Long.MAX_VALUE;
        sections.add(i, rest.insert(position, limit));
        return i;
    }

    /// Passes the requests that end at or before absolute offset `position`, from which on
    /// the walk reads.
    void releaseBefore(long position) {
        while (held < sections.size() && sections.get(held).end() <= position) {
            sections.get(held++).release();
        }
    }

    /// Passes every request the walk holds, once it has ended or stopped.
    void releaseAll() {
        while (held < sections.size()) {
            sections.get(held++).release();
        }
    }

    /// Inserts, as a demanded request, the part of a column chunk ending at `chunkEnd` that
    /// a walk reaches past its requests: from where it reads to the next request or
    /// the chunk's end, at most `maxSpan` bytes.
    public static Rest rest(Sections sections, int workItemIndex, InputFile inputFile, long chunkEnd,
                     String reason, int maxSpan) {
        return (start, limit) -> {
            long end = Math.min(Math.min(limit, chunkEnd), start + maxSpan);
            if (end <= start) {
                throw new IllegalStateException("No byte of the column chunk ending at " + chunkEnd
                        + " is left to read at offset " + start);
            }
            return sections.insertDemanded(workItemIndex, inputFile, start, Math.toIntExact(end - start), reason);
        };
    }
}
