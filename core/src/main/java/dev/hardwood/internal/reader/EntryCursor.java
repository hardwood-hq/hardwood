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

/// One walk's view of the [FetchSequence] entries that overlap its column chunk, in offset order,
/// fixed when the row group was planned. [#read] takes a range's bytes without waiting: a slice of
/// the entry holding it, or a copy of the parts of the entries a range straddles, once every one
/// of them is present; otherwise it returns `null` and [#pending] names the first entry missing.
/// The walk reads forward, so an entry that ends before where it reads next is never read again,
/// and the walk passes it ([#releaseBefore]).
///
/// A column without an offset index under a row limit holds entries for only the first part of its
/// chunk; a read past them inserts the rest, up to the next entry or the chunk's end, as a demanded
/// entry ([Rest]).
final class EntryCursor {

    /// Inserts the entry for the part of the chunk a row limit left out that a walk has reached.
    @FunctionalInterface
    interface Rest {

        /// An entry starting at `start`, at most up to `limit`, the next entry or the chunk's end.
        FetchSequence.Entry insert(long start, long limit);
    }

    private final List<FetchSequence.Entry> entries;
    private final Rest rest;
    /// The first entry the walk has not passed.
    private int held;
    private FetchSequence.Entry pending;

    /// @param entries the entries overlapping the column chunk, in offset order
    /// @param rest inserts the rest of the chunk, or `null` when the entries cover every range the
    ///        walk reads
    EntryCursor(List<FetchSequence.Entry> entries, Rest rest) {
        this.entries = new ArrayList<>(entries);
        this.rest = rest;
    }

    /// The entry the last [#read] stopped on, `null` when it returned bytes.
    FetchSequence.Entry pending() {
        return pending;
    }

    /// The `length` bytes at absolute `start`, or `null` when an entry holding some of them is not
    /// present, which [#pending] then names.
    ///
    /// @throws IOException if the demand read of an entry failed
    ByteBuffer read(long start, int length) throws IOException {
        pending = null;
        long end = start + length;
        int first = covering(start);
        FetchSequence.Entry entry = entries.get(first);
        if (end <= entry.end()) {
            ByteBuffer bytes = entry.poll();
            if (bytes == null) {
                pending = entry;
                return null;
            }
            return entry.slice(bytes, start, length);
        }
        // Straddles a cut: copied once every entry holding a byte of it is present, so a copy is
        // never left part done
        List<ByteBuffer> parts = new ArrayList<>();
        for (int i = first; ; i = covering(entries.get(i).end())) {
            FetchSequence.Entry part = entries.get(i);
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
            FetchSequence.Entry part = entries.get(first + p);
            int take = Math.toIntExact(Math.min(part.end(), end) - position);
            combined.put(part.slice(parts.get(p), position, take));
            position += take;
        }
        return combined.flip();
    }

    /// The index of the entry holding absolute offset `position`, inserting the rest of the chunk
    /// where no entry holds it.
    private int covering(long position) {
        int i = held;
        while (i < entries.size() && entries.get(i).end() <= position) {
            i++;
        }
        if (i < entries.size() && entries.get(i).offset() <= position) {
            return i;
        }
        if (rest == null) {
            throw new IllegalStateException("No entry holds offset " + position);
        }
        long limit = i < entries.size() ? entries.get(i).offset() : Long.MAX_VALUE;
        entries.add(i, rest.insert(position, limit));
        return i;
    }

    /// Passes the entries that end at or before absolute offset `position`, from which on the walk
    /// reads.
    void releaseBefore(long position) {
        while (held < entries.size() && entries.get(held).end() <= position) {
            entries.get(held++).release();
        }
    }

    /// Passes every entry the walk holds, once it has ended.
    void releaseAll() {
        while (held < entries.size()) {
            entries.get(held++).release();
        }
    }
}
