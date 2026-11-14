/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import dev.hardwood.InputFile;
import dev.hardwood.internal.io.HeldBytes;
import dev.hardwood.internal.reader.fetch.FileRange;

/// The fetch of one range of a file, for the planner before a unit is batched or for a batched
/// unit's pages: the bytes once arrived, held until the last reader is done, their charge against
/// R from the fetch's start, and the pages starting in it whose headers are parsed once it has
/// arrived, before its bytes are handed on; its bytes
/// are its fetch's [Output], which pages, scans and the planner wait on. [RangeFetcher] fetches it
/// and releases it. Used under the read's lock.
final class RangeFetch {

    private final FileRange range;
    private final Charge charge = new Charge();
    private final Output<ByteBuffer> bytes = new Output<>();
    /// the pages whose headers are parsed once it has arrived, before its bytes are handed on
    private final List<TrackedPage> headed = new ArrayList<>(0);

    RangeFetch(FileRange range) {
        this.range = range;
    }

    /// The fetch of `range` of `file`, arrived already where the file holds those bytes in memory.
    static RangeFetch of(InputFile file, FileRange range) {
        RangeFetch r = new RangeFetch(range);
        ByteBuffer held = HeldBytes.slice(file, range.start(), range.length());
        if (held != null) {
            r.bytes.arrived(held);
        }
        return r;
    }

    FileRange range() {
        return range;
    }

    /// Its charge, which the scheduler charges as its fetch starts and takes back with its last
    /// reader.
    Charge charge() {
        return charge;
    }

    /// The header of `page`, which starts in it and whose bytes past it the file holds, is parsed
    /// once it has arrived; added before its fetch starts.
    void parsesHeaderOf(TrackedPage page) {
        headed.add(page);
    }

    /// The pages whose headers are parsed once it has arrived.
    List<TrackedPage> headed() {
        return headed;
    }

    /// One more reader, before the bytes can be let go.
    void addReader() {
        charge.reader();
    }

    /// A reader is done; returns whether it was the last, which lets go of the bytes.
    boolean readerDone() {
        if (charge.readerDone()) {
            bytes.drop();
            return true;
        }
        return false;
    }

    /// Its fetch's output: its bytes, or the fetch's failure.
    Output<ByteBuffer> output() {
        return bytes;
    }

    /// The first of `ranges` whose bytes have not arrived, failed ones included; `null` where all
    /// have.
    static RangeFetch firstMissing(List<RangeFetch> ranges) {
        for (RangeFetch r : ranges) {
            if (!r.bytes.hasValue()) {
                return r;
            }
        }
        return null;
    }

    /// `[offset, offset + length)` from `bytes`, the arrived bytes of `range`, in which it starts,
    /// and for a part past its end from what `file` holds: a slice where `range` holds it all, a
    /// copy otherwise; `null` where the file does not hold that part. Read outside the read's lock.
    static ByteBuffer read(InputFile file, FileRange range, ByteBuffer bytes, long offset, int length) {
        int into = Math.toIntExact(offset - range.start());
        long end = offset + length;
        if (end <= range.end()) {
            return bytes.slice(into, length);
        }
        int before = Math.toIntExact(range.end() - offset);
        ByteBuffer rest = HeldBytes.slice(file, range.end(), length - before);
        if (rest == null) {
            return null;
        }
        ByteBuffer copy = ByteBuffer.allocate(length);
        copy.put(0, bytes, into, before);
        copy.put(before, rest, 0, length - before);
        return copy;
    }

    /// `[offset, offset + length)` from those of `ranges` that hold part of it: a slice where one
    /// holds it, a copy where it straddles two; `null` where a range holding part of it has not
    /// arrived, or `ranges` do not reach its end.
    static ByteBuffer read(List<RangeFetch> ranges, long offset, int length) {
        long end = offset + length;
        if (ranges.isEmpty() || ranges.getLast().range.end() < end) {
            return null;
        }
        ByteBuffer copy = null;
        for (RangeFetch r : ranges) {
            long start = r.range.start();
            if (start >= end || r.range.end() <= offset) {
                continue;
            }
            if (r.bytes.value() == null) {
                return null;
            }
            if (start <= offset && r.range.end() >= end) {
                return r.bytes.value().slice(Math.toIntExact(offset - start), length);
            }
            if (copy == null) {
                copy = ByteBuffer.allocate(length);
            }
            long s = Math.max(offset, start);
            long e = Math.min(end, r.range.end());
            copy.put(Math.toIntExact(s - offset), r.bytes.value(), Math.toIntExact(s - start), Math.toIntExact(e - s));
        }
        return copy;
    }
}
