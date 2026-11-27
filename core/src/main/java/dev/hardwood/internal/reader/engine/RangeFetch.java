/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import dev.hardwood.InputFile;
import dev.hardwood.internal.io.HeldBytes;
import dev.hardwood.internal.io.ReadRanges;
import dev.hardwood.internal.reader.fetch.FileRange;

/// The fetch of one range of a file, for the planner before a unit is batched or for a batched
/// unit's pages: the bytes once arrived, held until the last reader is done, their charge against
/// R from the fetch's start; its bytes are its fetch's [Output], which pages, scans and the planner
/// wait on. [RangeFetcher] fetches it and releases it. Used under the read's lock.
final class RangeFetch {

    private final FileRange range;
    private final Charge charge = new Charge();
    private final Output<ByteBuffer> bytes = new Output<>();

    RangeFetch(FileRange range) {
        this.range = range;
    }

    /// The fetch of `range` of `file`, arrived already where the file holds those bytes in memory.
    static RangeFetch of(InputFile file, FileRange range) {
        RangeFetch r = new RangeFetch(range);
        ByteBuffer held = held(file, range);
        if (held != null) {
            r.bytes.arrived(held);
        }
        return r;
    }

    /// `range` of `file` where the file holds it: a slice of what it holds, or, for a file that
    /// holds every byte without a slice to give (a local file mapped region by region), the range
    /// read in place, which maps it; `null` otherwise. A read in place that fails leaves the range
    /// to its fetch, which reads it again and fails the read with the file's context.
    private static ByteBuffer held(InputFile file, FileRange range) {
        ByteBuffer slice = HeldBytes.slice(file, range.start(), range.length());
        if (slice != null || !HeldBytes.inMemory(file)) {
            return slice;
        }
        try {
            return ReadRanges.read(file, range.start(), range.length());
        }
        catch (IOException e) {
            return null;
        }
    }

    FileRange range() {
        return range;
    }

    /// Its charge, which the scheduler charges as its fetch starts and takes back with its last
    /// reader.
    Charge charge() {
        return charge;
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

    /// `[offset, offset + length)` from those of `ranges` that hold part of it: a slice where one
    /// holds it, a copy where it straddles several; `null` where a range holding part of it has
    /// not arrived, or the ranges leave part of it uncovered. The ranges may come in any order
    /// and overlap, as the ranges of structures a footer lays over each other do.
    static ByteBuffer read(List<RangeFetch> ranges, long offset, int length) {
        long end = offset + length;
        List<RangeFetch> parts = null;
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
            if (parts == null) {
                parts = new ArrayList<>(2);
            }
            parts.add(r);
        }
        return parts == null ? null : copy(parts, offset, length);
    }

    /// `[offset, offset + length)` copied from `parts`, each of which holds part of it; `null`
    /// where they leave part of it uncovered, found before anything is allocated.
    private static ByteBuffer copy(List<RangeFetch> parts, long offset, int length) {
        parts.sort(Comparator.comparingLong(r -> r.range.start()));
        long end = offset + length;
        long covered = offset;
        for (RangeFetch r : parts) {
            if (r.range.start() > covered) {
                return null;
            }
            covered = Math.max(covered, Math.min(end, r.range.end()));
        }
        if (covered < end) {
            return null;
        }
        ByteBuffer copy = ByteBuffer.allocate(length);
        covered = offset;
        for (RangeFetch r : parts) {
            long e = Math.min(end, r.range.end());
            if (e > covered) {
                long start = r.range.start();
                copy.put(Math.toIntExact(covered - offset), r.bytes.value(), Math.toIntExact(covered - start),
                        Math.toIntExact(e - covered));
                covered = e;
            }
        }
        return copy;
    }
}
