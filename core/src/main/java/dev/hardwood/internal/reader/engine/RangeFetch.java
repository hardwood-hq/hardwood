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
/// R from the fetch's start, the pages that wait for them, and what else waits for them to arrive.
/// [RangeFetcher] fetches it and releases it. Used under the read's lock.
final class RangeFetch {

    private final FileRange range;
    private final Charge charge = new Charge();
    private final List<TrackedPage> waiting = new ArrayList<>();
    /// what waits for the range beyond its pages, run once it has arrived or failed: the scan of
    /// its chunk
    private final List<Runnable> listeners = new ArrayList<>(1);
    private ByteBuffer bytes;
    private boolean arrived;
    private Throwable failure;
    /// its fetch job, once added, so that a scan finding a page in it can key it at the page's row
    private JobScheduler.Job job;

    RangeFetch(FileRange range) {
        this.range = range;
    }

    /// The fetch of `range` of `file`, arrived already where the file holds those bytes in memory.
    static RangeFetch of(InputFile file, FileRange range) {
        RangeFetch r = new RangeFetch(range);
        ByteBuffer held = HeldBytes.slice(file, range.start(), range.length());
        if (held != null) {
            r.arrived(held);
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

    /// One more reader, before the bytes can be let go.
    void addReader() {
        charge.reader();
    }

    /// A reader is done; returns whether it was the last, which lets go of the bytes.
    boolean readerDone() {
        if (charge.readerDone()) {
            bytes = null;
            return true;
        }
        return false;
    }

    /// Its bytes, `null` until they have arrived and once the last reader is done.
    ByteBuffer bytes() {
        return bytes;
    }

    /// Whether its bytes have arrived.
    boolean hasArrived() {
        return arrived;
    }

    /// Its bytes have arrived.
    void arrived(ByteBuffer bytes) {
        this.bytes = bytes;
        this.arrived = true;
    }

    /// Its fetch failed with `failure`.
    void failed(Throwable failure) {
        this.failure = failure;
    }

    /// Its fetch's failure, `null` where none.
    Throwable failure() {
        return failure;
    }

    /// `page` waits for its bytes.
    void await(TrackedPage page) {
        waiting.add(page);
    }

    /// The pages that wait for its bytes.
    List<TrackedPage> waiting() {
        return waiting;
    }

    /// `listener` runs once its bytes have arrived or its fetch failed.
    void onArrival(Runnable listener) {
        listeners.add(listener);
    }

    /// Runs what waits for it beyond its pages.
    void notifyListeners() {
        listeners.forEach(Runnable::run);
    }

    /// Its fetch job, `null` until added.
    JobScheduler.Job job() {
        return job;
    }

    void job(JobScheduler.Job job) {
        this.job = job;
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
            if (r.bytes == null) {
                return null;
            }
            if (start <= offset && r.range.end() >= end) {
                return r.bytes.slice(Math.toIntExact(offset - start), length);
            }
            if (copy == null) {
                copy = ByteBuffer.allocate(length);
            }
            long s = Math.max(offset, start);
            long e = Math.min(end, r.range.end());
            copy.put(Math.toIntExact(s - offset), r.bytes, Math.toIntExact(s - start), Math.toIntExact(e - s));
        }
        return copy;
    }
}
