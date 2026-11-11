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

import dev.hardwood.internal.reader.fetch.FileRange;

/// The fetch of one range of a file, for the planner before a unit is batched or for a
/// batched unit's pages: the bytes once arrived, held until the last reader is done, their charge against
/// R from the fetch's start, the pages that wait for them, and what else waits for them to arrive.
/// [RangeFetcher] fetches it and releases it. Used under the read's lock.
final class RangeFetch {

    final FileRange range;
    final Charge charge = new Charge();
    final List<TrackedPage> waiting = new ArrayList<>();
    ByteBuffer bytes;
    boolean arrived;
    Throwable failure;
    /// what waits for the range beyond its pages, run once it has arrived or failed: the scan of
    /// its chunk
    final List<Runnable> listeners = new ArrayList<>(1);
    /// its fetch job, once added, so that a scan finding a page in it can key it at the page's row
    JobScheduler.Job job;

    RangeFetch(FileRange range) {
        this.range = range;
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
