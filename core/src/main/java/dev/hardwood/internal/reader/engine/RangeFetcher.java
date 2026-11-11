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
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import dev.hardwood.InputFile;
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.FetchReason;
import dev.hardwood.internal.io.ReadRanges;

/// Fetches a read's ranges ([RangeFetch]), the planner's as well as a batched unit's, each as a
/// job of the read's [JobScheduler]; lowers the key of a fetch not yet started where a page in it
/// turns out to serve an earlier row; and releases each range once its last reader is done. Used
/// under the read's lock, except the fetches' work.
final class RangeFetcher {

    private final JobScheduler scheduler;
    /// the job fetching each range, until it has ended or is taken out
    private final Map<RangeFetch, JobScheduler.Job<ByteBuffer>> fetching = new IdentityHashMap<>();

    RangeFetcher(JobScheduler scheduler) {
        this.scheduler = scheduler;
    }

    /// Fetches `r` from `file` as a job keyed at `key`, ending its output ([RangeFetch#output]),
    /// which pages, scans and the planner wait on; a fetch that fails returns its charge.
    void fetch(InputFile file, RangeFetch r, long key) {
        JobScheduler.Job<ByteBuffer> job = new JobScheduler.Job<>(key, r.range().length(), JobScheduler.Kind.FETCH,
                r.charge(), buffers -> {
                    try (FetchReason.Scope ignored = FetchReason.set(r.range().reason())) {
                        return ReadRanges.read(file, r.range().start(), r.range().length());
                    }
                }, bytes -> {
                    fetching.remove(r);
                    r.output().arrived(bytes);
                }, t -> {
                    fetching.remove(r);
                    r.output().failed(t instanceof Exception e
                            ? new IOException(ExceptionContext.filePrefix(file.name()) + "Failed to fetch data at offset "
                                    + r.range().start() + " (length " + r.range().length() + ")", e)
                            : t);
                });
        fetching.put(r, job);
        scheduler.add(job);
    }

    /// A reader of each of `ranges` is done; the last of a range's returns its charge, lets go of
    /// its bytes, and takes out its fetch where that has not started: nothing would read what it
    /// fetches.
    void readersDone(List<RangeFetch> ranges) {
        for (RangeFetch r : ranges) {
            if (r.readerDone()) {
                scheduler.release(r.charge());
                JobScheduler.Job<ByteBuffer> job = fetching.get(r);
                if (job != null && scheduler.remove(job)) {
                    fetching.remove(r);
                }
            }
        }
    }

    /// Keys the fetches of `ranges` not started yet at `key` where that is lower: a scan found a
    /// page serving row `key` in them, whose bytes the consumer's need must reach.
    void keyAt(List<RangeFetch> ranges, long key) {
        for (RangeFetch r : ranges) {
            JobScheduler.Job<ByteBuffer> job = fetching.get(r);
            if (job != null) {
                scheduler.lower(job, key);
            }
        }
    }
}
