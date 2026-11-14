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
import dev.hardwood.internal.reader.decode.ParsedHeader;
import dev.hardwood.metadata.PageLocation;

/// Fetches a read's ranges ([RangeFetch]), the planner's as well as a batched unit's, each as a
/// job of the read's [JobScheduler], and parses the headers of the pages starting in a range
/// ([RangeFetch#headed]) on a job of the pool once it has arrived, or for a range the file holds
/// in place of its fetch ([#arrive]), before its bytes are handed on;
/// lowers the key of a fetch not yet started where a page in it turns out to serve an earlier row;
/// and releases each range once its last reader is done. Used under the read's lock, except the
/// jobs' work.
final class RangeFetcher {

    private final JobScheduler scheduler;
    /// the job fetching each range, or parsing its pages' headers, until it has ended or is taken out
    private final Map<RangeFetch, JobScheduler.Job<?>> fetching = new IdentityHashMap<>();

    RangeFetcher(JobScheduler scheduler) {
        this.scheduler = scheduler;
    }

    /// Fetches `r` from `file` as a job keyed at `key`, ending its output ([RangeFetch#output]),
    /// which pages, scans and the planner wait on, once the headers of its pages are parsed ([#parseHeaders]);
    /// a fetch that fails returns its charge.
    void fetch(InputFile file, RangeFetch r, long key) {
        JobScheduler.Job<ByteBuffer> job = new JobScheduler.Job<>(key, r.range().length(), JobScheduler.Kind.FETCH,
                r.charge(), buffers -> {
                    try (FetchReason.Scope ignored = FetchReason.set(r.range().reason())) {
                        return ReadRanges.read(file, r.range().start(), r.range().length());
                    }
                }, bytes -> {
                    JobScheduler.Job<?> fetched = fetching.remove(r);
                    if (r.headed().isEmpty()) {
                        r.output().arrived(bytes);
                    }
                    else {
                        parseHeaders(file, r, bytes, fetched.key());
                    }
                }, t -> {
                    fetching.remove(r);
                    r.output().failed(failure(file, r, t));
                });
        fetching.put(r, job);
        scheduler.add(job);
    }

    /// Hands on the bytes of `r`, a range of `file` that holds them ([RangeFetch#held]), once the
    /// headers of its pages are parsed, on a CPU job keyed at `key` ([#parseHeaders]); at once
    /// where no page starts in it.
    void arrive(InputFile file, RangeFetch r, long key) {
        if (r.headed().isEmpty()) {
            r.output().arrived(r.held());
        }
        else {
            parseHeaders(file, r, r.held(), key);
        }
    }

    /// Parses the headers of `r`'s pages from `bytes`, its arrived bytes, and from the file's tail
    /// for a part past its end, on a CPU job keyed at `key`, then ends its output with `bytes`; a
    /// header that does not parse is handed on as the failure its parse met, which its page's
    /// decode fails with.
    private void parseHeaders(InputFile file, RangeFetch r, ByteBuffer bytes, long key) {
        PageLocation[] pages = new PageLocation[r.headed().size()];
        for (int i = 0; i < pages.length; i++) {
            pages[i] = r.headed().get(i).page().location();
        }
        JobScheduler.Job<ParsedHeader[]> job = new JobScheduler.Job<>(key, 0, JobScheduler.Kind.CPU, null,
                buffers -> {
                    ParsedHeader[] headers = new ParsedHeader[pages.length];
                    for (int i = 0; i < pages.length; i++) {
                        ByteBuffer page = RangeFetch.read(file, r.range(), bytes, pages[i].offset(),
                                pages[i].compressedPageSize());
                        if (page == null) {
                            throw new IllegalStateException("The tail holding the end of the page at offset "
                                    + pages[i].offset() + " was dropped before its header was parsed");
                        }
                        headers[i] = ParsedHeader.of(page, pages[i].offset());
                    }
                    return headers;
                }, headers -> {
                    fetching.remove(r);
                    for (int i = 0; i < headers.length; i++) {
                        r.headed().get(i).headerRead(headers[i]);
                    }
                    r.output().arrived(bytes);
                }, t -> {
                    fetching.remove(r);
                    r.output().failed(t);
                });
        fetching.put(r, job);
        scheduler.add(job);
    }

    /// The failure `r`'s fetch from `file` ends its output with, for `t`: an exception as one naming
    /// the range.
    private static Throwable failure(InputFile file, RangeFetch r, Throwable t) {
        return t instanceof Exception e
                ? new IOException(ExceptionContext.filePrefix(file.name()) + "Failed to fetch data at offset "
                        + r.range().start() + " (length " + r.range().length() + ")", e)
                : t;
    }

    /// A reader of each of `ranges` is done; the last of a range's returns its charge, lets go of
    /// its bytes, and takes out its fetch where that has not started: nothing would read what it
    /// fetches.
    void readersDone(List<RangeFetch> ranges) {
        for (RangeFetch r : ranges) {
            if (r.readerDone()) {
                scheduler.release(r.charge());
                JobScheduler.Job<?> job = fetching.get(r);
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
            JobScheduler.Job<?> job = fetching.get(r);
            if (job != null) {
                scheduler.lower(job, key);
            }
        }
    }
}
