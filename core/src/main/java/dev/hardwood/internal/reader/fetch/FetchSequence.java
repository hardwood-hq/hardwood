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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.locks.ReentrantLock;

import dev.hardwood.InputFile;
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.io.HeldBytes;
import dev.hardwood.internal.reader.memory.InternalProperty;
import dev.hardwood.internal.reader.memory.ReadAheadBuffer;

/// Schedules the requests of one read: when each starts, what it is charged to while it is held,
/// and when it is released. Most are section requests, requests for a row group's data: a range of
/// the file its columns' needed ranges were coalesced into. Planning a row group inserts its
/// section requests ([#insert]); the columns' walks take a page's bytes as a slice of a completed
/// section request ([SectionRequest#poll]), through an index fixed when the row group was planned.
/// Each section request's read runs on a virtual thread of the context's I/O executor
/// ([RequestAttempt]). Other parts of the read queue requests of their own ([#queue]), such as the
/// footers of files it has not reached, and release them ([#discard]).
///
/// The sequence keeps its requests in position order, the order the read consumes them: by file,
/// then by work item, then by offset, a file's other requests before its section requests. A
/// request the read cannot go on without starts at once and is charged to nothing. A request
/// started ahead of need is charged: its bytes count against a limit from its start until its
/// release, and it starts only while they fit. There are two limits: F, the bytes of speculative
/// section requests one read holds, and R, the context's [ReadAheadBuffer], which its reads share.
/// Nothing waits for room under a limit, so no read waits for a charge that only its own progress
/// releases.
///
/// | Request | Starts | Charged to | Released |
/// |---|---|---|---|
/// | A section request a step yields on ([#demand]), or one a walk finds it needs while it parses ([#insertDemanded]) | at once | nothing | once every column holding it has passed it ([#release]) |
/// | The first section request of the next unit, the work item after the one the first column has entered ([#nextUnit]) | at once | nothing | as above |
/// | Every other section request, under no row limit | in position order, while it fits F and R | F and R | as above |
/// | A section request planning reads: a window's request, a dictionary page, a probe ([#plannedSection]) | when planning demands it | nothing | once each of its holders has released it |
/// | A section request of a file in memory, or inside a file's tail the read holds ([#inPlace]) | never: sliced in place | nothing | not held |
/// | A request another part of the read queues ([#queue]) | in position order, while it fits R; at once when started now ([#startNow]) | R; nothing when started now | when its owner discards it ([#discard]) |
///
/// The pump starts what may start now: from the front of the queue every request that fits,
/// stopping at the first that does not. One that does not fit F starts once a section request's
/// release returns its bytes; one that does not fit R starts once any read releases R, which runs
/// the pump again ([#onRelease]). The pump runs after every event that can make room or add work:
/// requests queued, a request released or dropped from the queue, R released.
///
/// A page slice taken from a section request keeps its bytes alive past its release. A speculative
/// section request that fails is dropped and started again when demanded. [#close] drops the
/// requests not started and releases the charges of the section requests; the reads in flight are
/// the read's [PrefetchTasks], which its owner waits for.
///
/// One lock guards the requests and their positions. It is held across no I/O, no waiter and no
/// completion of a read: reads are submitted, waiters registered and charges released after it is
/// let go. A walk polls a section request without it.
public final class FetchSequence {

    public static final String FETCH_AHEAD_BYTES_PROPERTY = "hardwood.internal.fetchAheadBytes";

    /// The work item of a section request planning reads ([#plannedSection]), which no work item
    /// holds.
    public static final int PLANNING = -1;

    private final PrefetchTasks tasks;
    private final ReadAheadBuffer readAhead;
    /// F: the most bytes of speculative section requests the read holds started and not released;
    /// `0` for a read that starts none, such as one with a row limit.
    private final long fetchAheadBytes;
    /// Told, outside the lock, that a request has started, which may let planning go further.
    private final Runnable onStarted;
    /// Registered with R while a request did not fit; one instance, so [#close] can cancel it.
    private final Runnable onRelease = this::onRelease;

    private final ReentrantLock lock = new ReentrantLock();
    /// Guarded by `lock`: the requests queued and not yet started, in position order. One started
    /// otherwise stays in the queue until the pump reaches it, which then drops it.
    private final PriorityQueue<Request> queued = new PriorityQueue<>(Comparator
            .comparingInt((Request request) -> request.fileIndex)
            .thenComparingInt(request -> request.workItemIndex)
            .thenComparingLong(request -> request.offset));
    /// Guarded by `lock`: the section requests of each work item, in position order, until it is
    /// forgotten.
    private final Map<Integer, List<SectionRequest>> sections = new HashMap<>();
    /// Guarded by `lock`: the bytes of the speculative section requests started and not released.
    private long speculativeBytes;
    /// Guarded by `lock`: the bytes of the section requests inserted and not started.
    private long queuedBytes;
    /// Guarded by `lock`: the last work item with a started section request, `-1` before any.
    private int lastStarted = -1;
    /// Guarded by `lock`: the last work item whose first section request is the read's next unit,
    /// or that a column asked to plan.
    private int nextUnit = -1;
    /// Guarded by `lock`: whether [#onRelease] is registered with R.
    private boolean waitingForRoom;
    private boolean closed;

    /// @param tasks where the section requests' reads run, which the read's owner waits for before
    ///        it closes the files
    /// @param readAhead the context's read-ahead buffer, R
    /// @param fetchAheadBytes F; `0` for a read that starts no speculative section request
    /// @param onStarted run outside the lock whenever a request has started
    public FetchSequence(PrefetchTasks tasks, ReadAheadBuffer readAhead, long fetchAheadBytes, Runnable onStarted) {
        this.tasks = tasks;
        this.readAhead = readAhead;
        this.fetchAheadBytes = fetchAheadBytes;
        this.onStarted = onStarted;
    }

    /// A sequence whose section requests start on demand only, for plans built outside a read.
    public static FetchSequence demandOnly(HardwoodContextImpl context) {
        return new FetchSequence(new PrefetchTasks(context.ioExecutor()), new ReadAheadBuffer(0), 0, () -> {});
    }

    /// F for a read whose largest request is `maxSpanBytes`: twice that unless overridden through
    /// [#FETCH_AHEAD_BYTES_PROPERTY]. A value that is not a non-negative byte count is rejected
    /// rather than replaced by the default.
    public static long fetchAheadBytes(int maxSpanBytes) {
        return InternalProperty.longValue(FETCH_AHEAD_BYTES_PROPERTY, 2L * maxSpanBytes, 0, "a non-negative byte count");
    }

    /// A range of the file a planned row group reads as one section request: `length` bytes at
    /// `start`, requested under `reason`, held by `holders` columns until each has passed it.
    public record Span(long start, int length, String reason, int holders) {
    }

    /// The section requests of work item `workItemIndex` of file `fileIndex`, `inputFile`, one per
    /// span in position order: those inserted before, if the work item has any, or new ones, which
    /// start by the rules above. For a file in memory, and for a span inside the file's tail,
    /// section requests the sequence does not start, sliced in place.
    public List<SectionRequest> insert(int fileIndex, int workItemIndex, InputFile inputFile, List<Span> spans) {
        if (HeldBytes.inMemory(inputFile)) {
            List<SectionRequest> inMemory = new ArrayList<>(spans.size());
            for (Span span : spans) {
                inMemory.add(inPlace(workItemIndex, inputFile, span));
            }
            return inMemory;
        }
        List<SectionRequest> inserted;
        RequestAttempt<ByteBuffer> first = null;
        lock.lock();
        try {
            List<SectionRequest> existing = sections.get(workItemIndex);
            if (existing != null) {
                return existing;
            }
            inserted = new ArrayList<>(spans.size());
            for (Span span : spans) {
                SectionRequest inTail = inPlace(workItemIndex, inputFile, span);
                if (inTail != null) {
                    // Present already: as far as planning ahead goes, started
                    inserted.add(inTail);
                    lastStarted = Math.max(lastStarted, workItemIndex);
                    continue;
                }
                SectionRequest section = new SectionRequest(this, inputFile, span, fileIndex, workItemIndex, null);
                inserted.add(section);
                enqueue(section);
            }
            sections.put(workItemIndex, inserted);
            if (workItemIndex <= nextUnit) {
                first = startFirst(workItemIndex);
            }
        }
        finally {
            lock.unlock();
        }
        started(first);
        pump();
        return inserted;
    }

    /// A section request of `length` bytes at `start` that a walk of work item `workItemIndex`
    /// found it needs while it parses, such as the rest of a column chunk a row limit left out: not
    /// queued, since the walk demands it at once.
    SectionRequest insertDemanded(int workItemIndex, InputFile inputFile, long start, int length, String reason) {
        Span span = new Span(start, length, reason, 1);
        SectionRequest inPlace = inPlace(workItemIndex, inputFile, span);
        if (inPlace != null) {
            return inPlace;
        }
        SectionRequest section = new SectionRequest(this, inputFile, span, Request.UNQUEUED, workItemIndex, null);
        lock.lock();
        try {
            section.released = closed;
            sections.computeIfAbsent(workItemIndex, i -> new ArrayList<>()).add(section);
        }
        finally {
            lock.unlock();
        }
        return section;
    }

    /// A section request of `length` bytes at `start` that the read's planning reads, released once
    /// each of its `holders` has released it: a window's request, a dictionary page, a probe. It is
    /// neither queued nor kept by work item: it starts when planning demands it
    /// ([SectionRequest#issue]), and is charged to neither F nor R. For a file in memory, and for a
    /// range inside the file's tail, a section request sliced in place.
    public SectionRequest plannedSection(InputFile inputFile, long start, int length, String reason, int holders) {
        Span span = new Span(start, length, reason, holders);
        SectionRequest inPlace = inPlace(PLANNING, inputFile, span);
        if (inPlace != null) {
            return inPlace;
        }
        SectionRequest section = new SectionRequest(this, inputFile, span, Request.UNQUEUED, PLANNING, null);
        lock.lock();
        try {
            section.released = closed;
        }
        finally {
            lock.unlock();
        }
        return section;
    }

    /// A section request for `span` that needs no read: of a file in memory, sliced on its first
    /// poll, or inside the file's tail, sliced now, since the tail may be dropped before the
    /// section request is read. `null` for any other span.
    public static SectionRequest inPlace(int workItemIndex, InputFile inputFile, Span span) {
        if (HeldBytes.inMemory(inputFile)) {
            return new SectionRequest(null, inputFile, span, Request.UNQUEUED, workItemIndex, null);
        }
        ByteBuffer held = HeldBytes.slice(inputFile, span.start(), span.length());
        return held != null ? new SectionRequest(null, inputFile, span, Request.UNQUEUED, workItemIndex, held) : null;
    }

    /// The section requests inserted for work item `workItemIndex`, `null` when it has none.
    public List<SectionRequest> sections(int workItemIndex) {
        lock.lock();
        try {
            return sections.get(workItemIndex);
        }
        finally {
            lock.unlock();
        }
    }

    /// Queues `requests`, which start in position order while they fit R; one that has started or
    /// is released already is not queued.
    public void queue(List<? extends Request> requests) {
        lock.lock();
        try {
            for (Request request : requests) {
                enqueue(request);
            }
        }
        finally {
            lock.unlock();
        }
        pump();
    }

    /// Starts `request` at once and uncharged, unless it has started or is released.
    public void startNow(Request request) {
        Runnable start;
        lock.lock();
        try {
            if (closed || request.released || request.started) {
                return;
            }
            start = start(request);
        }
        finally {
            lock.unlock();
        }
        start.run();
        onStarted.run();
    }

    /// Records that work item `workItemIndex` is the read's next unit, since the first column has
    /// entered the one before it, or that a column asked to plan it and waits for the planning
    /// that inserts it; starts its first section request if it is inserted and not started.
    public void nextUnit(int workItemIndex) {
        RequestAttempt<ByteBuffer> first;
        lock.lock();
        try {
            if (closed || workItemIndex <= nextUnit) {
                return;
            }
            nextUnit = workItemIndex;
            first = startFirst(workItemIndex);
        }
        finally {
            lock.unlock();
        }
        started(first);
    }

    /// Whether work item `workItemIndex` is within the read's next unit, which the planning chain
    /// plans whatever F holds.
    public boolean withinNextUnit(int workItemIndex) {
        lock.lock();
        try {
            return workItemIndex <= nextUnit;
        }
        finally {
            lock.unlock();
        }
    }

    /// Whether the planning chain may plan work item `workItemIndex` further ahead than the next
    /// unit: while the bytes of the section requests not started are below F and the work item is
    /// at most one past the last with a started section request, which bounds the dictionaries and
    /// index windows held for planned row groups. A work item whose section requests lie in a
    /// file's tail counts as started.
    public boolean mayPlanAhead(int workItemIndex) {
        lock.lock();
        try {
            return queuedBytes < fetchAheadBytes && workItemIndex <= lastStarted + 1;
        }
        finally {
            lock.unlock();
        }
    }

    /// Where the read's reads run.
    public PrefetchTasks tasks() {
        return tasks;
    }

    /// Starts, uncharged, the first section request of `workItemIndex` not sliced in place if it is
    /// not started. Called under `lock`.
    ///
    /// @return the read to submit, `null` when there is none
    private RequestAttempt<ByteBuffer> startFirst(int workItemIndex) {
        List<SectionRequest> ofWorkItem = sections.get(workItemIndex);
        if (ofWorkItem == null) {
            return null;
        }
        for (SectionRequest section : ofWorkItem) {
            if (section.sequence != null) {
                return section.attempt == null && !section.released ? begin(section, true) : null;
            }
        }
        return null;
    }

    /// The read of `section`'s bytes: the one under way or done, or one started as demand when none
    /// is under way or one started ahead of demand failed with an exception.
    ///
    /// @throws IllegalStateException if every column holding the section request has passed it
    RequestAttempt<ByteBuffer> demand(SectionRequest section) {
        RequestAttempt<ByteBuffer> current = section.attempt;
        if (current != null && !current.failedAhead()) {
            return current;
        }
        RequestAttempt<ByteBuffer> attempt;
        lock.lock();
        try {
            if (section.released) {
                throw new IllegalStateException("Section request at offset " + section.offset + " (length "
                        + section.length + ") of " + section.inputFile.name() + " was released before it was read");
            }
            current = section.attempt;
            if (current != null && !current.failedAhead()) {
                return current;
            }
            attempt = begin(section, false);
        }
        finally {
            lock.unlock();
        }
        started(attempt);
        return attempt;
    }

    /// Starts every queued request that fits, in position order, and registers for a release of R
    /// when the next one does not fit R.
    private void pump() {
        List<Runnable> starts = new ArrayList<>();
        boolean register = false;
        long releasesSeen;
        lock.lock();
        try {
            if (closed) {
                return;
            }
            releasesSeen = readAhead.releases();
            Request next;
            while ((next = queued.peek()) != null) {
                if (!next.queued) {
                    // Started otherwise, or released, since it was queued
                    queued.poll();
                    continue;
                }
                if (speculativeBytes + next.towardF() > fetchAheadBytes) {
                    break;
                }
                if (!readAhead.tryCharge(next.length)) {
                    register = !waitingForRoom;
                    waitingForRoom = true;
                    break;
                }
                queued.poll();
                next.charged = true;
                speculativeBytes += next.towardF();
                starts.add(start(next));
            }
        }
        finally {
            lock.unlock();
        }
        for (Runnable start : starts) {
            start.run();
        }
        if (register) {
            readAhead.whenReleased(releasesSeen, onRelease);
        }
        if (!starts.isEmpty()) {
            onStarted.run();
        }
    }

    /// Run by R on the thread releasing it, perhaps another read's: takes the lock only briefly.
    private void onRelease() {
        lock.lock();
        try {
            waitingForRoom = false;
        }
        finally {
            lock.unlock();
        }
        pump();
    }

    /// Records that a column holding `section` has passed it; the last one releases it.
    void release(SectionRequest section) {
        long uncharge;
        lock.lock();
        try {
            if (section.released || --section.holders > 0) {
                return;
            }
            uncharge = drop(section);
        }
        finally {
            lock.unlock();
        }
        uncharge(uncharge);
    }

    /// Releases `request` whatever its holders, returning its charge, also while its read is in
    /// flight.
    public void discard(Request request) {
        long uncharge;
        lock.lock();
        try {
            if (request.released) {
                return;
            }
            uncharge = drop(request);
        }
        finally {
            lock.unlock();
        }
        uncharge(uncharge);
    }

    /// Releases the section requests of work item `workItemIndex` that columns still hold, once
    /// every column has released the work item, and forgets them.
    public void forget(int workItemIndex) {
        long uncharge = 0;
        lock.lock();
        try {
            List<SectionRequest> ofWorkItem = sections.remove(workItemIndex);
            if (ofWorkItem == null) {
                return;
            }
            for (SectionRequest section : ofWorkItem) {
                if (!section.released) {
                    uncharge += drop(section);
                }
            }
        }
        finally {
            lock.unlock();
        }
        uncharge(uncharge);
    }

    /// Drops every request not started and releases the charges of the section requests. The reads
    /// in flight are left to the read's [PrefetchTasks], which its owner waits for next; the
    /// requests queued by other parts of the read are theirs to discard.
    public void close() {
        long uncharge = 0;
        lock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            for (List<SectionRequest> ofWorkItem : sections.values()) {
                for (SectionRequest section : ofWorkItem) {
                    if (!section.released) {
                        uncharge += drop(section);
                    }
                }
            }
            sections.clear();
            queued.clear();
        }
        finally {
            lock.unlock();
        }
        readAhead.cancel(onRelease);
        if (uncharge > 0) {
            readAhead.release(uncharge);
        }
    }

    /// The bytes of the speculative section requests started and not released, for tests.
    long speculativeBytes() {
        lock.lock();
        try {
            return speculativeBytes;
        }
        finally {
            lock.unlock();
        }
    }

    /// Queues `request`, unless it has started or is released, or releases it if the sequence is
    /// closed. Called under `lock`.
    private void enqueue(Request request) {
        if (closed || request.released || request.started) {
            request.released |= closed;
            return;
        }
        request.queued = true;
        queued.add(request);
        if (request.chargesF) {
            queuedBytes += request.length;
        }
    }

    /// Takes `request` out of the queue, if it waits there. Called under `lock`.
    private void unqueue(Request request) {
        if (request.queued) {
            request.queued = false;
            if (request.chargesF) {
                queuedBytes -= request.length;
            }
        }
    }

    /// Starts `request`, which the caller has charged if it starts ahead of need. Called under
    /// `lock`.
    ///
    /// @return what starts its I/O, for the caller to run once it has let go of the lock
    private Runnable start(Request request) {
        unqueue(request);
        request.started = true;
        return request.begin();
    }

    /// Starts `section`'s read; one started `aheadOfDemand`, a speculative or next-unit read, that
    /// fails with an exception does not fail the read, and a step that needs its bytes reads them
    /// again ([RequestAttempt#failedAhead]). Called under `lock`, which the caller has charged the
    /// section request under if it is speculative.
    ///
    /// @return the read, for the caller to submit once it has let go of the lock
    RequestAttempt<ByteBuffer> begin(SectionRequest section, boolean aheadOfDemand) {
        unqueue(section);
        section.started = true;
        RequestAttempt<ByteBuffer> attempt = new RequestAttempt<>(
                ExceptionContext.filePrefix(section.inputFile.name()) + "Failed to fetch data at offset "
                        + section.offset + " (length " + section.length + ")",
                aheadOfDemand, tasks, section::readBytes, () -> {
                    throw new IOException(ExceptionContext.filePrefix(section.inputFile.name())
                            + "The read was closed before the range at offset " + section.offset + " (length "
                            + section.length + ") was fetched");
                });
        section.attempt = attempt;
        lastStarted = Math.max(lastStarted, section.workItemIndex);
        attempt.whenDone(() -> completed(section, attempt));
        return attempt;
    }

    /// Submits `read`, if any, and tells planning a section request has started. Called outside
    /// `lock`.
    private void started(RequestAttempt<ByteBuffer> attempt) {
        if (attempt != null) {
            attempt.submit();
            onStarted.run();
        }
    }

    /// Drops `section`'s charges once its read started ahead of demand has failed, so that it is
    /// started again only on demand.
    private void completed(SectionRequest section, RequestAttempt<ByteBuffer> attempt) {
        if (!attempt.failedAhead()) {
            return;
        }
        long uncharge = 0;
        lock.lock();
        try {
            if (section.attempt == attempt && section.charged) {
                section.charged = false;
                speculativeBytes -= section.length;
                uncharge = section.length;
            }
        }
        finally {
            lock.unlock();
        }
        uncharge(uncharge);
    }

    /// Marks `request` released, takes it out of the queue and drops its bytes. Called under `lock`.
    ///
    /// @return the bytes of R the request held, for the caller to release once it has let go of the
    ///         lock
    private long drop(Request request) {
        request.released = true;
        request.dropped();
        unqueue(request);
        if (!request.charged) {
            return 0;
        }
        request.charged = false;
        speculativeBytes -= request.towardF();
        return request.length;
    }

    /// Returns `bytes` to R and starts what then fits: a release may also have dropped a request
    /// from the queue, which lets those behind it start. Called outside `lock`.
    private void uncharge(long bytes) {
        if (bytes > 0) {
            readAhead.release(bytes);
        }
        pump();
    }
}
