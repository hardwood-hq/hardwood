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
import dev.hardwood.internal.FetchReason;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.io.HeldBytes;
import dev.hardwood.internal.io.ReadRanges;
import dev.hardwood.internal.reader.memory.InternalProperty;
import dev.hardwood.internal.reader.memory.ReadAheadBuffer;

/// Schedules the requests of one read: when each starts, what it is charged to while it is held,
/// and when it is released. Most are entries, requests for a row group's data: a range of the file
/// its columns' needed ranges were coalesced into. Planning a row group inserts its entries
/// ([#insert]); the columns' walks take a page's bytes as a slice of a completed entry
/// ([Entry#poll]), through an index fixed when the row group was planned. Each entry's read runs on
/// a virtual thread of the context's I/O executor ([RequestAttempt]). Other parts of the read queue
/// requests of their own ([#queue]), such as the footers of files it has not reached, and release
/// them ([#discard]).
///
/// The sequence keeps its requests in position order, the order the read consumes them: by file,
/// then by work item, then by offset, a file's other requests before its entries. A request the
/// read cannot go on without starts at once and is charged to nothing. A request started ahead of
/// need is charged: its bytes count against a limit from its start until its release, and it
/// starts only while they fit. There are two limits: F, the bytes of speculative entries one read
/// holds, and R, the context's [ReadAheadBuffer], which its reads share. Nothing waits for room
/// under a limit, so no read waits for a charge that only its own progress releases.
///
/// | Request | Starts | Charged to | Released |
/// |---|---|---|---|
/// | An entry a step yields on ([#demand]), or one a walk finds it needs while it parses ([#insertDemanded]) | at once | nothing | once every column holding it has passed it ([#release]) |
/// | The first entry of the next unit, the work item after the one the first column has entered ([#nextUnit]) | at once | nothing | as above |
/// | Every other entry, under no row limit | in position order, while it fits F and R | F and R | as above |
/// | An entry planning reads: a window's request, a dictionary page, a probe ([#plannedEntry]) | when planning demands it | nothing | once each of its holders has released it |
/// | An entry of a file in memory, or inside a file's tail the read holds ([#inPlace]) | never: sliced in place | nothing | not held |
/// | A request another part of the read queues ([#queue]) | in position order, while it fits R; at once when started now ([#startNow]) | R; nothing when started now | when its owner discards it ([#discard]) |
///
/// The pump starts what may start now: from the front of the queue every request that fits,
/// stopping at the first that does not. One that does not fit F starts once an entry's release
/// returns its bytes; one that does not fit R starts once any read releases R, which runs the pump
/// again ([#onRelease]). The pump runs after every event that can make room or add work: requests
/// queued, a request released or dropped from the queue, R released.
///
/// A page slice taken from an entry keeps its bytes alive past its release. A speculative entry that
/// fails is dropped and started again when demanded. [#close] drops the requests not started and
/// releases the charges of the entries; the reads in flight are the read's [PrefetchTasks], which
/// its owner waits for.
///
/// One lock guards the requests and their positions. It is held across no I/O, no waiter and no
/// completion of a read: reads are submitted, waiters registered and charges released after it is
/// let go. A walk polls an entry without it.
public final class FetchSequence {

    public static final String FETCH_AHEAD_BYTES_PROPERTY = "hardwood.internal.fetchAheadBytes";

    /// The work item of an entry planning reads ([#plannedEntry]), which no work item holds.
    public static final int PLANNING = -1;

    private final PrefetchTasks tasks;
    private final ReadAheadBuffer readAhead;
    /// F: the most bytes of speculative entries the read holds started and not released; `0` for a
    /// read that starts none, such as one with a row limit.
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
    /// Guarded by `lock`: the entries of each work item, in position order, until it is forgotten.
    private final Map<Integer, List<Entry>> entries = new HashMap<>();
    /// Guarded by `lock`: the bytes of the speculative entries started and not released.
    private long speculativeBytes;
    /// Guarded by `lock`: the bytes of the entries inserted and not started.
    private long queuedBytes;
    /// Guarded by `lock`: the last work item with a started entry, `-1` before any.
    private int lastStarted = -1;
    /// Guarded by `lock`: the last work item whose first entry is the read's next unit, or that a
    /// column asked to plan.
    private int nextUnit = -1;
    /// Guarded by `lock`: whether [#onRelease] is registered with R.
    private boolean waitingForRoom;
    private boolean closed;

    /// @param tasks where the entries' reads run, which the read's owner waits for before it
    ///        closes the files
    /// @param readAhead the context's read-ahead buffer, R
    /// @param fetchAheadBytes F; `0` for a read that starts no speculative entry
    /// @param onStarted run outside the lock whenever a request has started
    public FetchSequence(PrefetchTasks tasks, ReadAheadBuffer readAhead, long fetchAheadBytes, Runnable onStarted) {
        this.tasks = tasks;
        this.readAhead = readAhead;
        this.fetchAheadBytes = fetchAheadBytes;
        this.onStarted = onStarted;
    }

    /// A sequence whose entries start on demand only, for plans built outside a read.
    public static FetchSequence demandOnly(HardwoodContextImpl context) {
        return new FetchSequence(new PrefetchTasks(context.ioExecutor()), new ReadAheadBuffer(0), 0, () -> {});
    }

    /// F for a read whose largest request is `maxSpanBytes`: twice that unless overridden through
    /// [#FETCH_AHEAD_BYTES_PROPERTY]. A value that is not a non-negative byte count is rejected
    /// rather than replaced by the default.
    public static long fetchAheadBytes(int maxSpanBytes) {
        return InternalProperty.longValue(FETCH_AHEAD_BYTES_PROPERTY, 2L * maxSpanBytes, 0, "a non-negative byte count");
    }

    /// A range of the file a planned row group reads as one entry: `length` bytes at `start`,
    /// requested under `reason`, held by `holders` columns until each has passed it.
    public record Span(long start, int length, String reason, int holders) {
    }

    /// The entries of work item `workItemIndex` of file `fileIndex`, `inputFile`, one per span in
    /// position order: those inserted before, if the work item has any, or new ones, which start by
    /// the rules above. For a file in memory, and for a span inside the file's tail, entries the
    /// sequence does not start, sliced in place.
    public List<Entry> insert(int fileIndex, int workItemIndex, InputFile inputFile, List<Span> spans) {
        if (HeldBytes.inMemory(inputFile)) {
            List<Entry> inMemory = new ArrayList<>(spans.size());
            for (Span span : spans) {
                inMemory.add(inPlace(workItemIndex, inputFile, span));
            }
            return inMemory;
        }
        List<Entry> inserted;
        RequestAttempt<ByteBuffer> first = null;
        lock.lock();
        try {
            List<Entry> existing = entries.get(workItemIndex);
            if (existing != null) {
                return existing;
            }
            inserted = new ArrayList<>(spans.size());
            for (Span span : spans) {
                Entry inTail = inPlace(workItemIndex, inputFile, span);
                if (inTail != null) {
                    // Present already: as far as planning ahead goes, started
                    inserted.add(inTail);
                    lastStarted = Math.max(lastStarted, workItemIndex);
                    continue;
                }
                Entry entry = new Entry(this, inputFile, span, fileIndex, workItemIndex, null);
                inserted.add(entry);
                enqueue(entry);
            }
            entries.put(workItemIndex, inserted);
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

    /// An entry of `length` bytes at `start` that a walk of work item `workItemIndex` found it
    /// needs while it parses, such as the rest of a column chunk a row limit left out: not queued,
    /// since the walk demands it at once.
    Entry insertDemanded(int workItemIndex, InputFile inputFile, long start, int length, String reason) {
        Span span = new Span(start, length, reason, 1);
        Entry inPlace = inPlace(workItemIndex, inputFile, span);
        if (inPlace != null) {
            return inPlace;
        }
        Entry entry = new Entry(this, inputFile, span, Request.UNQUEUED, workItemIndex, null);
        lock.lock();
        try {
            entry.released = closed;
            entries.computeIfAbsent(workItemIndex, i -> new ArrayList<>()).add(entry);
        }
        finally {
            lock.unlock();
        }
        return entry;
    }

    /// An entry of `length` bytes at `start` that the read's planning reads, released once each of
    /// its `holders` has released it: a window's request, a dictionary page, a probe. It is neither
    /// queued nor kept by work item: it starts when planning demands it ([Entry#issue]), and is
    /// charged to neither F nor R. For a file in memory, and for a range inside the file's tail, an
    /// entry sliced in place.
    public Entry plannedEntry(InputFile inputFile, long start, int length, String reason, int holders) {
        Span span = new Span(start, length, reason, holders);
        Entry inPlace = inPlace(PLANNING, inputFile, span);
        if (inPlace != null) {
            return inPlace;
        }
        Entry entry = new Entry(this, inputFile, span, Request.UNQUEUED, PLANNING, null);
        lock.lock();
        try {
            entry.released = closed;
        }
        finally {
            lock.unlock();
        }
        return entry;
    }

    /// An entry for `span` that needs no read: of a file in memory, sliced on its first poll, or
    /// inside the file's tail, sliced now, since the tail may be dropped before the entry is read.
    /// `null` for any other span.
    public static Entry inPlace(int workItemIndex, InputFile inputFile, Span span) {
        if (HeldBytes.inMemory(inputFile)) {
            return new Entry(null, inputFile, span, Request.UNQUEUED, workItemIndex, null);
        }
        ByteBuffer held = HeldBytes.slice(inputFile, span.start(), span.length());
        return held != null ? new Entry(null, inputFile, span, Request.UNQUEUED, workItemIndex, held) : null;
    }

    /// The entries inserted for work item `workItemIndex`, `null` when it has none.
    public List<Entry> entries(int workItemIndex) {
        lock.lock();
        try {
            return entries.get(workItemIndex);
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
    /// that inserts it; starts its first entry if it is inserted and not started.
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
    /// unit: while the bytes of the entries not started are below F and the work item is at most
    /// one past the last with a started entry, which bounds the dictionaries and index windows held
    /// for planned row groups. A work item whose entries lie in a file's tail counts as started.
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

    /// Starts, uncharged, the first entry of `workItemIndex` not sliced in place if it is not
    /// started. Called under `lock`.
    ///
    /// @return the read to submit, `null` when there is none
    private RequestAttempt<ByteBuffer> startFirst(int workItemIndex) {
        List<Entry> ofWorkItem = entries.get(workItemIndex);
        if (ofWorkItem == null) {
            return null;
        }
        for (Entry entry : ofWorkItem) {
            if (entry.sequence != null) {
                return entry.attempt == null && !entry.released ? begin(entry, true) : null;
            }
        }
        return null;
    }

    /// The read of `entry`'s bytes: the one under way or done, or one started as demand when none
    /// is under way or one started ahead of demand failed with an exception.
    ///
    /// @throws IllegalStateException if every column holding the entry has passed it
    RequestAttempt<ByteBuffer> demand(Entry entry) {
        RequestAttempt<ByteBuffer> current = entry.attempt;
        if (current != null && !current.failedAhead()) {
            return current;
        }
        RequestAttempt<ByteBuffer> attempt;
        lock.lock();
        try {
            if (entry.released) {
                throw new IllegalStateException("Entry at offset " + entry.offset + " (length " + entry.length
                        + ") of " + entry.inputFile.name() + " was released before it was read");
            }
            current = entry.attempt;
            if (current != null && !current.failedAhead()) {
                return current;
            }
            attempt = begin(entry, false);
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

    /// Records that a column holding `entry` has passed it; the last one releases it.
    void release(Entry entry) {
        long uncharge;
        lock.lock();
        try {
            if (entry.released || --entry.holders > 0) {
                return;
            }
            uncharge = drop(entry);
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

    /// Releases the entries of work item `workItemIndex` that columns still hold, once every
    /// column has released the work item, and forgets them.
    public void forget(int workItemIndex) {
        long uncharge = 0;
        lock.lock();
        try {
            List<Entry> ofWorkItem = entries.remove(workItemIndex);
            if (ofWorkItem == null) {
                return;
            }
            for (Entry entry : ofWorkItem) {
                if (!entry.released) {
                    uncharge += drop(entry);
                }
            }
        }
        finally {
            lock.unlock();
        }
        uncharge(uncharge);
    }

    /// Drops every request not started and releases the charges of the entries. The reads in flight
    /// are left to the read's [PrefetchTasks], which its owner waits for next; the requests queued
    /// by other parts of the read are theirs to discard.
    public void close() {
        long uncharge = 0;
        lock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            for (List<Entry> ofWorkItem : entries.values()) {
                for (Entry entry : ofWorkItem) {
                    if (!entry.released) {
                        uncharge += drop(entry);
                    }
                }
            }
            entries.clear();
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

    /// The bytes of the speculative entries started and not released, for tests.
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

    /// Starts `entry`'s read; one started `aheadOfDemand`, a speculative or next-unit read, that
    /// fails with an exception does not fail the read, and a step that needs its bytes reads them
    /// again ([RequestAttempt#failedAhead]). Called under `lock`, which the caller has charged the entry
    /// under if it is speculative.
    ///
    /// @return the read, for the caller to submit once it has let go of the lock
    private RequestAttempt<ByteBuffer> begin(Entry entry, boolean aheadOfDemand) {
        unqueue(entry);
        entry.started = true;
        RequestAttempt<ByteBuffer> attempt = new RequestAttempt<>(
                ExceptionContext.filePrefix(entry.inputFile.name()) + "Failed to fetch data at offset "
                        + entry.offset + " (length " + entry.length + ")",
                aheadOfDemand, tasks, entry::readBytes, () -> {
                    throw new IOException(ExceptionContext.filePrefix(entry.inputFile.name())
                            + "The read was closed before the range at offset " + entry.offset + " (length "
                            + entry.length + ") was fetched");
                });
        entry.attempt = attempt;
        lastStarted = Math.max(lastStarted, entry.workItemIndex);
        attempt.whenDone(() -> completed(entry, attempt));
        return attempt;
    }

    /// Submits `read`, if any, and tells planning an entry has started. Called outside `lock`.
    private void started(RequestAttempt<ByteBuffer> attempt) {
        if (attempt != null) {
            attempt.submit();
            onStarted.run();
        }
    }

    /// Drops `entry`'s charges once its read started ahead of demand has failed, so that it is
    /// started again only on demand.
    private void completed(Entry entry, RequestAttempt<ByteBuffer> attempt) {
        if (!attempt.failedAhead()) {
            return;
        }
        long uncharge = 0;
        lock.lock();
        try {
            if (entry.attempt == attempt && entry.charged) {
                entry.charged = false;
                speculativeBytes -= entry.length;
                uncharge = entry.length;
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

    /// A request of the read, at a position in the sequence: an entry, or a request another part of
    /// the read queues ([#queue]) ahead of a file's entries, such as the file's footer.
    public abstract static class Request {

        /// The file of a request that is never queued, which needs no position.
        static final int UNQUEUED = -1;

        final int fileIndex;
        final int workItemIndex;
        final long offset;
        final int length;
        /// Whether a speculative start charges the request to F as well as to R: an entry's does.
        final boolean chargesF;
        /// Guarded by the sequence's lock: whether the request holds a charge.
        boolean charged;
        /// Guarded by the sequence's lock: whether the request waits in the queue.
        boolean queued;
        /// Guarded by the sequence's lock: whether the request is released.
        boolean released;
        /// Written under the sequence's lock: whether the request has started.
        volatile boolean started;

        /// A request of `length` bytes, the charge it takes from R when it starts ahead of need,
        /// positioned ahead of every entry of file `fileIndex`.
        protected Request(int fileIndex, int length) {
            this(fileIndex, PLANNING, 0, length, false);
        }

        private Request(int fileIndex, int workItemIndex, long offset, int length, boolean chargesF) {
            this.fileIndex = fileIndex;
            this.workItemIndex = workItemIndex;
            this.offset = offset;
            this.length = length;
            this.chargesF = chargesF;
        }

        /// Starts the request. Called under the sequence's lock, which it takes no other lock under.
        ///
        /// @return what starts its I/O, which the sequence runs once it has let go of the lock
        protected abstract Runnable begin();

        /// Whether the request has started.
        public boolean started() {
            return started;
        }

        /// Drops what the request holds once it is released. Called under the sequence's lock.
        void dropped() {
        }

        int towardF() {
            return chargesF ? length : 0;
        }
    }

    /// One data request of a read: a range of the file the needed ranges of one or more columns of
    /// a row group were coalesced into. A walk asks for its bytes with [#poll], which never waits
    /// and issues no read; while they are not present the entry is the [ByteNeed] the step issues
    /// ([#issue]), which starts it on demand or joins the read under way.
    public static final class Entry extends Request implements ByteNeed {

        /// `null` for an entry sliced in place, which no sequence holds.
        private final FetchSequence sequence;
        private final InputFile inputFile;
        private final String reason;
        /// Guarded by the sequence's lock: the columns that have not passed the entry.
        private int holders;
        /// The read under way or done; `null` before it starts and once released. Replaced only to
        /// repeat a read started ahead of demand that failed with an exception.
        private volatile RequestAttempt<ByteBuffer> attempt;
        /// The bytes of an entry sliced in place: of a file in memory, sliced on first poll, or
        /// inside the file's tail, sliced as it is inserted.
        private volatile ByteBuffer inPlace;

        private Entry(FetchSequence sequence, InputFile inputFile, Span span, int fileIndex, int workItemIndex,
                      ByteBuffer inPlace) {
            super(fileIndex, workItemIndex, span.start(), span.length(), true);
            this.inPlace = inPlace;
            this.sequence = sequence;
            this.inputFile = inputFile;
            this.reason = span.reason();
            this.holders = span.holders();
        }

        public long offset() {
            return offset;
        }

        int length() {
            return length;
        }

        public long end() {
            return offset + length;
        }

        int workItemIndex() {
            return workItemIndex;
        }

        String reason() {
            return reason;
        }

        @Override
        protected Runnable begin() {
            return sequence.begin(this, true)::submit;
        }

        @Override
        void dropped() {
            attempt = null;
        }

        /// The entry's bytes, or `null` while they are not present, without waiting and without
        /// issuing a read, except for a file whose bytes are in memory, which it slices in place.
        ///
        /// @throws IOException if the demand read failed
        public ByteBuffer poll() throws IOException {
            if (sequence == null) {
                ByteBuffer bytes = inPlace;
                if (bytes == null) {
                    // A race slices twice, which costs nothing on a file in memory
                    bytes = ReadRanges.read(inputFile, offset, length);
                    inPlace = bytes;
                }
                return bytes;
            }
            RequestAttempt<ByteBuffer> current = attempt;
            if (current == null || !current.isDone() || current.failedAhead()) {
                return null;
            }
            return current.take();
        }

        /// Reads the entry's bytes from the file, under the entry's [FetchReason].
        private ByteBuffer readBytes() throws IOException {
            try (FetchReason.Scope ignored = FetchReason.set(reason)) {
                return ReadRanges.read(inputFile, offset, length);
            }
        }

        /// A slice of `length` bytes at absolute `fileOffset` from bytes [#poll] returned.
        ///
        /// @throws IllegalArgumentException if the range lies outside the entry
        public ByteBuffer slice(ByteBuffer polled, long fileOffset, int sliceLength) {
            if (fileOffset < offset || sliceLength < 0 || fileOffset + sliceLength > end()) {
                throw new IllegalArgumentException("Range [" + fileOffset + ", " + (fileOffset + sliceLength)
                        + ") lies outside the entry [" + offset + ", " + end() + ") (" + reason + ")");
            }
            return polled.slice(Math.toIntExact(fileOffset - offset), sliceLength);
        }

        @Override
        public RequestAttempt<ByteBuffer> issue() {
            if (sequence == null) {
                throw new IllegalStateException("An entry sliced in place is never read");
            }
            return sequence.demand(this);
        }

        /// Records that a column holding the entry has passed it.
        public void release() {
            if (sequence != null) {
                sequence.release(this);
            }
        }

        /// Releases the entry whatever its holders, as the read closes.
        public void discard() {
            if (sequence != null) {
                sequence.discard(this);
            }
        }

        @Override
        public String toString() {
            return "Entry[" + reason + " at " + offset + " (length " + length + ")]";
        }
    }
}
