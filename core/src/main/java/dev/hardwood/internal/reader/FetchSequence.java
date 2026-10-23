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
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.locks.ReentrantLock;

import dev.hardwood.InputFile;

/// The data requests of one read, its entries, at positions in the order the read consumes them:
/// by work item, then by offset. Planning a row group inserts its entries ([#insert]), each a
/// range of the file its columns' needed ranges were coalesced into; the columns' walks take a
/// page's bytes as a slice of a completed entry ([Entry#poll]), through an index fixed when the
/// row group was planned. The sequence starts each entry by one of three rules, runs it on a
/// virtual thread of the context's I/O executor ([RangeRead]) and completes it:
///
/// - **Demand.** A step that yields on an entry starts it at once, whatever F and R hold, and it is
///   charged to neither ([#demand]).
/// - **The next unit.** When the first column enters a row group, the first entry of the next
///   work item in position order starts without F and is not charged to R ([#nextUnit]).
/// - **Speculative.** Every other entry starts in position order while the read's started
///   speculative bytes stay within F and the context's [ReadAheadBuffer] (R) has room for them
///   ([#pump]). Nothing waits for R: an entry that does not fit starts on a later release. A read
///   with a row limit starts none.
///
/// An entry is released once every column whose chunk extent overlaps it has passed it
/// ([#release]), which returns its charges to F and R and drops its bytes; a page slice taken
/// from it keeps them alive past that. A speculative entry that fails is dropped and started again
/// when demanded. [#close] drops the entries not started and releases every charge; the reads in
/// flight are the read's [PrefetchTasks], which its owner waits for.
///
/// A file whose bytes are in memory gets no entries in the sequence: its entries are sliced from the
/// mapping in place ([#insert]).
///
/// One lock guards the entries and their positions. It is held across no I/O, no waiter and no
/// completion of a read: reads are submitted, waiters registered and charges released after it is
/// let go. A walk polls an entry without it.
final class FetchSequence {

    static final String FETCH_AHEAD_BYTES_PROPERTY = "hardwood.internal.fetchAheadBytes";

    private final PrefetchTasks tasks;
    private final ReadAheadBuffer readAhead;
    /// F: the most bytes of speculative entries the read holds started and not released.
    private final long fetchAheadBytes;
    /// Whether the read starts speculative entries at all; not under a row limit.
    private final boolean speculates;
    /// Told, outside the lock, that an entry has started, which may let planning go further.
    private final Runnable onStarted;
    /// Registered with R while a speculative entry did not fit; one instance, so [#close] can cancel it.
    private final Runnable onRelease = this::onRelease;

    private final ReentrantLock lock = new ReentrantLock();
    /// Guarded by `lock`: the entries inserted and not yet started, in position order. An entry
    /// started on demand stays in the queue until the pump reaches it, which then drops it.
    private final PriorityQueue<Entry> queued = new PriorityQueue<>(
            Comparator.comparingInt(Entry::workItemIndex).thenComparingLong(Entry::offset));
    /// Guarded by `lock`: the entries of each work item, in position order, until it is forgotten.
    private final Map<Integer, List<Entry>> entries = new HashMap<>();
    /// Guarded by `lock`: the work items every column has released, which get no entries again.
    private final BitSet forgotten = new BitSet();
    /// Guarded by `lock`: the bytes of the speculative entries started and not released.
    private long speculativeBytes;
    /// Guarded by `lock`: the bytes of the entries inserted and not started.
    private long queuedBytes;
    /// Guarded by `lock`: the last work item with a started entry, `-1` before any.
    private int lastStarted = -1;
    /// Guarded by `lock`: the last work item whose first entry is the read's next unit.
    private int nextUnit = -1;
    /// Guarded by `lock`: the last work item a column has asked to plan, `-1` before the read
    /// starts, and the next one the planning chain plans ahead of the read.
    private int requested = -1;
    private int planFrom;
    /// Guarded by `lock`: whether [#onRelease] is registered with R.
    private boolean waitingForRoom;
    private boolean closed;

    /// @param tasks where the entries' reads run, which the read's owner waits for before it
    ///        closes the files
    /// @param readAhead the context's read-ahead buffer, R
    /// @param fetchAheadBytes F
    /// @param speculates whether speculative entries start; `false` under a row limit
    /// @param onStarted run outside the lock whenever an entry has started
    FetchSequence(PrefetchTasks tasks, ReadAheadBuffer readAhead, long fetchAheadBytes, boolean speculates,
                  Runnable onStarted) {
        this.tasks = tasks;
        this.readAhead = readAhead;
        this.fetchAheadBytes = fetchAheadBytes;
        this.speculates = speculates;
        this.onStarted = onStarted;
    }

    /// A sequence whose entries start on demand only, for plans built outside a read.
    static FetchSequence demandOnly(HardwoodContextImpl context) {
        return new FetchSequence(new PrefetchTasks(context.ioExecutor()), new ReadAheadBuffer(0), 0, false, () -> {});
    }

    /// F for a read whose largest request is `maxSpanBytes`: twice that unless overridden through
    /// [#FETCH_AHEAD_BYTES_PROPERTY]. A value that is not a non-negative byte count is rejected
    /// rather than replaced by the default.
    static long fetchAheadBytes(int maxSpanBytes) {
        String override = System.getProperty(FETCH_AHEAD_BYTES_PROPERTY);
        if (override == null) {
            return 2L * maxSpanBytes;
        }
        long bytes;
        try {
            bytes = Long.parseLong(override);
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    FETCH_AHEAD_BYTES_PROPERTY + " must be a non-negative byte count, but was: " + override, e);
        }
        if (bytes < 0) {
            throw new IllegalArgumentException(
                    FETCH_AHEAD_BYTES_PROPERTY + " must be a non-negative byte count, but was: " + override);
        }
        return bytes;
    }

    /// A range of the file a planned row group reads as one entry: `length` bytes at `start`,
    /// requested under `reason`, held by `holders` columns until each has passed it.
    record Span(long start, int length, String reason, int holders) {
    }

    /// The entries of work item `workItemIndex` of `inputFile`, one per span in position order:
    /// those inserted before, if the work item has any, or new ones, which start by the rules
    /// above; none for a work item every column has released ([#forget]). For a file in memory,
    /// entries the sequence does not hold, sliced in place.
    List<Entry> insert(int workItemIndex, InputFile inputFile, List<Span> spans) {
        if (RangeRead.inMemory(inputFile)) {
            List<Entry> inPlace = new ArrayList<>(spans.size());
            for (Span span : spans) {
                inPlace.add(new Entry(null, inputFile, span, workItemIndex));
            }
            return inPlace;
        }
        List<Entry> inserted;
        RangeRead first = null;
        lock.lock();
        try {
            List<Entry> existing = entries.get(workItemIndex);
            if (existing != null) {
                return existing;
            }
            if (forgotten.get(workItemIndex)) {
                // Planning ahead that the read has passed
                return List.of();
            }
            inserted = new ArrayList<>(spans.size());
            for (Span span : spans) {
                Entry entry = new Entry(this, inputFile, span, workItemIndex);
                inserted.add(entry);
                if (closed) {
                    entry.released = true;
                }
                else {
                    entry.queued = true;
                    queued.add(entry);
                    queuedBytes += entry.length;
                }
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
        if (RangeRead.inMemory(inputFile)) {
            return new Entry(null, inputFile, span, workItemIndex);
        }
        Entry entry = new Entry(this, inputFile, span, workItemIndex);
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

    /// The entries inserted for work item `workItemIndex`, `null` when it has none.
    List<Entry> entries(int workItemIndex) {
        lock.lock();
        try {
            return entries.get(workItemIndex);
        }
        finally {
            lock.unlock();
        }
    }

    /// Records that the first column has entered the work item before `workItemIndex`, so the
    /// first entry of `workItemIndex` is the read's next unit, and starts it if it is inserted and
    /// not started.
    void nextUnit(int workItemIndex) {
        RangeRead first;
        lock.lock();
        try {
            if (closed) {
                return;
            }
            nextUnit = Math.max(nextUnit, workItemIndex);
            first = startFirst(workItemIndex);
        }
        finally {
            lock.unlock();
        }
        started(first);
    }

    /// Records that a column asks to plan work item `workItemIndex`: its first entry starts as the
    /// next unit's does, since the column waits for the planning that inserts it.
    void requested(int workItemIndex) {
        lock.lock();
        try {
            if (workItemIndex <= requested) {
                return;
            }
            requested = workItemIndex;
        }
        finally {
            lock.unlock();
        }
        nextUnit(workItemIndex);
    }

    /// The next work item the planning chain may plan ahead of the read, `-1` when it may plan none
    /// now: none before the read starts, then the next unit whatever F holds, and further, for a read
    /// that plans `deep`, while the bytes of the entries not started are below F and the work item
    /// is at most one past the last with a started entry, which bounds the dictionaries and index
    /// windows held for planned row groups. A work item a column asked for is its own planning's.
    int nextToPlan(boolean deep) {
        lock.lock();
        try {
            if (requested < 0) {
                return -1;
            }
            planFrom = Math.max(planFrom, requested + 1);
            if (planFrom <= nextUnit
                    || deep && queuedBytes < fetchAheadBytes && planFrom <= lastStarted + 1) {
                return planFrom;
            }
            return -1;
        }
        finally {
            lock.unlock();
        }
    }

    /// Records that the planning chain has planned work item `workItemIndex`.
    void planned(int workItemIndex) {
        lock.lock();
        try {
            planFrom = Math.max(planFrom, workItemIndex + 1);
        }
        finally {
            lock.unlock();
        }
    }

    /// Starts, uncharged, the first entry of `workItemIndex` if it is not started. Called under `lock`.
    ///
    /// @return the read to submit, `null` when there is none
    private RangeRead startFirst(int workItemIndex) {
        List<Entry> ofWorkItem = entries.get(workItemIndex);
        if (ofWorkItem == null || ofWorkItem.isEmpty()) {
            return null;
        }
        Entry entry = ofWorkItem.getFirst();
        return entry.read == null && !entry.released ? begin(entry, true) : null;
    }

    /// The read of `entry`'s bytes: the one under way or done, or one started as demand when none
    /// is under way or a speculative one failed.
    ///
    /// @throws IllegalStateException if every column holding the entry has passed it
    RangeRead demand(Entry entry) {
        RangeRead current = entry.read;
        if (current != null && !current.failedSpeculatively()) {
            return current;
        }
        RangeRead read;
        lock.lock();
        try {
            if (entry.released) {
                throw new IllegalStateException("Entry at offset " + entry.offset + " (length " + entry.length
                        + ") of " + entry.inputFile.name() + " was released before it was read");
            }
            current = entry.read;
            if (current != null && !current.failedSpeculatively()) {
                return current;
            }
            read = begin(entry, false);
        }
        finally {
            lock.unlock();
        }
        started(read);
        return read;
    }

    /// Starts every speculative entry that fits, in position order, and registers for a release of
    /// R when the next one does not fit R.
    void pump() {
        if (!speculates) {
            return;
        }
        List<RangeRead> reads = new ArrayList<>();
        boolean register = false;
        long releasesSeen;
        lock.lock();
        try {
            if (closed) {
                return;
            }
            releasesSeen = readAhead.releases();
            Entry next;
            while ((next = queued.peek()) != null) {
                if (!next.queued) {
                    // Started on demand, or released, since it was queued
                    queued.poll();
                    continue;
                }
                if (speculativeBytes + next.length > fetchAheadBytes) {
                    break;
                }
                if (!readAhead.tryCharge(next.length)) {
                    register = !waitingForRoom;
                    waitingForRoom = true;
                    break;
                }
                queued.poll();
                next.charged = true;
                speculativeBytes += next.length;
                reads.add(begin(next, true));
            }
        }
        finally {
            lock.unlock();
        }
        for (RangeRead read : reads) {
            read.submit();
        }
        if (register) {
            readAhead.whenReleased(releasesSeen, onRelease);
        }
        if (!reads.isEmpty()) {
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

    /// Releases the entries of work item `workItemIndex` that columns still hold, once every
    /// column has released the work item, and forgets them.
    void forget(int workItemIndex) {
        long uncharge = 0;
        lock.lock();
        try {
            forgotten.set(workItemIndex);
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

    /// Drops every entry not started and releases every charge. The reads in flight are left to the
    /// read's [PrefetchTasks], which its owner waits for next.
    void close() {
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

    /// Starts `entry`'s read; one started `aheadOfDemand`, a speculative or next-unit read, fails
    /// without failing the read, and a step that needs its bytes reads them again. Called under
    /// `lock`, which the caller has charged the entry under if it is speculative.
    ///
    /// @return the read, for the caller to submit once it has let go of the lock
    private RangeRead begin(Entry entry, boolean aheadOfDemand) {
        if (entry.queued) {
            entry.queued = false;
            queuedBytes -= entry.length;
        }
        RangeRead read = new RangeRead(entry.inputFile, entry.offset, entry.length, entry.reason, aheadOfDemand,
                tasks);
        entry.read = read;
        lastStarted = Math.max(lastStarted, entry.workItemIndex);
        read.whenDone(() -> completed(entry, read));
        return read;
    }

    /// Submits `read`, if any, and tells planning an entry has started. Called outside `lock`.
    private void started(RangeRead read) {
        if (read != null) {
            read.submit();
            onStarted.run();
        }
    }

    /// Drops `entry`'s charges once its read started ahead of demand has failed, so that it is
    /// started again only on demand.
    private void completed(Entry entry, RangeRead read) {
        if (!read.failedSpeculatively()) {
            return;
        }
        long uncharge = 0;
        lock.lock();
        try {
            if (entry.read == read && entry.charged) {
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

    /// Marks `entry` released and drops its bytes. Called under `lock`.
    ///
    /// @return the bytes of R the entry held, for the caller to release once it has let go of the lock
    private long drop(Entry entry) {
        entry.released = true;
        entry.read = null;
        if (entry.queued) {
            entry.queued = false;
            queuedBytes -= entry.length;
        }
        if (!entry.charged) {
            return 0;
        }
        entry.charged = false;
        speculativeBytes -= entry.length;
        return entry.length;
    }

    /// Returns `bytes` to R and starts what then fits. Called outside `lock`.
    private void uncharge(long bytes) {
        if (bytes > 0) {
            readAhead.release(bytes);
            pump();
        }
    }

    /// One data request of a read: a range of the file the needed ranges of one or more columns of
    /// a row group were coalesced into. A walk asks for its bytes with [#poll], which never waits
    /// and issues no read; while they are not present the entry is the [ByteNeed] the step issues
    /// ([#issue]), which starts it on demand or joins the read under way.
    static final class Entry implements ByteNeed {

        /// `null` for an entry of a file in memory, which no sequence holds.
        private final FetchSequence sequence;
        private final InputFile inputFile;
        private final long offset;
        private final int length;
        private final int workItemIndex;
        private final String reason;
        /// Guarded by the sequence's lock: the columns that have not passed the entry.
        private int holders;
        /// Guarded by the sequence's lock: whether the entry holds a speculative charge.
        private boolean charged;
        /// Guarded by the sequence's lock: whether the entry waits in the queue.
        private boolean queued;
        /// Guarded by the sequence's lock: whether every holder has passed the entry.
        private boolean released;
        /// The read under way or done; `null` before it starts and once released. Replaced only to
        /// repeat a speculative read that failed.
        private volatile RangeRead read;
        /// The bytes of an entry of a file in memory, sliced on first poll.
        private volatile ByteBuffer inPlace;

        private Entry(FetchSequence sequence, InputFile inputFile, Span span, int workItemIndex) {
            this.sequence = sequence;
            this.inputFile = inputFile;
            this.offset = span.start();
            this.length = span.length();
            this.workItemIndex = workItemIndex;
            this.reason = span.reason();
            this.holders = span.holders();
        }

        long offset() {
            return offset;
        }

        int length() {
            return length;
        }

        long end() {
            return offset + length;
        }

        int workItemIndex() {
            return workItemIndex;
        }

        String reason() {
            return reason;
        }

        /// The entry's bytes, or `null` while they are not present, without waiting and without
        /// issuing a read, except for a file whose bytes are in memory, which it slices in place.
        ///
        /// @throws IOException if the demand read failed
        ByteBuffer poll() throws IOException {
            if (sequence == null) {
                ByteBuffer bytes = inPlace;
                if (bytes == null) {
                    // A race slices twice, which costs nothing on a file in memory
                    bytes = inputFile.readRange(offset, length);
                    inPlace = bytes;
                }
                return bytes;
            }
            RangeRead current = read;
            if (current == null || !current.isDone() || current.failedSpeculatively()) {
                return null;
            }
            return current.bytes();
        }

        /// A slice of `length` bytes at absolute `fileOffset` from bytes [#poll] returned.
        ///
        /// @throws IllegalArgumentException if the range lies outside the entry
        ByteBuffer slice(ByteBuffer polled, long fileOffset, int sliceLength) {
            if (fileOffset < offset || sliceLength < 0 || fileOffset + sliceLength > end()) {
                throw new IllegalArgumentException("Range [" + fileOffset + ", " + (fileOffset + sliceLength)
                        + ") lies outside the entry [" + offset + ", " + end() + ") (" + reason + ")");
            }
            return polled.slice(Math.toIntExact(fileOffset - offset), sliceLength);
        }

        @Override
        public RangeRead issue() {
            if (sequence == null) {
                throw new IllegalStateException("An entry of a file in memory is sliced in place, never read");
            }
            return sequence.demand(this);
        }

        /// Records that a column holding the entry has passed it.
        void release() {
            if (sequence != null) {
                sequence.release(this);
            }
        }

        @Override
        public String toString() {
            return "Entry[" + reason + " at " + offset + " (length " + length + ")]";
        }
    }
}
