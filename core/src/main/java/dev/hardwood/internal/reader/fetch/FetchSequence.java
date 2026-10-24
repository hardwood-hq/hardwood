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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;

import dev.hardwood.InputFile;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.io.ReadRanges;
import dev.hardwood.internal.reader.memory.InternalProperty;
import dev.hardwood.internal.reader.memory.ReadAheadBuffer;

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
/// mapping in place ([#insert]). So are the entries inside a file's tail while the read holds it
/// ([TailedInputFile#sliceTail]): they start no request and are charged to neither F nor R, since
/// the tail is charged already.
///
/// The sequence also starts the read's footer entries: the footer of the file after the one the
/// read has reached, uncharged, and of the files after that up to [#maxFilesAhead] files past the
/// one reached while the planning chain has room to plan them (the entries planned and not started
/// are below F), each charged to R at the tail a footer read takes ([TailedInputFile#tailBytes()])
/// from its start until the read is done with the file ([#filesDoneBefore]), which also drops the
/// file's tail. Footers are outside F, and start whether or not the read speculates. A footer that
/// does not fit R starts on a later release. The read's
/// [dev.hardwood.internal.reader.plan.FileMetadataCache] holds each footer entry, so another read
/// of the same reader may take it; one in flight when the read closes keeps its charge until it
/// completes.
///
/// One lock guards the entries and their positions. It is held across no I/O, no waiter and no
/// completion of a read: reads are submitted, waiters registered and charges released after it is
/// let go. A walk polls an entry without it.
public final class FetchSequence {

    public static final String FETCH_AHEAD_BYTES_PROPERTY = "hardwood.internal.fetchAheadBytes";

    public static final String MAX_FILES_AHEAD_PROPERTY = "hardwood.internal.maxFilesAhead";

    /// The files after the one a read has reached, the next one included, whose footers it reads
    /// ahead without the override, while its planning chain has room under F: a bound on the
    /// footer requests a read has in flight for files it has not reached. A read of many small
    /// files, whose data never fills F, is bound by round trips, and overlaps as many files as
    /// this lets it; a read of larger files fills F first and reads no footer past the next.
    static final int DEFAULT_MAX_FILES_AHEAD = 64;

    /// Where a read's footer entries are read and dropped: its
    /// [dev.hardwood.internal.reader.plan.FileMetadataCache].
    public interface Footers {

        /// Starts the footer read of `fileIndex` unless it has started or is read in place.
        ///
        /// @return the read started, which completes once it has, failed or not; `null` if none
        CompletableFuture<?> start(int fileIndex);

        /// Drops the tail and the footer of `fileIndex`, which the read is done with.
        void drop(int fileIndex);
    }

    /// The work item of an entry planning reads ([#plannedEntry]), which no work item holds.
    public static final int PLANNING = -1;

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
    /// The read's footer entries; `null` for a sequence that reads no footer.
    private final Footers footers;
    /// The files after the one the read has reached whose footers start ahead of it, `0` for none.
    private final int filesAhead;
    private final int fileCount;
    /// What each footer started past the next file is charged to R: the tail its read takes.
    private final long footerCharge;

    private final ReentrantLock lock = new ReentrantLock();
    /// Guarded by `lock`: the entries inserted and not yet started, in position order. An entry
    /// started on demand stays in the queue until the pump reaches it, which then drops it.
    private final PriorityQueue<Entry> queued = new PriorityQueue<>(
            Comparator.comparingInt(Entry::workItemIndex).thenComparingLong(Entry::offset));
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
    /// Guarded by `lock`: the last file the read has reached.
    private int reachedFile;
    /// Guarded by `lock`: the next file whose footer is to start.
    private int nextFooter = 1;
    /// Guarded by `lock`: the files before this one the read is done with.
    private int doneBefore;
    /// Guarded by `lock`: the footers started and charged to R, by file, with their reads.
    private final Map<Integer, CompletableFuture<?>> chargedFooters = new HashMap<>();
    /// Guarded by `lock`: whether footers start no more, since the read plans no further file.
    private boolean footersStopped;
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
        this(tasks, readAhead, fetchAheadBytes, speculates, onStarted, null, 0, 0);
    }

    /// @param footers where the read's footer entries are read and dropped
    /// @param filesAhead the files after the one the read has reached whose footers start ahead of
    ///        it, the next one included; `0` for none
    /// @param fileCount the read's files
    public FetchSequence(PrefetchTasks tasks, ReadAheadBuffer readAhead, long fetchAheadBytes, boolean speculates,
                  Runnable onStarted, Footers footers, int filesAhead, int fileCount) {
        this.tasks = tasks;
        this.readAhead = readAhead;
        this.fetchAheadBytes = fetchAheadBytes;
        this.speculates = speculates;
        this.onStarted = onStarted;
        this.footers = footers;
        this.filesAhead = filesAhead;
        this.fileCount = fileCount;
        this.footerCharge = footers != null ? TailedInputFile.tailBytes() : 0;
    }

    /// The files after the one it has reached a read reads footers of ahead: [#DEFAULT_MAX_FILES_AHEAD] unless
    /// overridden through [#MAX_FILES_AHEAD_PROPERTY], read for each read. A value that is not a
    /// positive count is rejected rather than replaced by the default; `1` reads only the next
    /// file's footer ahead.
    public static int maxFilesAhead() {
        return InternalProperty.intValue(MAX_FILES_AHEAD_PROPERTY, DEFAULT_MAX_FILES_AHEAD, 1, "a positive count");
    }

    /// A sequence whose entries start on demand only, for plans built outside a read.
    public static FetchSequence demandOnly(HardwoodContextImpl context) {
        return new FetchSequence(new PrefetchTasks(context.ioExecutor()), new ReadAheadBuffer(0), 0, false, () -> {});
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

    /// The entries of work item `workItemIndex` of `inputFile`, one per span in position order:
    /// those inserted before, if the work item has any, or new ones, which start by the rules
    /// above. For a file in memory, and for a span inside the file's tail, entries the sequence
    /// does not start, sliced in place.
    public List<Entry> insert(int workItemIndex, InputFile inputFile, List<Span> spans) {
        if (RangeRead.inMemory(inputFile)) {
            List<Entry> inMemory = new ArrayList<>(spans.size());
            for (Span span : spans) {
                inMemory.add(inPlace(workItemIndex, inputFile, span));
            }
            return inMemory;
        }
        List<Entry> inserted;
        RangeRead first = null;
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
                Entry entry = new Entry(this, inputFile, span, workItemIndex, null);
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
        Entry inPlace = inPlace(workItemIndex, inputFile, span);
        if (inPlace != null) {
            return inPlace;
        }
        Entry entry = new Entry(this, inputFile, span, workItemIndex, null);
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
        Entry entry = new Entry(this, inputFile, span, PLANNING, null);
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
        if (RangeRead.inMemory(inputFile)) {
            return new Entry(null, inputFile, span, workItemIndex, null);
        }
        ByteBuffer inTail = TailedInputFile.sliceTail(inputFile, span.start(), span.length());
        return inTail != null ? new Entry(null, inputFile, span, workItemIndex, inTail) : null;
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

    /// Records that work item `workItemIndex` is the read's next unit, since the first column has
    /// entered the one before it, or that a column asked to plan it and waits for the planning
    /// that inserts it; starts its first entry if it is inserted and not started.
    public void nextUnit(int workItemIndex) {
        RangeRead first;
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
    /// for planned row groups. A work item whose entries lie in a file's tail counts as started;
    /// the files the chain lists ahead are bounded by their footers instead ([#footerStarted]).
    public boolean mayPlanAhead(int workItemIndex) {
        lock.lock();
        try {
            return queuedBytes < fetchAheadBytes && workItemIndex <= lastStarted + 1;
        }
        finally {
            lock.unlock();
        }
    }

    /// Whether the read has started the footer of `fileIndex`, within the files ahead and charged
    /// to R ([#pump]), or reads no footers: the planning chain lists a file ahead of the read only
    /// then.
    public boolean footerStarted(int fileIndex) {
        if (footers == null) {
            return true;
        }
        lock.lock();
        try {
            return fileIndex < nextFooter;
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
    private RangeRead startFirst(int workItemIndex) {
        List<Entry> ofWorkItem = entries.get(workItemIndex);
        if (ofWorkItem == null) {
            return null;
        }
        for (Entry entry : ofWorkItem) {
            if (entry.sequence != null) {
                return entry.read == null && !entry.released ? begin(entry, true) : null;
            }
        }
        return null;
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

    /// Starts the footers within reach and every speculative entry that fits, in position order,
    /// and registers for a release of R when the next one does not fit R.
    public void pump() {
        if (!speculates && footers == null) {
            return;
        }
        List<RangeRead> reads = new ArrayList<>();
        List<Integer> footersToStart = new ArrayList<>();
        boolean register = false;
        long releasesSeen;
        lock.lock();
        try {
            if (closed) {
                return;
            }
            releasesSeen = readAhead.releases();
            boolean blocked = footers != null && startFooters(footersToStart);
            Entry next;
            while (speculates && !blocked && (next = queued.peek()) != null) {
                if (!next.queued) {
                    // Started on demand, or released, since it was queued
                    queued.poll();
                    continue;
                }
                if (speculativeBytes + next.length > fetchAheadBytes) {
                    break;
                }
                if (!readAhead.tryCharge(next.length)) {
                    blocked = true;
                    break;
                }
                queued.poll();
                next.charged = true;
                speculativeBytes += next.length;
                reads.add(begin(next, true));
            }
            if (!blocked && footers != null && !reads.isEmpty()) {
                // The entries started left the planning chain room for more files
                blocked = startFooters(footersToStart);
            }
            if (blocked) {
                register = !waitingForRoom;
                waitingForRoom = true;
            }
        }
        finally {
            lock.unlock();
        }
        for (int fileIndex : footersToStart) {
            startFooter(fileIndex);
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

    /// Records that the read has reached `fileIndex`, and starts the footers the move brings within
    /// reach.
    public void reachedFile(int fileIndex) {
        lock.lock();
        try {
            if (fileIndex <= reachedFile) {
                return;
            }
            reachedFile = fileIndex;
        }
        finally {
            lock.unlock();
        }
        pump();
    }

    /// Records that the read is done with every file before `fileIndex`: their tails are dropped
    /// and their footers' charges released.
    public void filesDoneBefore(int fileIndex) {
        if (footers == null) {
            return;
        }
        int from;
        long uncharge = 0;
        lock.lock();
        try {
            from = doneBefore;
            if (fileIndex <= from) {
                return;
            }
            doneBefore = fileIndex;
            for (int i = from; i < fileIndex; i++) {
                if (chargedFooters.remove(i) != null) {
                    uncharge += footerCharge;
                }
            }
        }
        finally {
            lock.unlock();
        }
        for (int i = from; i < fileIndex; i++) {
            footers.drop(i);
        }
        uncharge(uncharge);
    }

    /// Starts no further footer, since the read plans no further file, and releases the charges of
    /// the footers started ahead.
    public void stopFooters() {
        Map<Integer, CompletableFuture<?>> charged;
        lock.lock();
        try {
            footersStopped = true;
            charged = takeFooterCharges();
        }
        finally {
            lock.unlock();
        }
        releaseFooterCharges(charged);
    }

    /// The footers started and charged, by file, with their reads, which the caller releases. Called
    /// under `lock`.
    private Map<Integer, CompletableFuture<?>> takeFooterCharges() {
        Map<Integer, CompletableFuture<?>> charged = new HashMap<>(chargedFooters);
        chargedFooters.clear();
        return charged;
    }

    /// Releases the charge of each footer in `charged`, and drops its file's tail, once its read
    /// has completed, at once for one that has: the reader's cache shares the entry, so a footer in
    /// flight keeps its charge until it completes, whichever read started it. Called outside `lock`.
    private void releaseFooterCharges(Map<Integer, CompletableFuture<?>> charged) {
        charged.forEach((fileIndex, read) -> read.whenComplete((ignored, failure) -> {
            footers.drop(fileIndex);
            readAhead.release(footerCharge);
        }));
    }

    /// Adds to `toStart` the footers within reach, in file order, charging R for each past the next
    /// file, which starts only while the planning chain has room to plan it: the entries planned
    /// and not started are below F. Called under `lock`.
    ///
    /// @return whether R had no room for one
    private boolean startFooters(List<Integer> toStart) {
        if (footersStopped) {
            return false;
        }
        int last = Math.min(reachedFile + filesAhead, fileCount - 1);
        while (nextFooter <= last) {
            int fileIndex = nextFooter;
            if (fileIndex > reachedFile + 1) {
                if (queuedBytes >= fetchAheadBytes) {
                    // The planning chain has no room to plan the file, so its footer would wait
                    return false;
                }
                if (!readAhead.tryCharge(footerCharge)) {
                    return true;
                }
                chargedFooters.put(fileIndex, CompletableFuture.completedFuture(null));
            }
            nextFooter++;
            if (fileIndex > reachedFile) {
                toStart.add(fileIndex);
            }
        }
        return false;
    }

    /// Starts the footer read of `fileIndex`, outside `lock`, and releases its charge at once when
    /// it started none: its entry was read or loaded already, or is read in place.
    private void startFooter(int fileIndex) {
        CompletableFuture<?> read = footers.start(fileIndex);
        long uncharge = 0;
        lock.lock();
        try {
            if (chargedFooters.containsKey(fileIndex)) {
                if (read == null) {
                    chargedFooters.remove(fileIndex);
                    uncharge = footerCharge;
                }
                else {
                    chargedFooters.put(fileIndex, read);
                }
            }
        }
        finally {
            lock.unlock();
        }
        uncharge(uncharge);
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

    /// Releases `entry` whatever its holders.
    void discard(Entry entry) {
        long uncharge;
        lock.lock();
        try {
            if (entry.released) {
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

    /// Drops every entry not started and releases every charge. The reads in flight are left to the
    /// read's [PrefetchTasks], which its owner waits for next.
    public void close() {
        long uncharge = 0;
        Map<Integer, CompletableFuture<?>> charged;
        int footersUpTo;
        lock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            charged = takeFooterCharges();
            footersUpTo = nextFooter;
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
        releaseFooterCharges(charged);
        if (footers != null) {
            for (int i = 0; i < footersUpTo; i++) {
                footers.drop(i);
            }
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

    /// Returns `bytes` to R and starts what then fits, and for a read with footer entries the
    /// footers an entry dropped from the queue leaves the planning chain room for. Called outside
    /// `lock`.
    private void uncharge(long bytes) {
        if (bytes > 0) {
            readAhead.release(bytes);
        }
        if (bytes > 0 || footers != null) {
            pump();
        }
    }

    /// One data request of a read: a range of the file the needed ranges of one or more columns of
    /// a row group were coalesced into. A walk asks for its bytes with [#poll], which never waits
    /// and issues no read; while they are not present the entry is the [ByteNeed] the step issues
    /// ([#issue]), which starts it on demand or joins the read under way.
    public static final class Entry implements ByteNeed {

        /// `null` for an entry sliced in place, which no sequence holds.
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
        /// The bytes of an entry sliced in place: of a file in memory, sliced on first poll, or
        /// inside the file's tail, sliced as it is inserted.
        private volatile ByteBuffer inPlace;

        private Entry(FetchSequence sequence, InputFile inputFile, Span span, int workItemIndex,
                      ByteBuffer inPlace) {
            this.inPlace = inPlace;
            this.sequence = sequence;
            this.inputFile = inputFile;
            this.offset = span.start();
            this.length = span.length();
            this.workItemIndex = workItemIndex;
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
            RangeRead current = read;
            if (current == null || !current.isDone() || current.failedSpeculatively()) {
                return null;
            }
            return current.bytes();
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
        public RangeRead issue() {
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
