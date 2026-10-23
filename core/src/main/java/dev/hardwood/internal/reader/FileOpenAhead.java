/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import dev.hardwood.InputFile;
import dev.hardwood.internal.FetchReason;

/// Opens the files of a multi-file read ahead of the file the read has reached: up to
/// [#maxFilesAhead] files after it have their footers read, and a file planned ahead has the first
/// data request of its first row group started, a span its [FetchedAhead] holds ([#span]).
///
/// The file after the one reached is the next file, which a read opens ahead in any case: its
/// footer read and its first data request are not charged. Every file further ahead is opened only
/// while what it fetches fits the context's [ReadAheadBuffer]: its footer read is charged from its
/// start until the file is planned, which parses the footer, and its span its length from its
/// start until a read takes from it. A footer read is charged the tail a [TailHolder] fetches
/// ([TailHolder#tailReservation]), and a fixed [#FOOTER_RESERVATION] for any other file. A footer
/// or span that does not fit is started on a later release of the buffer, if its file is then
/// still ahead. Nothing waits for the buffer.
///
/// Opening stops for good once planning a file has failed, since no file after it is planned
/// ([#stop]), which releases what the files past the failed one hold.
///
/// [#pump] does the opening and runs on whichever thread changes what may be opened: the read
/// reaching a file ([#reached]), a file being planned ([#planned]), a span planned ([#span]) and a
/// release of the buffer, which may be another read's. It never waits: a footer read and a span
/// run on the read's [PrefetchTasks], and a call while a thread pumps, that thread included, makes
/// it pump again. Nothing is released to the buffer under the pump's lock: a release runs the
/// waiters of every read on the context, on the releasing thread, and a waiter that took the lock
/// of a read whose own release waits for this one's would deadlock the two.
final class FileOpenAhead {

    static final String MAX_FILES_AHEAD_PROPERTY = "hardwood.internal.maxFilesAhead";

    /// The files a read opens ahead without the override. A bound on the requests a read has in
    /// flight for files it has not reached, not a tuning value: the buffer bounds the bytes, and
    /// stops the open-ahead well before this on most data sets, but a run of files statistics
    /// prune holds no charge once planned, and small files fit the buffer by the thousand.
    static final int DEFAULT_MAX_FILES_AHEAD = 32;

    /// What a footer read ahead of a file that holds no tail is charged. The footer's size is not
    /// known before its read, which [#maxFilesAhead] bounds in number.
    static final long FOOTER_RESERVATION = 64 * 1024;

    private final int maxFilesAhead;
    private final int fileCount;
    private final FileMetadataCache files;
    private final ReadAheadBuffer buffer;
    private final FetchedAhead fetchedAhead;
    private final PrefetchTasks tasks;
    /// Plans the files whose footers have been read, in order, on the read's planning carrier.
    private final Runnable planAhead;

    private final ReentrantLock pumpLock = new ReentrantLock();
    private final AtomicBoolean pumpAgain = new AtomicBoolean();
    /// Set while a pump that found no room waits for a release of the buffer.
    private final AtomicBoolean waitingForRoom = new AtomicBoolean();
    /// Registered with the buffer while a pump found no room; one instance, so [#close] can cancel it.
    private final Runnable onRelease = this::onRelease;
    /// The last file the read has reached: whose planning the read itself asked for.
    private final AtomicInteger reached = new AtomicInteger();
    /// Guarded by `pumpLock`: the next file whose footer is to be read.
    private int nextToOpen;
    /// Guarded by `pumpLock`: the reservation of each charged footer read, until its file is planned.
    private final Map<Integer, Long> footerCharges = new HashMap<>();
    /// Guarded by `pumpLock`: the spans planned and not yet started, in file order.
    private final Deque<PlannedSpan> plannedSpans = new ArrayDeque<>();
    /// Guarded by `pumpLock`: what the pump under way releases to the buffer once it has unlocked.
    private long toRelease;
    /// Set once the read plans no further file.
    private volatile boolean stopped;
    private volatile boolean closed;

    /// The first data request of a file planned ahead: `length` bytes at `offset` of `inputFile`,
    /// covering the row group of work item `workItemIndex`.
    record PlannedSpan(int fileIndex, InputFile inputFile, int workItemIndex, int rowGroupIndex,
                       long offset, int length) {
    }

    /// @param planAhead runs the planning of the files whose footers have been read; called on the
    ///        thread that completes a footer read, so it hands the planning to a carrier of its own
    FileOpenAhead(int maxFilesAhead, int fileCount, FileMetadataCache files, ReadAheadBuffer buffer,
                  PrefetchTasks tasks, Runnable planAhead) {
        this.maxFilesAhead = maxFilesAhead;
        this.fileCount = fileCount;
        this.files = files;
        this.buffer = buffer;
        this.tasks = tasks;
        this.planAhead = planAhead;
        this.fetchedAhead = new FetchedAhead(buffer);
        this.nextToOpen = 1;
        tasks.serveFrom(fetchedAhead);
    }

    /// The files a read opens ahead: [#DEFAULT_MAX_FILES_AHEAD] unless overridden through
    /// [#MAX_FILES_AHEAD_PROPERTY], read for each read. A value that is not a positive count is
    /// rejected rather than replaced by the default; `1` opens only the next file ahead.
    static int maxFilesAhead() {
        String override = System.getProperty(MAX_FILES_AHEAD_PROPERTY);
        if (override == null) {
            return DEFAULT_MAX_FILES_AHEAD;
        }
        int count;
        try {
            count = Integer.parseInt(override);
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    MAX_FILES_AHEAD_PROPERTY + " must be a positive count, but was: " + override, e);
        }
        if (count < 1) {
            throw new IllegalArgumentException(
                    MAX_FILES_AHEAD_PROPERTY + " must be a positive count, but was: " + override);
        }
        return count;
    }

    /// The last file the planning ahead may plan.
    int horizon() {
        return reached.get() + maxFilesAhead;
    }

    /// Whether `fileIndex` is past the next file, so what it fetches ahead is charged.
    boolean beyondNext(int fileIndex) {
        return fileIndex > reached.get() + 1;
    }

    /// Records that the read has reached `fileIndex`, and opens the files the move brings within
    /// reach.
    void reached(int fileIndex) {
        if (reached.getAndAccumulate(fileIndex, Math::max) < fileIndex) {
            pump();
        }
    }

    /// Records that `fileIndex` has been planned, which parsed its footer, releasing the footer's
    /// reservation.
    void planned(int fileIndex) {
        pumpLock.lock();
        Long charge;
        try {
            charge = footerCharges.remove(fileIndex);
        }
        finally {
            pumpLock.unlock();
        }
        if (charge != null) {
            buffer.release(charge);
        }
    }

    /// What the footer read of `fileIndex` is charged: for a [TailHolder] the tail its open
    /// fetches.
    private long footerReservation(int fileIndex) {
        return files.inputFiles().get(fileIndex) instanceof TailHolder holder
                ? holder.tailReservation(files.footersSupplied())
                : FOOTER_RESERVATION;
    }

    /// Queues the first data request of a file planned ahead, which starts once it fits the
    /// buffer, if its file is then still past the next one.
    void span(PlannedSpan span) {
        pumpLock.lock();
        try {
            plannedSpans.addLast(span);
        }
        finally {
            pumpLock.unlock();
        }
        pump();
    }

    /// Drops what was fetched ahead for work item `workItemIndex`, which every column has released.
    void released(int workItemIndex) {
        fetchedAhead.drop(workItemIndex);
    }

    /// Opens what may be opened, on the calling thread, without waiting; see the class comment.
    void pump() {
        pumpAgain.set(true);
        if (pumpLock.isHeldByCurrentThread()) {
            // Called back from within the pump under way on this thread, which pumps again
            return;
        }
        while (pumpAgain.get() && pumpLock.tryLock()) {
            long release;
            try {
                pumpAgain.set(false);
                if (!closed && !stopped) {
                    open();
                }
                release = toRelease;
                toRelease = 0;
            }
            finally {
                pumpLock.unlock();
            }
            release(release);
        }
    }

    /// Stops opening files, since the read plans no further one, and releases the reservations of
    /// the footers read ahead and drops the spans not yet started; a span started stays held until
    /// released or closed.
    void stop() {
        stopped = true;
        release(dropPlanned());
    }

    /// Stops opening files and releases every charge still held.
    void close() {
        closed = true;
        release(dropPlanned());
        buffer.cancel(onRelease);
        fetchedAhead.close();
    }

    /// Drops the footer reservations and the spans not yet started.
    ///
    /// @return the bytes the reservations held
    private long dropPlanned() {
        pumpLock.lock();
        try {
            long held = 0;
            for (long charge : footerCharges.values()) {
                held += charge;
            }
            footerCharges.clear();
            plannedSpans.clear();
            return held;
        }
        finally {
            pumpLock.unlock();
        }
    }

    private void release(long bytes) {
        if (bytes > 0) {
            buffer.release(bytes);
        }
    }

    /// Starts the spans, then the footer reads, that fit, each in file order: a span's file is
    /// nearer than any whose footer is still to be read. Called under `pumpLock`.
    private void open() {
        long releasesSeen = buffer.releases();
        boolean blocked = startSpans() || openFooters();
        if (blocked && waitingForRoom.compareAndSet(false, true)) {
            buffer.whenReleased(releasesSeen, onRelease);
        }
    }

    /// Run by the buffer on the thread releasing it, perhaps another read's: takes no lock.
    private void onRelease() {
        waitingForRoom.set(false);
        pump();
    }

    /// Starts the footer reads within reach.
    ///
    /// @return whether the buffer had no room for one
    private boolean openFooters() {
        int last = Math.min(horizon(), fileCount - 1);
        while (nextToOpen <= last) {
            int fileIndex = nextToOpen;
            if (fileIndex <= reached.get()) {
                // Reached, and so loaded by the read itself
                nextToOpen++;
                continue;
            }
            boolean charged = beyondNext(fileIndex);
            long reservation = charged ? footerReservation(fileIndex) : 0;
            if (charged && !buffer.tryCharge(reservation)) {
                return true;
            }
            nextToOpen++;
            boolean started = files.prefetch(fileIndex, planAhead);
            if (charged) {
                if (started) {
                    footerCharges.put(fileIndex, reservation);
                }
                else {
                    toRelease += reservation;
                }
            }
        }
        return false;
    }

    /// Starts the planned spans of files still past the next one.
    ///
    /// @return whether the buffer had no room for one
    private boolean startSpans() {
        while (!plannedSpans.isEmpty()) {
            PlannedSpan span = plannedSpans.peekFirst();
            if (!beyondNext(span.fileIndex())) {
                // The read's own prefetch of its next row group reads it now
                plannedSpans.removeFirst();
                continue;
            }
            if (!buffer.tryCharge(span.length())) {
                return true;
            }
            plannedSpans.removeFirst();
            RangeRead read;
            try (FetchReason.Scope ignored = FetchReason.set("open-ahead file=" + span.fileIndex())) {
                read = RangeRead.start(span.inputFile(), span.offset(), span.length(),
                        "rg=" + span.rowGroupIndex() + " span", RangeRead.Priority.SPECULATIVE, tasks);
            }
            if (!fetchedAhead.add(span.inputFile(), span.workItemIndex(), read)) {
                toRelease += span.length();
            }
        }
        return false;
    }
}
