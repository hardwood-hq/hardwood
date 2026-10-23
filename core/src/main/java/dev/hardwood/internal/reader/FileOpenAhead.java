/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/// Opens the files of a multi-file read ahead of the file the read has reached: up to
/// [#maxFilesAhead] files after it have their footers read, so the read's planning chain can plan
/// them ahead of it and their data enters the read's [FetchSequence].
///
/// The file after the one reached is the next file, which a read opens ahead in any case: its
/// footer read is not charged. Every file further ahead is opened only while its footer fits the
/// context's [ReadAheadBuffer]: its footer read is charged a fixed [#FOOTER_RESERVATION] from its
/// start until the file is planned, which parses the footer. A footer that does not fit is started
/// on a later release of the buffer, if its file is then still ahead. Nothing waits for the
/// buffer.
///
/// Opening stops for good once planning a file has failed, since no file after it is planned
/// ([#stop]), which releases what the files past the failed one hold.
///
/// [#pump] does the opening and runs on whichever thread changes what may be opened: the read
/// reaching a file ([#reached]), a file being planned ([#planned]) and a release of the buffer,
/// which may be another read's. It never waits: a footer read runs on a virtual thread
/// ([FileMetadataCache#prefetch]), and a call while a thread pumps, that thread included, makes
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

    /// What a footer read ahead is charged: the tail an S3 open fetches. The footer's size is not
    /// known before its read, which [#maxFilesAhead] bounds in number.
    static final long FOOTER_RESERVATION = 64 * 1024;

    private final int maxFilesAhead;
    private final int fileCount;
    private final FileMetadataCache files;
    private final ReadAheadBuffer buffer;
    /// Tells the read's planning chain a footer has been read.
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
    /// Guarded by `pumpLock`: what the pump under way releases to the buffer once it has unlocked.
    private long toRelease;
    /// Set once the read plans no further file.
    private volatile boolean stopped;
    private volatile boolean closed;

    /// @param planAhead tells the read's planning chain a footer has been read; called on the
    ///        thread that completes a footer read, so it hands the planning to a carrier of its own
    FileOpenAhead(int maxFilesAhead, int fileCount, FileMetadataCache files, ReadAheadBuffer buffer,
                  Runnable planAhead) {
        this.maxFilesAhead = maxFilesAhead;
        this.fileCount = fileCount;
        this.files = files;
        this.buffer = buffer;
        this.planAhead = planAhead;
        this.nextToOpen = 1;
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
    /// the footers read ahead.
    void stop() {
        stopped = true;
        release(dropPlanned());
    }

    /// Stops opening files and releases every charge still held.
    void close() {
        closed = true;
        release(dropPlanned());
        buffer.cancel(onRelease);
    }

    /// Drops the footer reservations.
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

    /// Starts the footer reads that fit, in file order. Called under `pumpLock`.
    private void open() {
        long releasesSeen = buffer.releases();
        boolean blocked = openFooters();
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
        int last = Math.min(reached.get() + maxFilesAhead, fileCount - 1);
        while (nextToOpen <= last) {
            int fileIndex = nextToOpen;
            if (fileIndex <= reached.get()) {
                // Reached, and so loaded by the read itself
                nextToOpen++;
                continue;
            }
            boolean charged = beyondNext(fileIndex);
            if (charged && !buffer.tryCharge(FOOTER_RESERVATION)) {
                return true;
            }
            nextToOpen++;
            boolean started = files.prefetch(fileIndex, planAhead);
            if (charged) {
                if (started) {
                    footerCharges.put(fileIndex, FOOTER_RESERVATION);
                }
                else {
                    toRelease += FOOTER_RESERVATION;
                }
            }
        }
        return false;
    }
}
