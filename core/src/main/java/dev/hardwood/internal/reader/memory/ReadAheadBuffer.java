/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.memory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import dev.hardwood.internal.reader.step.Need;

/// The memory a context's reads hold by choice rather than because a file forces it, R: the buffers
/// of a read's tasks decoding beside the one its working set counts ([WorkingSet#admit]), the
/// speculative requests of a read ([dev.hardwood.internal.reader.engine.RowGroupIterator]), and
/// the footers a multi-file read reads for the files past the next one, each charged the tail it
/// reads until the read is done with the file.
///
/// Such a task, request or footer read starts only while it fits; one that does not fit is
/// not started and is considered again on a later release, whose waiters run in registration order.
/// Nothing waits for the buffer, so the reads of a context can share it: a read's own task, its
/// demanded requests, its next unit and its next file are never charged here, and every
/// charge is released by a task ending, a request every column has passed, a file being
/// planned, or the read closing, none of which depends on a read whose work was deferred.
public final class ReadAheadBuffer {

    public static final String READ_AHEAD_BYTES_PROPERTY = "hardwood.internal.readAheadBytes";

    /// The share of the maximum heap a context's read-ahead may take without the override.
    private static final int DEFAULT_HEAP_PERCENT = 25;

    private final long limit;
    private final AtomicLong held = new AtomicLong();
    /// Run once on the next release each; registered by what a charge that did not fit deferred.
    private final ConcurrentLinkedQueue<Runnable> waiters = new ConcurrentLinkedQueue<>();
    /// Counts the releases, so that a waiter registered after a release it raced with runs at once.
    private final AtomicLong releases = new AtomicLong();

    /// A buffer of [#readAheadBytes].
    public ReadAheadBuffer() {
        this(readAheadBytes());
    }

    public ReadAheadBuffer(long limit) {
        this.limit = limit;
    }

    /// The read-ahead buffer of a context: [#DEFAULT_HEAP_PERCENT] of the maximum heap unless
    /// overridden through [#READ_AHEAD_BYTES_PROPERTY]. A value that is not a non-negative byte
    /// count is rejected rather than replaced by the default.
    static long readAheadBytes() {
        return InternalProperty.longValue(READ_AHEAD_BYTES_PROPERTY,
                Runtime.getRuntime().maxMemory() / 100 * DEFAULT_HEAP_PERCENT, 0, "a non-negative byte count");
    }

    /// The releases so far, to hand to [#releaseAfter] or [#whenReleased] once a charge did not fit.
    public long releases() {
        return releases.get();
    }

    /// Charges `bytes` if they fit beside what is held.
    ///
    /// @return whether they were charged
    public boolean tryCharge(long bytes) {
        if (bytes == 0) {
            return true;
        }
        long current;
        do {
            current = held.get();
            if (current + bytes > limit) {
                return false;
            }
        }
        while (!held.compareAndSet(current, current + bytes));
        return true;
    }

    /// Releases `bytes` charged before, and runs what waited for a release.
    public void release(long bytes) {
        held.addAndGet(-bytes);
        wake();
    }

    /// Runs what waited for a release: a read's own task ended, which lets one of its tasks start
    /// without the buffer, or room was released, which lets a deferred task, footer read or span of
    /// any read start. A waiter runs on the releasing thread, so it takes no lock it could wait for.
    ///
    /// Runs the waiters registered before it only, every one of them even where one throws, and
    /// then rethrows the first failure. A waiter that finds no room again registers
    /// for the next release; were it run by this one too, it would retry for as long as it found
    /// no room.
    void wake() {
        releases.incrementAndGet();
        List<Runnable> woken = new ArrayList<>();
        Runnable waiter;
        while ((waiter = waiters.poll()) != null) {
            woken.add(waiter);
        }
        // Every waiter runs: one that throws must not leave another read's waiting for ever
        Throwable failure = null;
        for (Runnable each : woken) {
            try {
                each.run();
            }
            catch (RuntimeException | Error e) {
                if (failure == null) {
                    failure = e;
                }
                else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure instanceof RuntimeException runtime) {
            throw runtime;
        }
        if (failure instanceof Error error) {
            throw error;
        }
    }

    /// Runs `resume` on the next release after the one `releasesSeen` counted, on the thread
    /// releasing; at once on the caller's if one has come since.
    public void whenReleased(long releasesSeen, Runnable resume) {
        waiters.add(resume);
        if (releases.get() != releasesSeen && waiters.remove(resume)) {
            resume.run();
        }
    }

    /// The next release after the one `releasesSeen` counted, as a [Need]: met on the thread
    /// releasing, or at once on the caller's if one has come since ([#whenReleased]).
    Need releaseAfter(long releasesSeen) {
        return resume -> whenReleased(releasesSeen, resume);
    }

    /// Drops `resume` if it still waits, so that a waiter its owner no longer needs, such as one
    /// whose read has ended, does not keep the read reachable until the next release.
    public void cancel(Runnable resume) {
        waiters.remove(resume);
    }

    /// The bytes held, for tests.
    public long held() {
        return held.get();
    }
}
