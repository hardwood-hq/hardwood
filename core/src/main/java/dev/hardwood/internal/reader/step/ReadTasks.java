/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.step;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

/// The work of one read that its close waits for, and the wait of every thread using the read.
///
/// It counts the read's jobs that are submitted and not finished ([#started()], [#ended()]). The read's close waits until
/// none is left, and a poll on a context of zero threads that finds none asks the read whether it
/// can still progress ([dev.hardwood.internal.reader.batch.BatchExchange#poll()]), which fails a
/// read nothing could wake any more. A task's end that ends its column, such as a step the closed
/// context refuses, therefore comes after the column's failure or end is published: a poll that
/// found the read idle first would find it stalled.
///
/// A thread that waits for the read parks until the read signals ([#signal()]): its last task
/// ended, and, on a context of zero threads, an item was queued on or ended on the read's queue,
/// or one of its columns published, finished or failed. A thread reads the count of signals
/// before it looks, and parks only while the count is unchanged, so no signal is lost between its
/// look and its park. Any number of threads may wait at once.
public final class ReadTasks {

    /// How long a thread waiting on a read parks before it looks again. A read abandoned with the
    /// heap exhausted may fail to allocate the wake-up of its waiting threads, and a wait that
    /// looks again ends all the same.
    public static final long ABANDONED_CHECK_MILLIS = 500;

    private final AtomicInteger count = new AtomicInteger();
    private final AtomicLong signals = new AtomicLong();
    private final Set<Thread> parked = ConcurrentHashMap.newKeySet();

    /// Counts a walk advance or page task submitted for the read.
    public void started() {
        count.incrementAndGet();
    }

    /// Ends the count of a walk advance or page task, and signals once none is left.
    public void ended() {
        if (count.decrementAndGet() == 0) {
            signal();
        }
    }

    /// The walk advances and page tasks submitted and not finished.
    public int count() {
        return count.get();
    }

    /// Whether no walk advance or page task of the read is submitted and not finished.
    public boolean idle() {
        return count.get() == 0;
    }

    /// The count of signals so far, read before a thread looks for work and passed to [#park].
    public long signals() {
        return signals.get();
    }

    /// Wakes every thread parked on the read.
    public void signal() {
        signals.incrementAndGet();
        if (!parked.isEmpty()) {
            for (Thread thread : parked) {
                LockSupport.unpark(thread);
            }
        }
    }

    /// Parks the calling thread unless a signal came after `seen`. Returns on a signal, an unpark,
    /// an interrupt or after [#ABANDONED_CHECK_MILLIS], and may return for no reason, so the caller
    /// looks again.
    public void park(long seen) {
        Thread current = Thread.currentThread();
        parked.add(current);
        try {
            if (signals.get() == seen) {
                LockSupport.parkNanos(this, TimeUnit.MILLISECONDS.toNanos(ABANDONED_CHECK_MILLIS));
            }
        }
        finally {
            parked.remove(current);
        }
    }

    /// Waits until `done` holds, running `queue`, the read's queue on a context of zero threads,
    /// on the calling thread meanwhile, and parking while nothing is queued; `queue` is `null` on
    /// a context with a pool. Carries on through interrupts and restores them: a close waits for
    /// work that reads the files it is about to release.
    public void await(BooleanSupplier done, ReadQueue queue) {
        boolean interrupted = false;
        while (true) {
            long seen = signals();
            if (done.getAsBoolean()) {
                break;
            }
            if (queue != null && queue.runOne()) {
                continue;
            }
            park(seen);
            // A park returns at once while the flag is set: cleared here and restored at the end
            interrupted |= Thread.interrupted();
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
