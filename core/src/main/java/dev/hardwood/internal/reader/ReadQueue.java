/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import dev.hardwood.internal.DecodeBuffers;
import dev.hardwood.internal.DecodeExecutor;

/// The fetch steps, page tasks and planning of one read on a context without a pool
/// ([HardwoodContextImpl#hasPool()]), which the threads using the read run: its consumer, when a
/// poll finds no batch ([BatchExchange#poll()]), and the thread closing it ([ScheduledColumn#close()],
/// [PrefetchTasks#awaitAll()]).
///
/// The queue keeps the order of the pool's queue ([DecodeExecutor]): a fetch step goes to the head,
/// a page task and planning to the tail. Its items are the ones the pool runs, which are safe to run
/// on several threads at once, so a close on one thread and a poll on another may both run them.
///
/// A thread that finds nothing to run parks until the queue signals ([#signal()]): an item was
/// queued, such as a fetch step the completion of its read queues again; an item ended; or one of
/// the read's columns published, finished or failed. A thread reads the count of signals before it
/// looks, and parks only while the count is unchanged, so no signal is lost between its look and
/// its park.
///
/// The queue counts its outstanding work: the items queued or running, and the fetch steps that
/// yielded on a read in flight ([#yielded()]). With none outstanding, nothing changes what a poll
/// looks at any more.
public final class ReadQueue implements ReadExecutor {

    private final HardwoodContextImpl context;
    private final ConcurrentLinkedDeque<Runnable> items = new ConcurrentLinkedDeque<>();
    private final AtomicInteger outstanding = new AtomicInteger();
    private final AtomicLong signals = new AtomicLong();
    private final Set<Thread> parked = ConcurrentHashMap.newKeySet();

    /// @param context the context without a pool the read runs on
    ReadQueue(HardwoodContextImpl context) {
        this.context = context;
    }

    /// Queues a fetch step, at the head.
    ///
    /// @throws RejectedExecutionException if the context is closed, as its pool would refuse it
    @Override
    public void executeStep(Runnable step) {
        checkOpen();
        outstanding.incrementAndGet();
        items.addFirst(step);
        signal();
    }

    /// Queues a page task, at the tail, to run with a set of the context's decode buffers on the
    /// thread that takes it.
    ///
    /// @throws RejectedExecutionException if the context is closed, as its pool would refuse it
    @Override
    public void executePage(Consumer<DecodeBuffers> task) {
        addLast(() -> context.runInPlace(task));
    }

    /// One: the thread driving the read runs its pages, whichever threads run the queue.
    @Override
    public int decodeThreads() {
        return 1;
    }

    /// Queues planning, at the tail.
    ///
    /// @throws RejectedExecutionException if the context is closed, as its pool would refuse it
    @Override
    public void executePlanning(Runnable task) {
        addLast(task);
    }

    private void addLast(Runnable task) {
        checkOpen();
        outstanding.incrementAndGet();
        items.addLast(task);
        signal();
    }

    private void checkOpen() {
        if (context.isClosed()) {
            throw new RejectedExecutionException("The HardwoodContext is closed");
        }
    }

    /// Runs the item at the head on the calling thread, if there is one.
    ///
    /// @return whether an item ran
    boolean runOne() {
        Runnable item = items.pollFirst();
        if (item == null) {
            return false;
        }
        try {
            item.run();
        }
        finally {
            outstanding.decrementAndGet();
            signal();
        }
        return true;
    }

    /// Counts a fetch step that yielded on a read in flight as outstanding, until [#resumed()].
    @Override
    public void yielded() {
        outstanding.incrementAndGet();
    }

    /// Ends the count of a yielded fetch step, after the completion of its read queued the step
    /// again or failed to.
    @Override
    public void resumed() {
        outstanding.decrementAndGet();
        signal();
    }

    /// Whether no item is queued or running and no fetch step waits for a read.
    boolean idle() {
        return outstanding.get() == 0;
    }

    /// The count of signals so far, read before a thread looks for work and passed to [#park].
    long signals() {
        return signals.get();
    }

    /// Wakes every thread parked on the queue.
    void signal() {
        signals.incrementAndGet();
        if (!parked.isEmpty()) {
            for (Thread thread : parked) {
                LockSupport.unpark(thread);
            }
        }
    }

    /// Parks the calling thread unless a signal came after `seen`. Returns on a signal, an unpark
    /// or an interrupt, and may return for no reason, so the caller looks again.
    void park(long seen) {
        Thread current = Thread.currentThread();
        parked.add(current);
        try {
            if (signals.get() == seen) {
                LockSupport.park(this);
            }
        }
        finally {
            parked.remove(current);
        }
    }

    /// Runs the queue on the calling thread until `done` holds, parking while nothing is queued.
    /// Carries on through interrupts and restores them: a close waits for work that reads the
    /// files it is about to release.
    void runUntil(BooleanSupplier done) {
        boolean interrupted = false;
        while (true) {
            long seen = signals();
            if (done.getAsBoolean()) {
                break;
            }
            if (runOne()) {
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
