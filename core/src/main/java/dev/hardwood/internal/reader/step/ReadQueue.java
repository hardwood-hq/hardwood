/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.step;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Consumer;

import dev.hardwood.internal.DecodeBuffers;
import dev.hardwood.internal.DecodeExecutor;
import dev.hardwood.internal.HardwoodContextImpl;

/// The CPU jobs of one read on a context without a pool
/// ([HardwoodContextImpl#hasPool()]), which the threads using the read run: its consumer, when a
/// poll finds no batch ([dev.hardwood.internal.reader.batch.BatchExchange#poll()]), and the thread
/// closing it ([ReadTasks#await]).
///
/// The queue runs its jobs in the order they arrive, as the pool's queue does ([DecodeExecutor]).
/// Its items are the ones the pool runs, which are safe to run on several threads at once, so a
/// close on one thread and a poll on another may both run them.
///
/// A thread that finds nothing to run parks on the read's [ReadTasks] until it signals, which the
/// queue does whenever an item is queued or ends.
public final class ReadQueue {

    private final HardwoodContextImpl context;
    private final ReadTasks tasks = new ReadTasks();
    private final ConcurrentLinkedDeque<Runnable> items = new ConcurrentLinkedDeque<>();

    /// @param context the context without a pool the read runs on
    public ReadQueue(HardwoodContextImpl context) {
        this.context = context;
    }

    /// The read's jobs in flight, and the wait of the threads using it.
    public ReadTasks tasks() {
        return tasks;
    }

    /// Queues a job, at the tail, to run with a set of the context's decode buffers on the
    /// thread that takes it.
    ///
    /// @throws RejectedExecutionException if the context is closed, as its pool would refuse it
    public void executePage(Consumer<DecodeBuffers> task) {
        addLast(() -> context.runInPlace(task));
    }

    private void addLast(Runnable task) {
        checkOpen();
        items.addLast(task);
        tasks.signal();
    }

    private void checkOpen() {
        if (context.isClosed()) {
            throw new RejectedExecutionException("The HardwoodContext is closed");
        }
    }

    /// Runs the item at the head on the calling thread, if there is one.
    ///
    /// @return whether an item ran
    public boolean runOne() {
        Runnable item = items.pollFirst();
        if (item == null) {
            return false;
        }
        try {
            item.run();
        }
        finally {
            tasks.signal();
        }
        return true;
    }

}
