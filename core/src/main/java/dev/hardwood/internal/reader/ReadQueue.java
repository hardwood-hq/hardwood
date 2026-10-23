/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.RejectedExecutionException;
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
/// A thread that finds nothing to run parks on the read's [ReadTasks] until it signals, which the
/// queue does whenever an item is queued or ends.
public final class ReadQueue implements ReadExecutor {

    private final HardwoodContextImpl context;
    private final ReadTasks tasks = new ReadTasks();
    private final ConcurrentLinkedDeque<Runnable> items = new ConcurrentLinkedDeque<>();

    /// @param context the context without a pool the read runs on
    ReadQueue(HardwoodContextImpl context) {
        this.context = context;
    }

    @Override
    public ReadTasks tasks() {
        return tasks;
    }

    /// Queues a fetch step, at the head.
    ///
    /// @throws RejectedExecutionException if the context is closed, as its pool would refuse it
    @Override
    public void executeStep(Runnable step) {
        checkOpen();
        items.addFirst(step);
        tasks.signal();
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
    boolean runOne() {
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

    /// Wakes every thread waiting for the read, after one of its exchanges changed.
    void signal() {
        tasks.signal();
    }
}
