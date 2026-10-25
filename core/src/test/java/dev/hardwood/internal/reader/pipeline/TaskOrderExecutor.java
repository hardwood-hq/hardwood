/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.pipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import dev.hardwood.internal.HardwoodContextImpl;

/// A decode pool for tests that runs a read's jobs one at a time, in an order a seed decides: it
/// queues the jobs and runs them on a thread of its own, each time picking one of the queued jobs at
/// random from a seeded generator. Before it picks, it lets the read submit what it may: it waits
/// until as many jobs are queued as the read may have in flight, or until the thread that created
/// the executor, which reads, can submit no more before a job runs, being parked other than for
/// the executor's lock, or ended; the files a test reads are in memory, so no other thread submits.
/// The jobs of one column then run in any order, not only in page order.
///
/// Every job runs to its end before the next starts, so the order of the jobs is what varies: the
/// steps of the cell protocol that span jobs (which job decodes, measures, fills and completes a
/// cell) are taken in the order the seed gives, while the races inside one step are left to the
/// tests running real threads.
final class TaskOrderExecutor extends AbstractExecutorService {

    private final Random random;
    private final int target;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition arrived = lock.newCondition();
    private final List<Runnable> queued = new ArrayList<>();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final Thread runner;
    /// The thread that created the executor, which reads through it.
    private final Thread caller = Thread.currentThread();
    private volatile boolean shutdown;

    private TaskOrderExecutor(Random random, int target) {
        this.random = random;
        this.target = target;
        this.runner = Thread.ofPlatform().daemon().name("task-order").start(this::runQueued);
    }

    /// An executor running the queued jobs in the order `seed` picks.
    ///
    /// @param target the jobs it lets queue before it picks: the read's jobs in flight
    static TaskOrderExecutor shuffling(long seed, int target) {
        return new TaskOrderExecutor(new Random(seed), target);
    }

    /// A context whose decode pool is this executor, taken as `threads` threads. The executor's
    /// thread is the context's pool thread, so that a job it runs decodes in place, as on a pool.
    HardwoodContextImpl context(int threads) {
        return HardwoodContextImpl.create(this, threads, Set.of(runner));
    }

    /// The first failure a task threw, which the pool would have dropped; `null` if none.
    Throwable failure() {
        return failure.get();
    }

    @Override
    public void execute(Runnable command) {
        if (shutdown) {
            throw new RejectedExecutionException("The executor is shut down");
        }
        lock.lock();
        try {
            queued.add(command);
            arrived.signalAll();
        }
        finally {
            lock.unlock();
        }
    }

    private void runQueued() {
        while (true) {
            Runnable task;
            lock.lock();
            try {
                while (queued.isEmpty() && !shutdown) {
                    arrived.awaitUninterruptibly();
                }
                if (queued.isEmpty()) {
                    return;
                }
                while (queued.size() < target && !shutdown && callerMaySubmit()) {
                    lock.unlock();
                    try {
                        Thread.onSpinWait();
                    }
                    finally {
                        lock.lock();
                    }
                }
                task = queued.remove(random.nextInt(queued.size()));
            }
            finally {
                lock.unlock();
            }
            run(task);
        }
    }

    /// Whether the reading thread may yet submit a task before one runs: it runs, or waits only
    /// for the executor's lock to submit one.
    private boolean callerMaySubmit() {
        return caller.isAlive() && (caller.getState() == Thread.State.RUNNABLE || lock.hasQueuedThread(caller));
    }

    private void run(Runnable task) {
        try {
            task.run();
        }
        catch (Throwable t) {
            failure.compareAndSet(null, t);
        }
    }

    @Override
    public void shutdown() {
        lock.lock();
        try {
            shutdown = true;
            arrived.signalAll();
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    public List<Runnable> shutdownNow() {
        shutdown();
        return List.of();
    }

    @Override
    public boolean isShutdown() {
        return shutdown;
    }

    @Override
    public boolean isTerminated() {
        return shutdown && !runner.isAlive();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        runner.join(unit.toMillis(timeout));
        return isTerminated();
    }
}
