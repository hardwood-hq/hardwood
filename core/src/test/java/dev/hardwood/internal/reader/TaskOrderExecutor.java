/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/// A decode pool for tests that decides the order a read's page tasks run in, one task at a time.
///
/// - [#inline()] runs each task on the thread that submits it, as it is submitted: a page's task
///   has run before its column's fetcher takes the next page.
/// - [#shuffling(long)] queues the tasks and runs them on a thread of its own, each time picking
///   one of the queued tasks at random from a seeded generator. Before it picks, it lets the
///   scheduler submit what it may: it waits until the read has as many tasks queued as it may have
///   in flight, or until no task arrived for a moment. The tasks of one column then run in any
///   order, not only in page order.
///
/// Every task runs to its end before the next starts, so the order of the tasks is what varies:
/// the steps of the cell protocol that span tasks (which task measures, sizes, places, fills
/// and completes a cell, and which parks) are taken in the order the seed gives, while the
/// races inside one step, which the cell's monitor and atomics settle, are left to the tests
/// running real threads. The fetchers still run on threads of their own, so a seed picks among
/// whichever tasks they have let the scheduler submit by then. The [Listener] sees every task before and after it runs, on the thread running
/// it, with the page the scheduler submitted it for, and may inspect the page's state there
/// without racing the task.
///
/// The pages are told to the executor through [ReadScheduler#submissionObserver], which
/// [#install] sets and [#uninstall] clears.
final class TaskOrderExecutor extends AbstractExecutorService {

    /// Sees each task run.
    interface Listener {

        /// On the submitting thread, as the scheduler hands the page's task to the pool.
        default void submitted(ScheduledColumn<?> column, PageTask page) {
        }

        void beforeRun(ScheduledColumn<?> column, PageTask page);

        void afterRun(ScheduledColumn<?> column, PageTask page);
    }

    /// How long the shuffling thread waits for another task before it picks among those queued.
    private static final long QUIET_NANOS = TimeUnit.MICROSECONDS.toNanos(100);

    /// The page each thread is about to hand to the pool, from the scheduler's observer.
    private static final ThreadLocal<Submission> SUBMITTING = new ThreadLocal<>();

    private record Submission(ScheduledColumn<?> column, PageTask page) {
    }

    private record Task(Submission submission, Runnable run) {
    }

    private final Random random;
    private final int target;
    private final Listener listener;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition arrived = lock.newCondition();
    private final List<Task> queued = new ArrayList<>();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final Thread runner;
    private long arrivals;
    private volatile boolean shutdown;

    private TaskOrderExecutor(Random random, int target, Listener listener) {
        this.random = random;
        this.target = target;
        this.listener = listener;
        this.runner = random == null ? null : Thread.ofPlatform().daemon().name("task-order").start(this::runQueued);
    }

    /// An executor running each task on the submitting thread.
    static TaskOrderExecutor inline(Listener listener) {
        return new TaskOrderExecutor(null, 0, listener);
    }

    /// An executor running the queued tasks in the order `seed` picks.
    ///
    /// @param target the tasks it lets queue before it picks: the read's pages in flight
    static TaskOrderExecutor shuffling(long seed, int target, Listener listener) {
        return new TaskOrderExecutor(new Random(seed), target, listener);
    }

    /// A context whose decode pool is this executor, taken as `threads` threads.
    HardwoodContextImpl context(int threads) {
        return HardwoodContextImpl.create(this, threads);
    }

    /// Lets the executor see the page of each task the scheduler submits.
    static void install() {
        ReadScheduler.submissionObserver = (column, page, inFlight, lowestRow, readPublishedRow) -> SUBMITTING.set(new Submission(column, page));
    }

    static void uninstall() {
        ReadScheduler.submissionObserver = null;
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
        Submission submission = SUBMITTING.get();
        SUBMITTING.remove();
        Task task = new Task(submission, command);
        if (listener != null && submission != null) {
            listener.submitted(submission.column(), submission.page());
        }
        if (runner == null) {
            run(task);
            return;
        }
        lock.lock();
        try {
            queued.add(task);
            arrivals++;
            arrived.signalAll();
        }
        finally {
            lock.unlock();
        }
    }

    private void runQueued() {
        while (true) {
            Task task;
            lock.lock();
            try {
                while (queued.isEmpty() && !shutdown) {
                    arrived.awaitUninterruptibly();
                }
                if (queued.isEmpty()) {
                    return;
                }
                while (queued.size() < target && !shutdown) {
                    long seen = arrivals;
                    arrived.awaitNanos(QUIET_NANOS);
                    if (arrivals == seen) {
                        break;
                    }
                }
                task = queued.remove(random.nextInt(queued.size()));
            }
            catch (InterruptedException e) {
                return;
            }
            finally {
                lock.unlock();
            }
            run(task);
        }
    }

    private void run(Task task) {
        Submission submission = task.submission();
        try {
            if (listener != null && submission != null) {
                listener.beforeRun(submission.column(), submission.page());
            }
            task.run().run();
            if (listener != null && submission != null) {
                listener.afterRun(submission.column(), submission.page());
            }
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
        return shutdown && (runner == null || !runner.isAlive());
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        if (runner != null) {
            runner.join(unit.toMillis(timeout));
        }
        return isTerminated();
    }
}
