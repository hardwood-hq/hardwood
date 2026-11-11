/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

import dev.hardwood.internal.DecodeBuffers;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.reader.memory.ReadAheadBuffer;
import dev.hardwood.internal.reader.step.ReadQueue;
import dev.hardwood.internal.reader.step.ReadTasks;

/// Runs a read's jobs: fetches on the context's I/O executor, CPU jobs on its pool or, with
/// zero threads, on the read's [ReadQueue], which the threads using the read run.
///
/// A job's end is applied under the read's one lock, by its callbacks, and followed by a poll of
/// the [JobScheduler] for the jobs it made possible. No thread waits for the lock to apply a result: a
/// job that ends while another thread holds it queues its result, and the holder applies every
/// queued result before it lets go. Once it has let go, it dispatches the jobs its polls started
/// and returns to R what the results released, since R runs other reads' waiters on the releasing
/// thread.
final class ReadDriver {

    /// a result that changes nothing, queued where only the scheduler's answer may have changed (R
    /// has room again, an action under the lock completed), so that the lock's holder asks it again
    /// before it lets go
    private static final Runnable NOTHING = () -> {
    };

    private final ReentrantLock lock = new ReentrantLock();
    /// results of jobs that ended while another thread held the lock, for the holder to apply
    private final ConcurrentLinkedQueue<Runnable> results = new ConcurrentLinkedQueue<>();
    /// the jobs the holder's polls started, dispatched once it has let go of the lock
    private final List<JobScheduler.Job<?>> started = new ArrayList<>();
    private final JobScheduler scheduler;
    private final Supplier<JobScheduler.Position> position;
    private final ReadAheadBuffer readAhead;
    /// the read's queue on a context of zero threads, `null` on one with a pool
    private final ReadQueue queue;
    private final HardwoodContextImpl context;
    private final Executor io;
    /// the jobs started and not yet applied, which a close waits for
    private final ReadTasks tasks;
    /// fails the read with what a result threw, which no rule of the read should let happen
    private final Consumer<Throwable> broken;
    private boolean closed;
    /// jobs whose results were applied since the lock was last let go, counted as ended once what
    /// they started is counted and what they returned is released to R, so that a close that finds
    /// the read idle finds nothing on its way back to R
    private int finished;

    /// @param fetchAheadBytes F
    /// @param position reconciles the read's parts and says where the consumer stands, at each poll
    /// @param broken fails the read with what a result or a poll threw; under the lock
    /// @param queue the read's queue on a context of zero threads, `null` on one with a pool
    ReadDriver(HardwoodContextImpl context, ReadQueue queue, long fetchAheadBytes,
               Supplier<JobScheduler.Position> position, Consumer<Throwable> broken) {
        this.broken = broken;
        this.readAhead = context.readAheadBuffer();
        this.queue = queue;
        this.context = context;
        this.io = context.ioExecutor();
        this.tasks = queue != null ? queue.tasks() : new ReadTasks();
        this.position = position;
        this.scheduler = new JobScheduler(readAhead, () -> update(NOTHING),
                2 * (queue != null ? 1 : context.threads()), fetchAheadBytes);
    }

    JobScheduler scheduler() {
        return scheduler;
    }

    /// Applies `result` and dispatches what it made possible; or, where another thread holds the
    /// lock, leaves it to that thread. On the thread holding the lock, it is applied before the
    /// lock is let go.
    void update(Runnable result) {
        results.add(result);
        if (!lock.isHeldByCurrentThread() && lock.tryLock()) {
            unlock();
        }
    }

    /// Runs `action` under the lock, waiting for it, then polls and dispatches.
    void locked(Runnable action) {
        lock.lock();
        try {
            action.run();
            // asks the scheduler only where the action completed: one that threw leaves the read
            // half changed, which the caller fails
            results.add(NOTHING);
        }
        finally {
            unlock();
        }
    }

    /// Applies the queued results and polls for what they made possible; under the lock.
    private void applyQueued() {
        Runnable r = results.poll();
        if (r == null) {
            return;
        }
        for (; r != null; r = results.poll()) {
            try {
                r.run();
            }
            catch (RuntimeException | Error e) {
                broken.accept(e);
            }
        }
        if (!closed) {
            List<JobScheduler.Job<?>> jobs;
            try {
                jobs = scheduler.start(position.get());
            }
            catch (RuntimeException | Error e) {
                broken.accept(e);
                jobs = scheduler.start(position.get());
            }
            for (JobScheduler.Job<?> job : jobs) {
                // counted under the lock, so that a close never finds the read idle with a job
                // started and not yet dispatched
                tasks.started();
                started.add(job);
            }
        }
    }

    /// Lets go of the lock once every queued result is applied, then releases to R what they
    /// returned, counts their jobs as ended and dispatches what they started; takes the lock again
    /// where a result was queued as it let go. A poll that throws, a bug, still lets go of the lock;
    /// a release to R that throws, from another read's waiter, still counts and dispatches.
    private void unlock() {
        do {
            List<JobScheduler.Job<?>> dispatch;
            long released;
            int ended;
            try {
                applyQueued();
            }
            finally {
                dispatch = List.copyOf(started);
                started.clear();
                released = scheduler.takeReleased();
                ended = finished;
                finished = 0;
                lock.unlock();
            }
            try {
                if (released > 0) {
                    readAhead.release(released);
                }
            }
            finally {
                for (; ended > 0; ended--) {
                    tasks.ended();
                }
                for (JobScheduler.Job<?> job : dispatch) {
                    dispatch(job);
                }
            }
        } while (!results.isEmpty() && lock.tryLock());
    }

    private void dispatch(JobScheduler.Job<?> job) {
        try {
            if (job.cpu()) {
                if (queue != null) {
                    queue.executePage(buffers -> finish(job, buffers));
                }
                else {
                    context.execute(buffers -> finish(job, buffers));
                }
            }
            else {
                io.execute(() -> finish(job, null));
            }
        }
        catch (RejectedExecutionException e) {
            // a closed context refuses the job: it ends here, with its failure
            update(ended(scheduler.refused(job, HardwoodContextImpl.contextClosed(e))));
        }
    }

    private void finish(JobScheduler.Job<?> job, DecodeBuffers buffers) {
        update(ended(scheduler.run(job, buffers)));
    }

    private Runnable ended(Runnable apply) {
        return () -> {
            finished++;
            apply.run();
        };
    }

    /// Waits until no job of the read is in flight, running the read's queue meanwhile on a context
    /// of zero threads.
    void awaitIdle() {
        tasks.await(tasks::idle, queue);
    }

    /// Starts no further job, waits until none is in flight, and returns every charge still held
    /// to R. Interrupts nothing. Idempotent.
    void close() {
        locked(() -> closed = true);
        awaitIdle();
        locked(scheduler::releaseAll);
    }
}
