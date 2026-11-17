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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Supplier;

import dev.hardwood.internal.DecodeBuffers;
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.reader.batch.BatchExchange;
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
    /// ends every column's stream with a failure at once, from any thread and without the lock
    private final Consumer<Throwable> abandoned;
    /// set once a failure escaped the driver's own handling of its jobs' ends ([#lost]); the read
    /// starts and runs no job after it
    private volatile boolean lost;
    /// the jobs running on some thread now ([#run])
    private final AtomicInteger running = new AtomicInteger();
    /// whether a close may go on: no job in flight or, once the read is lost, none running;
    /// built with the driver, so that a close needs no linkage
    private final BooleanSupplier idle;
    private boolean closed;
    /// jobs whose results were applied since the lock was last let go, counted as ended once what
    /// they started is counted and what they returned is released to R, so that a close that finds
    /// the read idle finds nothing on its way back to R
    private int finished;

    /// @param fetchAheadBytes F
    /// @param position reconciles the read's parts and says where the consumer stands, at each poll
    /// @param broken fails the read with what a result or a poll threw, a runtime failure classified
    ///        as every failure of a read is ([ExceptionContext#asReadFailure]); under the lock
    /// @param abandoned ends every column's stream with a failure that escaped the driver's own
    ///        handling of its jobs' ends ([#lost]); from any thread, without the lock
    /// @param queue the read's queue on a context of zero threads, `null` on one with a pool
    ReadDriver(HardwoodContextImpl context, ReadQueue queue, long fetchAheadBytes,
               Supplier<JobScheduler.Position> position, Consumer<Throwable> broken,
               Consumer<Throwable> abandoned) {
        this.broken = broken;
        this.abandoned = abandoned;
        this.readAhead = context.readAheadBuffer();
        this.queue = queue;
        this.context = context;
        this.io = context.ioExecutor();
        this.tasks = queue != null ? queue.tasks() : new ReadTasks();
        this.idle = () -> tasks.idle() || lost && running.get() == 0;
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
                broken.accept(classified(e));
            }
        }
        if (!closed && !lost) {
            // a check of the file the position meets fails the read, which the position then
            // reaches; a throw starting jobs leaves those it started undispatched, and abandons the
            // read ([#unlock])
            JobScheduler.Position at;
            try {
                at = position.get();
            }
            catch (RuntimeException | Error e) {
                broken.accept(classified(e));
                at = position.get();
            }
            List<JobScheduler.Job<?>> jobs = scheduler.start(at);
            for (JobScheduler.Job<?> job : jobs) {
                // counted under the lock, so that a close never finds the read idle with a job
                // started and not yet dispatched
                tasks.started();
                started.add(job);
            }
        }
    }

    /// `t` as the read's failure: a runtime failure classified as a read failure, an [Error] as
    /// thrown. A result throws where a check of the file meets it while the result is applied, and
    /// the reader raises what the read failed with.
    private static Throwable classified(Throwable t) {
        return t instanceof RuntimeException e ? ExceptionContext.asReadFailure(e) : t;
    }

    /// Lets go of the lock once every queued result is applied, then releases to R what they
    /// returned, counts their jobs as ended and dispatches what they started; takes the lock again
    /// where a result was queued as it let go. A poll that throws, a bug or an `Error`, still lets
    /// go of the lock, counts and dispatches, and abandons the read ([#lost]) before the throw goes
    /// on to the caller; a release to R that throws,
    /// from another read's waiter, still counts and dispatches. The jobs to dispatch are copied
    /// before the lock is let go, which allocates: where that fails, they never run ([#lost]).
    private void unlock() {
        do {
            RuntimeException thrown = null;
            Error error = null;
            List<JobScheduler.Job<?>> dispatch = List.of();
            Throwable undispatched = null;
            long released;
            long releasedTransient;
            int ended;
            try {
                try {
                    applyQueued();
                }
                catch (RuntimeException e) {
                    thrown = e;
                }
                catch (Error e) {
                    error = e;
                }
            }
            finally {
                released = scheduler.takeReleased();
                releasedTransient = scheduler.takeReleasedTransient();
                ended = finished;
                finished = 0;
                try {
                    dispatch = List.copyOf(started);
                }
                catch (Throwable t) {
                    undispatched = t;
                }
                started.clear();
                lock.unlock();
            }
            try {
                if (released > 0) {
                    readAhead.release(released, releasedTransient);
                }
            }
            finally {
                for (; ended > 0; ended--) {
                    tasks.ended();
                }
                if (undispatched != null) {
                    lost(undispatched);
                }
                for (JobScheduler.Job<?> job : dispatch) {
                    dispatch(job);
                }
            }
            // the read's own bookkeeping threw, which no rule of the read lets happen: it starts no
            // further job, so it is abandoned, whichever thread met the throw
            if (thrown != null) {
                lost(thrown);
                throw thrown;
            }
            if (error != null) {
                lost(error);
                throw error;
            }
        } while (!results.isEmpty() && lock.tryLock());
    }

    private void dispatch(JobScheduler.Job<?> job) {
        try {
            Runnable run = () -> run(job);
            if (!job.cpu()) {
                io.execute(run);
            }
            else if (queue != null) {
                queue.execute(run);
            }
            else {
                context.execute(run);
            }
        }
        catch (RejectedExecutionException e) {
            // a closed context refuses the job: it ends here, with its failure
            update(ended(scheduler.refused(job, HardwoodContextImpl.contextClosed(e))));
        }
        catch (Throwable t) {
            // the job never runs
            lost(t);
        }
    }

    /// Runs a dispatched job on the thread it was dispatched to, with a set of decode buffers for
    /// a CPU job, and applies its end. This is the job's outermost frame on that thread, so what
    /// throws past the job's own failure handling loses the read ([#lost]) rather than the thread:
    /// with the heap exhausted, taking the buffers, reporting the job's failure and linking code on
    /// that path for the first time all allocate. A job of a lost read does not run.
    private void run(JobScheduler.Job<?> job) {
        running.incrementAndGet();
        try {
            if (lost) {
                return;
            }
            if (job.cpu()) {
                context.runInPlace(buffers -> finish(job, buffers));
            }
            else {
                finish(job, null);
            }
        }
        catch (Throwable t) {
            lost(t);
        }
        finally {
            if (running.decrementAndGet() == 0 && lost) {
                signalClose();
            }
        }
    }

    private void finish(JobScheduler.Job<?> job, DecodeBuffers buffers) {
        update(ended(scheduler.run(job, buffers)));
    }

    /// Fails the read with `t`, which escaped the driver's own handling of its jobs' ends. Throws
    /// nothing, so that the thread that met `t` survives it.
    ///
    /// What escapes is an `Error` such as an `OutOfMemoryError`, or a bug. Reporting `t` under
    /// the lock, as every other failure is reported, can need the allocations that just failed, so
    /// the read is abandoned instead ([BatchExchange#abandon]): every column's stream fails with
    /// `t` and the read starts and runs no further job. Which jobs have ended can no longer be told
    /// once a job's end went unapplied, so a close waits only until none of the read's jobs is
    /// running ([#awaitIdle]); what the read still holds in R is returned by the close's
    /// [JobScheduler#releaseAll].
    private void lost(Throwable t) {
        if (lost) {
            // abandoned already, with the first failure
            signalClose();
            return;
        }
        lost = true;
        Throwable failure = t;
        try {
            failure = classified(t);
        }
        catch (Throwable unclassified) {
            // classifying allocates; the failure then goes out as it was thrown
        }
        try {
            abandoned.accept(failure);
        }
        catch (Throwable notAbandoned) {
            // each column's abandon writes its field before anything that can throw
        }
        signalClose();
    }

    /// Wakes a close waiting for the read's jobs; best effort, since a close waiting on a pool's
    /// read wakes on its own to look again ([ReadTasks#await]).
    private void signalClose() {
        try {
            tasks.signal();
        }
        catch (Throwable notSignalled) {
            // signalling allocates where threads are parked; the close looks again
        }
    }

    private Runnable ended(Runnable apply) {
        return () -> {
            finished++;
            apply.run();
        };
    }

    /// Waits until no job of the read is in flight or, once the read is lost ([#lost]), running,
    /// running the read's queue meanwhile on a context of zero threads.
    void awaitIdle() {
        tasks.await(idle, queue);
    }

    /// Starts no further job, waits until none is in flight, and returns every charge still held
    /// to R. Interrupts nothing. Idempotent.
    void close() {
        locked(() -> closed = true);
        awaitIdle();
        locked(scheduler::releaseAll);
    }
}
