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
import java.util.Locale;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Supplier;

import dev.hardwood.internal.DecodeBuffers;
import dev.hardwood.internal.ExceptionContext;
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
/// thread. A thread of the pool that applies the end of its own job runs one of the jobs that end
/// started itself, up to [#CONTINUATIONS] after its first, rather than handing every one to the
/// pool's queue and waking another thread for it.
///
/// The read moves through the states of [State]; only a running read starts jobs.
final class ReadDriver {

    /// The jobs a task of the pool runs after its first, each started by the end of the one before
    /// and applied on its thread, before it returns to the pool's queue: it bounds how long a read
    /// keeps a thread of the pool from the tasks of other reads queued there.
    static final int CONTINUATIONS = 4;

    /// Where the read stands in its life. A close moves a running read to [#CLOSING] and, once
    /// no job is in flight, to [#CLOSED]; it moves an abandoned read to [#CLOSED] at once. A read
    /// running or closing is abandoned where a job's end cannot be applied; a closed one stays
    /// closed.
    private enum State {
        /// starts what the scheduler lets start
        RUNNING,
        /// starts nothing, and its close waits until no job is in flight
        CLOSING,
        /// a job's end could not be applied: starts nothing, and its jobs' counts no longer
        /// balance, so nothing waits for them
        ABANDONED,
        /// every charge it held is returned to R
        CLOSED;

        /// Whether a wait for the read to go idle can end only with its jobs.
        boolean awaitsJobs() {
            return this == RUNNING || this == CLOSING;
        }
    }

    /// A result that ends a job: counted as ended once the lock is let go, after what it returned
    /// is released to R, so that a close that finds the read idle finds nothing on its way back to R.
    private record JobEnd(Runnable apply) implements Runnable {
        @Override
        public void run() {
            apply.run();
        }
    }

    /// a result that changes nothing, queued where only the scheduler's answer may have changed (R
    /// has room again, an action under the lock completed), so that the lock's holder asks it again
    /// before it lets go
    private static final Runnable POLL_AGAIN = () -> {
    };

    private final ReentrantLock lock = new ReentrantLock();
    /// results of jobs that ended while another thread held the lock, for the holder to apply
    private final ConcurrentLinkedQueue<Runnable> results = new ConcurrentLinkedQueue<>();
    /// moved by the transitions below, each synchronized on the driver; read without the lock
    private volatile State state = State.RUNNING;
    private final JobScheduler scheduler;
    private final Supplier<JobScheduler.Position> reconcile;
    private final ReadAheadBuffer readAhead;
    /// the read's queue on a context of zero threads, `null` on one with a pool
    private final ReadQueue queue;
    private final HardwoodContextImpl context;
    private final Executor io;
    /// the jobs started and not yet applied, which a close waits for
    private final ReadTasks tasks;
    /// fails the read with what a result threw, which no rule of the read should let happen
    private final Consumer<Throwable> onApplyFailure;
    /// fails every column's stream with a failure that cannot be reported under the lock
    private final Consumer<Throwable> onAbandon;

    /// @param fetchAheadBytes F
    /// @param reconcile reconciles the read's parts and says where the consumer stands, at each poll
    /// @param onApplyFailure fails the read with what a result or a poll threw, a runtime failure
    ///        classified as every failure of a read is ([ExceptionContext#asReadFailure]); under the
    ///        lock
    /// @param onAbandon fails every column's stream with a failure that reporting a job's end threw,
    ///        without allocating ([BatchExchange#abandon])
    /// @param queue the read's queue on a context of zero threads, `null` on one with a pool
    ReadDriver(HardwoodContextImpl context, ReadQueue queue, long fetchAheadBytes,
               Supplier<JobScheduler.Position> reconcile, Consumer<Throwable> onApplyFailure,
               Consumer<Throwable> onAbandon) {
        this.onApplyFailure = onApplyFailure;
        this.onAbandon = onAbandon;
        this.readAhead = context.readAheadBuffer();
        this.queue = queue;
        this.context = context;
        this.io = context.ioExecutor();
        this.tasks = queue != null ? queue.tasks() : new ReadTasks();
        this.reconcile = reconcile;
        this.scheduler = new JobScheduler(readAhead, () -> submitOrAbandon(POLL_AGAIN),
                2 * (queue != null ? 1 : context.threads()), fetchAheadBytes);
    }

    JobScheduler scheduler() {
        return scheduler;
    }

    /// Queues `result` for the thread holding the lock, and drains the queue where none holds it:
    /// applies the result and dispatches what it made possible. On the thread holding the lock, it
    /// is applied before the lock is let go.
    void submit(Runnable result) {
        results.add(result);
        if (!lock.isHeldByCurrentThread() && lock.tryLock()) {
            drainAndUnlock();
        }
    }

    /// [#submit] where nothing above would take a throw for this read: on a thread of its own job,
    /// or of another read whose release of R wakes this one, or of a footer load. Applying a result
    /// can throw, as reporting a failure does when the heap is exhausted and the report allocates;
    /// the result is then never applied and the read cannot go on. It is abandoned, which allocates
    /// nothing, so that the consumer raises the failure rather than wait for a batch that never
    /// comes, and the throw stays here, reaching neither another read nor the thread's pool.
    void submitOrAbandon(Runnable result) {
        try {
            submit(result);
        }
        catch (Throwable t) {
            abandon(t);
        }
    }

    /// Abandons the read with `t`, which applying a result threw ([#submitOrAbandon]).
    private void abandon(Throwable t) {
        toAbandoned();
        onAbandon.accept(t);
        tasks.signal();
    }

    /// Runs `action` under the lock, waiting for it, then polls and dispatches.
    void runLocked(Runnable action) {
        lock.lock();
        try {
            action.run();
            // asks the scheduler only where the action completed: one that threw leaves the read
            // half changed, which the caller fails
            results.add(POLL_AGAIN);
        }
        finally {
            drainAndUnlock();
        }
    }

    /// Fails the read where it can no longer progress, for a consumer whose poll of `column` has
    /// waited without a batch. The read is stalled where its consumer still waits, no job of it is
    /// in flight and no result is queued, starting jobs at the consumer's position starts nothing,
    /// and it waits on nothing another read may release: room in R, or a footer load another read
    /// runs. Nothing could then wake it, and it is abandoned with an `IllegalStateException`
    /// naming what holds it back. A reconcile that publishes the consumer's batch, or a job that
    /// starts, fails it too: work no event tried, a lost wake-up the check does not cover for.
    /// A check that throws abandons the read with the throw.
    ///
    /// @param waiting whether the consumer still waits on the column; asked under the lock, where
    ///        the column publishes
    /// @param waitsOnOtherRead whether the read waits for a footer another read loads; under the lock
    void checkProgress(String column, BooleanSupplier waiting, BooleanSupplier waitsOnOtherRead) {
        Throwable stall;
        lock.lock();
        try {
            try {
                stall = stall(column, waiting, waitsOnOtherRead);
            }
            catch (Throwable t) {
                // a reconcile or a start that throws, as one does where the heap is exhausted, leaves
                // the read half changed: it is abandoned with the throw, as a result that throws is
                stall = t;
            }
            if (stall != null) {
                // before the lock is let go, so that no job starts past the stall
                toAbandoned();
            }
        }
        finally {
            drainAndUnlock();
        }
        if (stall != null) {
            onAbandon.accept(stall);
            tasks.signal();
        }
    }

    /// The failure the read stalled with, `null` where it can progress; under the lock.
    private Throwable stall(String column, BooleanSupplier waiting, BooleanSupplier waitsOnOtherRead) {
        if (state != State.RUNNING || !waiting.getAsBoolean() || !results.isEmpty() || scheduler.inFlight() > 0
                || !tasks.idle()) {
            return null;
        }
        // a wait on another read first: a release of R on its way has room for a job the start
        // below would take, before the release's wake-up is queued
        if (scheduler.waitingForRoom() || waitsOnOtherRead.getAsBoolean()) {
            return null;
        }
        JobScheduler.Position at = reconcile.get();
        if (!waiting.getAsBoolean()) {
            return new IllegalStateException("The read of column '" + column
                    + "' missed a wake-up: no job was in flight, and its next batch was publishable");
        }
        List<JobScheduler.Job<?>> started = scheduler.start(at);
        if (!started.isEmpty()) {
            JobScheduler.Job<?> job = started.getFirst();
            return new IllegalStateException("The read of column '" + column + "' missed a wake-up: no job was in flight, and a "
                    + job.kind().name().toLowerCase(Locale.ROOT) + " job at row " + job.key() + " could start");
        }
        if (scheduler.waitingForRoom()) {
            // the start's charge did not fit: R's next release wakes the read
            return null;
        }
        return new IllegalStateException("The read of column '" + column + "' stalled: " + scheduler.describeStall(at));
    }

    /// Runs `result` under the lock; one that throws fails the read.
    private void apply(Runnable result) {
        try {
            result.run();
        }
        catch (RuntimeException | Error e) {
            onApplyFailure.accept(classified(e));
        }
    }

    /// Starts what the scheduler lets start, where the read is running, and counts it as started;
    /// under the lock, so that a close never finds the read idle with a job started and not yet
    /// dispatched.
    private List<JobScheduler.Job<?>> startReady() {
        if (state != State.RUNNING) {
            return List.of();
        }
        List<JobScheduler.Job<?>> jobs;
        try {
            jobs = scheduler.start(reconcile.get());
        }
        catch (RuntimeException | Error e) {
            onApplyFailure.accept(classified(e));
            jobs = scheduler.start(reconcile.get());
        }
        for (int i = 0; i < jobs.size(); i++) {
            tasks.started();
        }
        return jobs;
    }

    /// `t` as the read's failure: a runtime failure classified as a read failure, an [Error] as
    /// thrown. A result throws where a check of the file meets it while the result is applied, and
    /// the reader raises what the read failed with.
    private static Throwable classified(Throwable t) {
        return t instanceof RuntimeException e ? ExceptionContext.asReadFailure(e) : t;
    }

    /// Drains the queued results on the thread holding the lock, until none is left, and lets go of
    /// it: applies them and polls for what they made possible, then lets go of the lock,
    /// releases to R what the results returned, counts their jobs as ended and dispatches what the
    /// poll started; takes the lock again where a result was queued as it let go. A poll that
    /// throws, a bug, still lets go of the lock, and the jobs whose results were applied still
    /// count as ended; a release to R that throws, from another read's waiter, still counts and
    /// dispatches.
    private void drainAndUnlock() {
        drainAndUnlock(false);
    }

    /// [#drainAndUnlock()], keeping out of the dispatch, where `keepOne`, the first CPU job a poll
    /// started (a plan first, as the scheduler starts plans ahead of other work, otherwise the
    /// lowest keyed) for the calling thread to run; returns it, `null` where none was started.
    private JobScheduler.Job<?> drainAndUnlock(boolean keepOne) {
        JobScheduler.Job<?> kept = null;
        do {
            List<JobScheduler.Job<?>> dispatch = List.of();
            int ended = 0;
            try {
                Runnable r = results.poll();
                if (r != null) {
                    for (; r != null; r = results.poll()) {
                        if (r instanceof JobEnd) {
                            ended++;
                        }
                        apply(r);
                    }
                    dispatch = startReady();
                }
            }
            finally {
                long released = scheduler.takeReleased();
                lock.unlock();
                if (keepOne && kept == null) {
                    int first = firstCpu(dispatch);
                    if (first >= 0) {
                        kept = dispatch.get(first);
                        dispatch = without(dispatch, first);
                    }
                }
                afterUnlock(released, ended, dispatch);
            }
        } while (!results.isEmpty() && lock.tryLock());
        return kept;
    }

    /// The position of the first CPU job in `jobs`, `-1` for none.
    private static int firstCpu(List<JobScheduler.Job<?>> jobs) {
        for (int i = 0; i < jobs.size(); i++) {
            if (jobs.get(i).cpu()) {
                return i;
            }
        }
        return -1;
    }

    /// `jobs` without the one at `i`.
    private static List<JobScheduler.Job<?>> without(List<JobScheduler.Job<?>> jobs, int i) {
        List<JobScheduler.Job<?>> rest = new ArrayList<>(jobs.size() - 1);
        for (int j = 0; j < jobs.size(); j++) {
            if (j != i) {
                rest.add(jobs.get(j));
            }
        }
        return rest;
    }

    /// Releases `released` to R, counts `ended` jobs as ended and dispatches `dispatch`, outside
    /// the lock.
    private void afterUnlock(long released, int ended, List<JobScheduler.Job<?>> dispatch) {
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
    }

    private void dispatch(JobScheduler.Job<?> job) {
        try {
            if (job.cpu()) {
                if (queue != null) {
                    queue.executePage(buffers -> runJob(job, buffers));
                }
                else {
                    context.execute(buffers -> runOnPool(job, buffers));
                }
            }
            else {
                io.execute(() -> runJob(job, null));
            }
        }
        catch (RejectedExecutionException e) {
            // a closed context refuses the job: it ends here, with its failure
            submit(new JobEnd(scheduler.refused(job, HardwoodContextImpl.contextClosed(e))));
        }
    }

    /// Runs `job`, a CPU job on a thread of the pool, then up to [#CONTINUATIONS] of the jobs its
    /// end and theirs start, as [#runJob] runs one: where the lock is free, the thread applies the
    /// end itself, keeps the first CPU job the poll started and runs it; where another thread holds
    /// the lock, that thread applies the end and dispatches what it starts, and this task ends.
    private void runOnPool(JobScheduler.Job<?> job, DecodeBuffers buffers) {
        try {
            JobScheduler.Job<?> next = job;
            for (int continued = 0; next != null; continued++) {
                results.add(new JobEnd(scheduler.run(next, buffers)));
                next = lock.tryLock() ? drainAndUnlock(continued < CONTINUATIONS) : null;
            }
        }
        catch (Throwable t) {
            abandon(t);
        }
    }

    /// Runs `job`'s work and submits its end as [#submitOrAbandon] submits a result, from building
    /// the result on: building it allocates too.
    private void runJob(JobScheduler.Job<?> job, DecodeBuffers buffers) {
        try {
            submit(new JobEnd(scheduler.run(job, buffers)));
        }
        catch (Throwable t) {
            abandon(t);
        }
    }

    /// Waits until no job of the read is in flight, running the read's queue meanwhile on a context
    /// of zero threads; for an abandoned or closed read not at all.
    void awaitIdle() {
        tasks.await(() -> tasks.idle() || !state.awaitsJobs(), queue);
    }

    /// Starts no further job, waits until none is in flight, or for an abandoned read not at all,
    /// and returns every charge still held to R. Interrupts nothing. Idempotent.
    void close() {
        runLocked(this::toClosing);
        awaitIdle();
        runLocked(() -> {
            if (toClosed()) {
                scheduler.releaseAll();
            }
        });
    }

    /// A running read starts closing.
    private synchronized void toClosing() {
        if (state == State.RUNNING) {
            state = State.CLOSING;
        }
    }

    /// A running or closing read is abandoned; a closed one stays closed. Allocates nothing, as it
    /// runs where an allocation has just failed.
    private synchronized void toAbandoned() {
        if (state == State.RUNNING || state == State.CLOSING) {
            state = State.ABANDONED;
        }
    }

    /// The read is closed; returns whether it was not before.
    private synchronized boolean toClosed() {
        boolean was = state != State.CLOSED;
        state = State.CLOSED;
        return was;
    }
}
