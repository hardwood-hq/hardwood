/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

import dev.hardwood.internal.FetchReason;
import dev.hardwood.internal.util.Lazy;
import dev.hardwood.reader.ParquetReadException;

/// The reads one [RowGroupIterator]'s read starts on the context's I/O executor
/// ([HardwoodContextImpl#ioExecutor()]): every read of a file not in memory ([RangeRead]), demand
/// reads a step yields on as well as speculative ones.
///
/// Each reads the input file, so whoever closes the file must first wait for the ones already
/// running. [#awaitAll()] stops admitting new tasks, makes those not yet started skip their work
/// (running the drop action [#submit(Runnable, Runnable)] takes, which completes a read so that
/// no step waits for it forever), and waits for the rest.
///
/// A [RangeRead] completes with its own failure and throws nothing: a demand read's failure
/// reaches the step waiting for it, and a speculative read's leaves its bytes to a demand read.
/// Any other task's failure does not fail the read, since the demand path repeats the work and
/// reports what fails there ([#report]). [#awaitAll()] rethrows an `Error` a task raised, and one
/// a read failed with that no step took ([#recordUntaken]).
public final class PrefetchTasks {

    private static final Lazy<System.Logger> LOG = Lazy.of(() -> System.getLogger(PrefetchTasks.class.getName()));

    private final Executor executor;
    private final Set<CompletableFuture<Void>> running = ConcurrentHashMap.newKeySet();
    /// Written under `this`, so no task is admitted after [#awaitAll()] has taken its snapshot;
    /// read by each task as it starts.
    private volatile boolean closed;
    /// The first `Error` a task raised, the later ones suppressed beneath it. Guarded by `this`.
    private Error failure;
    /// The work that failed with an `Error`, raised by [#awaitAll()] unless a step took it.
    /// Guarded by `this`.
    private final List<Untaken> failedWork = new ArrayList<>();

    /// Work whose failure a step takes, such as a read it yielded on: an `Error` it failed with
    /// that no step took is raised by [#awaitAll()].
    interface Untaken {

        /// The `Error` the work failed with if no step has taken it, `null` otherwise.
        Error untakenError();
    }

    /// @param executor where the tasks run
    public PrefetchTasks(Executor executor) {
        this.executor = executor;
    }

    /// Runs `task` on the executor, carrying the caller's [FetchReason], unless
    /// [#awaitAll()] has been called or the executor refuses it, in which case the task is
    /// dropped.
    public void submit(Runnable task) {
        submit(task, () -> {});
    }

    /// Runs `task` as [#submit(Runnable)] does, and `dropped` instead when the task is dropped:
    /// at once when it is not admitted, on the executor when [#awaitAll()] is called before it
    /// starts. A read a step waits for completes either way.
    public void submit(Runnable task, Runnable dropped) {
        Runnable bound = FetchReason.bind(task);
        CompletableFuture<Void> future;
        synchronized (this) {
            if (closed) {
                dropped.run();
                return;
            }
            try {
                future = CompletableFuture.runAsync(() -> run(bound, dropped), executor);
            }
            catch (RejectedExecutionException e) {
                // The context was closed; the demand path fails the read
                dropped.run();
                return;
            }
            running.add(future);
        }
        future.whenComplete((ignored, unused) -> running.remove(future));
    }

    private void run(Runnable task, Runnable dropped) {
        if (closed) {
            dropped.run();
            return;
        }
        try {
            task.run();
        }
        catch (RuntimeException | Error e) {
            recordFailure(e);
        }
    }

    private synchronized void recordFailure(Throwable e) {
        failure = report(e, failure);
    }

    /// Records work that failed with an `Error`, for [#awaitAll()] to raise if no step takes it.
    synchronized void recordUntaken(Untaken work) {
        failedWork.add(work);
    }

    /// Reports the failure of speculative work that no caller waits for: a failure of the file
    /// (an I/O failure, a malformed or unsupported file) is logged at `DEBUG`, since the demand
    /// path repeats the work and reports it, any other `RuntimeException` at `WARNING`, and an
    /// `Error` is returned, suppressed beneath `earlier` if there is one, for the caller to throw.
    ///
    /// @return the `Error` to throw, `null` when there is none
    static Error report(Throwable failure, Error earlier) {
        switch (failure) {
            case UncheckedIOException e -> LOG.get().log(System.Logger.Level.DEBUG, "Prefetch failed", e);
            case ParquetReadException e -> LOG.get().log(System.Logger.Level.DEBUG, "Prefetch failed", e);
            case UnsupportedOperationException e -> LOG.get().log(System.Logger.Level.DEBUG, "Prefetch failed", e);
            case Error e -> {
                if (earlier == null) {
                    return e;
                }
                if (e != earlier) {
                    earlier.addSuppressed(e);
                }
            }
            default -> LOG.get().log(System.Logger.Level.WARNING, "Prefetch failed", failure);
        }
        return earlier;
    }

    /// Stops admitting tasks and waits until every admitted one has finished; one that has not
    /// started by then returns without doing its work. Does not give up on an interrupt, since
    /// returning early would let the caller close a file a task is still reading. Idempotent.
    ///
    /// @throws Error the first `Error` a task raised, or a read or a planning failed with that no
    ///         step took, once every task has finished
    public void awaitAll() {
        List<CompletableFuture<Void>> started;
        synchronized (this) {
            closed = true;
            started = List.copyOf(running);
        }
        for (CompletableFuture<Void> future : started) {
            // A task catches its own failures, so the join returns normally.
            future.join();
        }
        Error raised;
        synchronized (this) {
            // The steps have ended by now (the caller closes the columns first), so an Error
            // still untaken is one no step will take
            for (Untaken work : failedWork) {
                Error untaken = work.untakenError();
                if (untaken != null) {
                    failure = report(untaken, failure);
                }
            }
            failedWork.clear();
            raised = failure;
            failure = null;
        }
        if (raised != null) {
            throw raised;
        }
    }
}
