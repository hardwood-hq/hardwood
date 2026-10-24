/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.fetch;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

import dev.hardwood.internal.FetchReason;

/// The reads one [dev.hardwood.internal.reader.plan.RowGroupIterator]'s read starts on the
/// context's I/O executor ([dev.hardwood.internal.HardwoodContextImpl#ioExecutor()]): every read of
/// a file not in memory ([RequestAttempt]), demand reads a step yields on as well as those started ahead of
/// demand, and the footers it reads.
///
/// Each reads the input file, so whoever closes the file must first wait for the ones already
/// running. [#awaitAll()] stops admitting new tasks, makes those not yet started skip their work
/// (running the drop action [#submit] takes, which completes a read so that no step waits for it
/// forever), and waits for the rest.
///
/// A [RequestAttempt] completes with its own failure and throws nothing. [#awaitAll()] rethrows an `Error`
/// a read failed with that no step took ([#recordUntaken]).
public final class PrefetchTasks {

    private final Executor executor;
    private final Set<CompletableFuture<Void>> running = ConcurrentHashMap.newKeySet();
    /// Written under `this`, so no task is admitted after [#awaitAll()] has taken its snapshot;
    /// read by each task as it starts.
    private volatile boolean closed;
    /// The reads that failed with an `Error`, raised by [#awaitAll()] unless a step took it.
    /// Guarded by `this`.
    private final List<RequestAttempt<?>> failedAttempts = new ArrayList<>();

    /// @param executor where the tasks run
    public PrefetchTasks(Executor executor) {
        this.executor = executor;
    }

    /// Runs `task` on the executor, carrying the caller's [FetchReason], and `dropped` instead when
    /// the task is dropped: at once when [#awaitAll()] has been called or the executor refuses it,
    /// on the executor when [#awaitAll()] is called before it starts. A read a step waits for
    /// completes either way. Neither throws: a [RequestAttempt] completes with its failure.
    void submit(Runnable task, Runnable dropped) {
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
        task.run();
    }

    /// Records a read that failed with an `Error`, for [#awaitAll()] to raise if no step takes it.
    synchronized void recordUntaken(RequestAttempt<?> attempt) {
        failedAttempts.add(attempt);
    }

    /// Stops admitting tasks and waits until every admitted one has finished; one that has not
    /// started by then returns without doing its work. Does not give up on an interrupt, since
    /// returning early would let the caller close a file a task is still reading. Idempotent.
    ///
    /// @throws Error the first `Error` a read failed with that no step took, the later ones
    ///         suppressed beneath it, once every task has finished
    public void awaitAll() {
        List<CompletableFuture<Void>> started;
        synchronized (this) {
            closed = true;
            started = List.copyOf(running);
        }
        for (CompletableFuture<Void> future : started) {
            // A read completes with its failure, so the join returns normally.
            future.join();
        }
        Error raised = null;
        synchronized (this) {
            // The steps have ended by now (the caller closes the columns first), so an Error
            // still untaken is one no step will take
            for (RequestAttempt<?> attempt : failedAttempts) {
                Error untaken = attempt.untakenError();
                if (untaken == null) {
                    continue;
                }
                if (raised == null) {
                    raised = untaken;
                }
                else if (untaken != raised) {
                    raised.addSuppressed(untaken);
                }
            }
            failedAttempts.clear();
        }
        if (raised != null) {
            throw raised;
        }
    }
}
