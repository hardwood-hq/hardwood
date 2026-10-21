/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.UncheckedIOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

import dev.hardwood.internal.FetchReason;
import dev.hardwood.internal.util.Lazy;
import dev.hardwood.reader.ParquetReadException;

/// The speculative tasks one [RowGroupIterator]'s read starts on one executor: on the context's
/// I/O executor ([HardwoodContextImpl#ioExecutor()]), the reads of the next row group's index
/// window and first chunk and the one-ahead chunk and region prefetches; at the tail of the
/// context's pool, the next row group's planning.
///
/// Nothing waits for such a task to deliver, but each reads the input file, so whoever
/// closes the file must first wait for the ones already running. [#awaitAll()] stops
/// admitting new tasks, makes those not yet started skip their work, and waits for the rest.
///
/// A task's failure does not fail the read, since the demand path repeats the work and
/// reports what fails there ([#report]). An `Error` is rethrown by [#awaitAll()].
public final class PrefetchTasks {

    private static final Lazy<System.Logger> LOG = Lazy.of(() -> System.getLogger(PrefetchTasks.class.getName()));

    private final Executor executor;
    private final Set<CompletableFuture<Void>> running = ConcurrentHashMap.newKeySet();
    /// Written under `this`, so no task is admitted after [#awaitAll()] has taken its snapshot;
    /// read by each task as it starts.
    private volatile boolean closed;
    /// The first `Error` a task raised, the later ones suppressed beneath it. Guarded by `this`.
    private Error failure;

    /// @param executor where the tasks run
    public PrefetchTasks(Executor executor) {
        this.executor = executor;
    }

    /// Runs `task` on the executor, carrying the caller's [FetchReason], unless
    /// [#awaitAll()] has been called or the executor refuses it, in which case the task is
    /// dropped.
    public void submit(Runnable task) {
        Runnable bound = FetchReason.bind(task);
        CompletableFuture<Void> future;
        synchronized (this) {
            if (closed) {
                return;
            }
            try {
                future = CompletableFuture.runAsync(() -> run(bound), executor);
            }
            catch (RejectedExecutionException e) {
                // The context was closed; the demand path fails the read
                return;
            }
            running.add(future);
        }
        future.whenComplete((ignored, unused) -> running.remove(future));
    }

    private void run(Runnable task) {
        if (closed) {
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
    /// @throws Error the first `Error` a task raised, once every task has finished
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
            raised = failure;
            failure = null;
        }
        if (raised != null) {
            throw raised;
        }
    }
}
