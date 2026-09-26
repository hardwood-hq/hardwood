/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;

import dev.hardwood.internal.FetchReason;

/// The speculative tasks one [RowGroupIterator]'s read starts on the common pool: the
/// next row group's planning and first-chunk prefetch, and the one-ahead chunk and
/// region prefetches.
///
/// Nothing waits for such a task to deliver, but each reads the input file, so whoever
/// closes the file must first wait for the ones already running. [#awaitAll()] stops
/// admitting new tasks and does that wait.
public final class PrefetchTasks {

    private final Set<CompletableFuture<Void>> running = ConcurrentHashMap.newKeySet();
    /// Guarded by `this`, so no task is admitted after [#awaitAll()] has taken its snapshot.
    private boolean closed;

    /// Runs `task` on the common pool, carrying the caller's [FetchReason], unless
    /// [#awaitAll()] has been called, in which case the task is dropped.
    public void submit(Runnable task) {
        CompletableFuture<Void> future;
        synchronized (this) {
            if (closed) {
                return;
            }
            future = CompletableFuture.runAsync(FetchReason.bind(task));
            running.add(future);
        }
        future.whenComplete((ignored, failure) -> running.remove(future));
    }

    /// Stops admitting tasks and waits until every admitted one has finished. A task's
    /// failure is not rethrown: speculative work leaves what it could not fetch to the
    /// demand path, which reports it. Does not give up on an interrupt, since returning
    /// early would let the caller close a file a task is still reading. Idempotent.
    public void awaitAll() {
        List<CompletableFuture<Void>> started;
        synchronized (this) {
            closed = true;
            started = List.copyOf(running);
        }
        for (CompletableFuture<Void> future : started) {
            try {
                future.join();
            }
            catch (CompletionException | CancellationException ignored) {
                // speculative: the demand path reports what the task could not do
            }
        }
    }
}
