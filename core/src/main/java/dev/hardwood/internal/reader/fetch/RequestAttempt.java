/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.fetch;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import dev.hardwood.internal.util.Lazy;

/// One attempt at a request's I/O, on a task of a read's [PrefetchTasks], and the future of its
/// result: the read of a [FetchSequence] entry's byte range, or of a file's footer
/// ([dev.hardwood.internal.reader.plan.FileMetadataCache]). An entry takes a new attempt where one
/// started ahead of demand failed ([#failedAhead]); a footer's attempt is the reader's, and the
/// footer requests of every read of the reader that reach the file while it runs join it.
///
/// The read runs on a task of the read's [PrefetchTasks], a virtual thread of the context's I/O
/// executor, which completes it; the read's owner waits for those tasks before it closes the file.
/// A step of the pool never waits for a read: it yields on it ([#whenDone]), and the completion
/// queues the step again. The read is created and then submitted ([#submit]), so that whoever
/// holds it does so before any thread can complete it.
///
/// A read that fails completes with its failure and throws nothing. A read started ahead of demand
/// that failed with an exception is read again by the demand that needs its result
/// ([#failedAhead]); one that failed with an `Error`, which is no transient fault, fails that
/// demand. An `Error` is thrown by the close of its [PrefetchTasks] ([PrefetchTasks#awaitAll()])
/// unless a caller has taken it from the read ([#take]): a read started ahead of demand that no
/// demand reached, or a demand read whose step stopped before it took it. An `Error` therefore
/// reaches the caller once, through the step or through the close.
///
/// @param <T> the read's result
public final class RequestAttempt<T> {

    private static final Lazy<System.Logger> LOG = Lazy.of(() -> System.getLogger(RequestAttempt.class.getName()));

    /// The I/O of a read, run on a task of its [PrefetchTasks].
    @FunctionalInterface
    public interface Work<T> {

        T run() throws IOException;
    }

    /// The message of a failure of the read that is not an `Error` or a `RuntimeException`,
    /// prefixed with the file's name, such as "Failed to fetch data at offset … (length …)".
    private final String failure;
    private final boolean aheadOfDemand;
    private final PrefetchTasks tasks;
    private final Work<T> work;
    private final Work<T> dropped;
    private final CompletableFuture<T> result = new CompletableFuture<>();
    /// The `Error` the read failed with, until a caller takes it ([#take]).
    private volatile Error untakenError;

    /// A read that runs `work` on a task of `tasks` once submitted.
    ///
    /// @param failure the message of the `IOException` [#take] raises for a failure that is
    ///        neither an `Error` nor a `RuntimeException`, prefixed with the file's name
    /// @param aheadOfDemand whether the read starts ahead of demand, so that a demand that needs
    ///        its result reads it again if it failed with an exception
    /// @param dropped the read's result, or its failure, when the close of `tasks` drops it before
    ///        it started
    public RequestAttempt(String failure, boolean aheadOfDemand, PrefetchTasks tasks, Work<T> work, Work<T> dropped) {
        this.failure = failure;
        this.aheadOfDemand = aheadOfDemand;
        this.tasks = Objects.requireNonNull(tasks, "tasks");
        this.work = work;
        this.dropped = dropped;
    }

    /// Runs this read on a task of its [PrefetchTasks], or completes it as dropped if they no
    /// longer admit it, as once its owner closes.
    public void submit() {
        tasks.submit(this::run, () -> complete(dropped, false));
    }

    private void run() {
        complete(work, true);
    }

    private void complete(Work<T> outcome, boolean ran) {
        try {
            result.complete(outcome.run());
        }
        catch (Throwable e) {
            // Any throwable, checked ones an InputFile throws undeclared included: a read that
            // never completes leaves the step waiting for it parked, and the close waiting for
            // that step with it
            if (ran && aheadOfDemand) {
                // The demand that needs the result reports the failure. DEBUG rather than WARN so
                // a sustained backend outage does not emit one line per read for failures that
                // are about to be reported properly.
                LOG.get().log(System.Logger.Level.DEBUG, "Read ahead of demand failed: {0}", failure, e);
            }
            if (e instanceof Error err) {
                // Before the completion, so a caller that takes the failure finds it here to clear
                untakenError = err;
                tasks.recordUntaken(this);
            }
            result.completeExceptionally(e);
        }
    }

    /// The `Error` the read failed with if no caller has taken it, `null` otherwise.
    Error untakenError() {
        return untakenError;
    }

    /// Whether the read has completed, with its result or with a failure.
    public boolean isDone() {
        return result.isDone();
    }

    /// Whether the read started ahead of demand and failed with an exception, so that the demand
    /// that needs its result reads it again and reports a failure of that read.
    public boolean failedAhead() {
        return aheadOfDemand && result.isCompletedExceptionally() && !(result.exceptionNow() instanceof Error);
    }

    /// The result of the read, waiting for it on a caller off the pool; a step of the pool takes
    /// only a read that has completed ([#isDone]).
    ///
    /// @throws IOException the read's failure, as the read's failure message, unless it is an
    ///         `Error` or a `RuntimeException`, which is thrown as it is
    public T take() throws IOException {
        try {
            return result.join();
        }
        catch (CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException r) {
                throw r;
            }
            if (cause instanceof Error err) {
                untakenError = null;
                throw err;
            }
            // An IOException, or a checked exception the InputFile threw undeclared
            throw new IOException(failure, cause);
        }
    }

    /// Waits until the read has completed, its failure included. For a thread off the pool only.
    void await() {
        try {
            result.join();
        }
        catch (CompletionException e) {
            // Reported by take(), which the caller reads next
        }
    }

    /// Runs `action` once the read has completed, on the thread that completes it, or at once on
    /// the caller's when it already has.
    public void whenDone(Runnable action) {
        result.whenComplete((ignored, failed) -> action.run());
    }

    /// A future of the read's result that completing does not complete the read.
    public CompletableFuture<T> future() {
        return result.copy();
    }
}
