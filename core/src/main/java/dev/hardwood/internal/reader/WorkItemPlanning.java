/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import dev.hardwood.internal.ExceptionContext;

/// The planning of the work item at one index of a read's work list: the work item, with its fetch
/// plans computed, or `null` when the list ends before the index. A read whose files are not all in
/// memory plans a work item on a virtual thread, waiting for the planning's reads there, and a
/// fetch step that reaches the work item first yields on its planning ([#whenMet]); a read whose
/// files are all in memory plans on the step, and the planning is complete when the step gets it.
///
/// A planning that fails completes with its failure, which every column that reaches the work item
/// takes ([#workItem]). An `Error` it fails with that no column takes, as when every column
/// stopped first, is raised by the close of the read's [PrefetchTasks].
final class WorkItemPlanning implements Need, PrefetchTasks.Untaken {

    /// What a planning runs: the work item at its index with its plans computed, recorded with
    /// [#resolved] once it exists.
    @FunctionalInterface
    interface Plan {
        RowGroupIterator.WorkItem plan(WorkItemPlanning planning) throws IOException;
    }

    private final CompletableFuture<RowGroupIterator.WorkItem> planned = new CompletableFuture<>();
    private volatile Error untakenError;
    /// The work item, once the work list holds it, which a failure planning its row group is
    /// reported against.
    private volatile RowGroupIterator.WorkItem workItem;

    /// Runs `plan` on the calling thread and completes with its outcome; an `Error` it raises is
    /// recorded with `tasks` until a column takes it.
    void run(Plan plan, PrefetchTasks tasks) {
        try {
            planned.complete(plan.plan(this));
        }
        catch (Throwable e) {
            // Any throwable: a planning that never completes leaves the steps waiting for it parked
            if (e instanceof Error err) {
                // Before the completion, so a column that takes the failure finds it here to clear
                untakenError = err;
                tasks.recordUntaken(this);
            }
            planned.completeExceptionally(e);
        }
    }

    /// Completes with `failure` without planning: the read was closed before the planning started.
    void drop(IOException failure) {
        planned.completeExceptionally(failure);
    }

    /// Whether the planning has completed, with its work item or with a failure.
    boolean isDone() {
        return planned.isDone();
    }

    /// Whether the planning has completed with a failure.
    boolean failed() {
        return planned.isCompletedExceptionally();
    }

    /// Records the work item the planning plans, before it plans its row group.
    void resolved(RowGroupIterator.WorkItem workItem) {
        this.workItem = workItem;
    }

    /// The work item a failed planning was planning the row group of, `null` when it failed
    /// before the work list held it, planning a file.
    RowGroupIterator.WorkItem failedWorkItem() {
        return workItem;
    }

    /// The planned work item, `null` when the work list ends before the planning's index.
    ///
    /// @throws IOException the planning's failure, as planning raised it
    /// @throws IllegalStateException if the planning has not completed
    RowGroupIterator.WorkItem workItem() throws IOException {
        if (!planned.isDone()) {
            throw new IllegalStateException("The work item has not been planned");
        }
        try {
            return planned.join();
        }
        catch (CompletionException e) {
            switch (e.getCause()) {
                case UncheckedIOException unchecked -> throw ExceptionContext.unwrap(unchecked);
                case IOException io -> throw io;
                case RuntimeException runtime -> throw runtime;
                case Error error -> {
                    untakenError = null;
                    throw error;
                }
                // A checked exception an InputFile threw undeclared
                case Throwable other -> throw new IOException(other);
            }
        }
    }

    @Override
    public void whenMet(Runnable resume) {
        planned.whenComplete((ignored, failure) -> resume.run());
    }

    @Override
    public Error untakenError() {
        return untakenError;
    }
}
