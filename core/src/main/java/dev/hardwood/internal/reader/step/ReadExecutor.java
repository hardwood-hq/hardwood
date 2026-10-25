/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.step;

import java.util.concurrent.RejectedExecutionException;
import java.util.function.Consumer;

import dev.hardwood.internal.DecodeBuffers;
import dev.hardwood.internal.HardwoodContextImpl;

/// Runs the walk advances, page tasks and planning of one read: on its context's pool
/// ([PoolExecutor]), or, on a context of zero threads, on the read's own queue ([ReadQueue]),
/// which the threads using the read run. Both keep the pool's order: a walk advance at the head, a
/// page task and planning at the tail.
public sealed interface ReadExecutor permits ReadExecutor.PoolExecutor, ReadQueue {

    /// The executor of a read on `context`: its pool, or a queue of the read's own where it has
    /// none.
    static ReadExecutor forContext(HardwoodContextImpl context) {
        return context.hasPool() ? new PoolExecutor(context, new ReadTasks()) : new ReadQueue(context);
    }

    /// Runs a column's walk advance, at the head.
    ///
    /// @throws RejectedExecutionException if the context is closed
    void executeStep(Runnable step);

    /// Runs a page task with a set of the context's decode buffers, at the tail.
    ///
    /// @throws RejectedExecutionException if the context is closed
    void executePage(Consumer<DecodeBuffers> task);

    /// Runs the planning of a row group ahead of its read, at the tail.
    ///
    /// @throws RejectedExecutionException if the context is closed
    void executePlanning(Runnable task);

    /// The threads that decode the read's pages: the pool's, or the one driving the read.
    int decodeThreads();

    /// The read's walk advances and page tasks in flight, and the wait of the threads using it.
    ReadTasks tasks();

    /// The read's work on its context's pool.
    record PoolExecutor(HardwoodContextImpl context, ReadTasks tasks) implements ReadExecutor {

        @Override
        public void executeStep(Runnable step) {
            context.executeStep(step);
        }

        @Override
        public int decodeThreads() {
            return context.threads();
        }

        @Override
        public void executePage(Consumer<DecodeBuffers> task) {
            context.execute(task);
        }

        @Override
        public void executePlanning(Runnable task) {
            context.executeSpeculative(task);
        }
    }
}
