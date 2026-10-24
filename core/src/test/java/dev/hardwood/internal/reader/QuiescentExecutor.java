/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/// Runs each task on a virtual thread of its own, as a context's I/O executor does, and waits
/// until none is running ([#awaitQuiescence]); for tests that check what speculative work did.
final class QuiescentExecutor implements Executor {

    /// Tasks submitted and not finished. Guarded by `this`.
    private int running;

    @Override
    public void execute(Runnable task) {
        synchronized (this) {
            running++;
        }
        Thread.ofVirtual().start(() -> {
            try {
                task.run();
            }
            finally {
                finished();
            }
        });
    }

    private synchronized void finished() {
        running--;
        notifyAll();
    }

    /// Waits until no task is running. A task a running one submits counts from its submission,
    /// so a chain of tasks is waited for to its end.
    ///
    /// @return whether that happened within `timeout`
    synchronized boolean awaitQuiescence(long timeout, TimeUnit unit) throws InterruptedException {
        return awaitQuiescence(timeout, unit, () -> true);
    }

    /// Waits until no task is running and `atRest` holds at once, such as a read's planning
    /// that submits the tasks: checked while no task can be submitted, so where only the tasks
    /// and `atRest`'s owner start more work, and `atRest` sees work its owner is about to resume,
    /// nothing runs any more.
    ///
    /// @return whether that happened within `timeout`
    synchronized boolean awaitQuiescence(long timeout, TimeUnit unit, BooleanSupplier atRest)
            throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        while (running > 0 || !atRest.getAsBoolean()) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                return false;
            }
            // Woken by a task's end; a change of `atRest` alone is looked for each millisecond
            TimeUnit.NANOSECONDS.timedWait(this, Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(1)));
        }
        return true;
    }
}
