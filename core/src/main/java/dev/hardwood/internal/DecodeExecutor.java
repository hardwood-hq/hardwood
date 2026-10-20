/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;

/// Runs decode work as tasks of a `HardwoodContext`'s pool, each with a set of [DecodeBuffers]
/// it takes when it starts and gives back when it ends. It is the only holder of the sets outside
/// tests, so decode code, which takes them as a parameter, runs only on the pool.
///
/// At most as many tasks run at once as the pool has threads, so the executor keeps that many
/// sets: each grows to the largest page a task decoded with it and lives as long as the context.
/// A task that finds none free, which more tasks than threads running at once would need, gets a
/// new set, dropped when it is given back.
///
/// A decode a thread waits for ([#decode]) runs ahead of the tasks already queued on a pool built
/// on [#newWorkQueue()]: the waiting thread is a read that cannot go on without it, while the
/// queued tasks are read-ahead. Every other task runs in the order it arrived.
public final class DecodeExecutor {

    private final ExecutorService executor;
    private final ArrayBlockingQueue<DecodeBuffers> idle;

    /// @param executor the pool the tasks run on
    /// @param threads the pool's threads, the sets the executor keeps
    public DecodeExecutor(ExecutorService executor, int threads) {
        this.executor = executor;
        this.idle = new ArrayBlockingQueue<>(threads);
    }

    /// Runs `task` on the pool with a set of buffers.
    ///
    /// @throws RejectedExecutionException if the pool is shut down
    public void execute(Consumer<DecodeBuffers> task) {
        executor.execute(() -> {
            DecodeBuffers buffers = take();
            try {
                task.accept(buffers);
            }
            finally {
                idle.offer(buffers);
            }
        });
    }

    /// Runs `work` on the pool with a set of buffers and waits for its result. A failure of `work`
    /// is thrown as it was thrown, with the waiting thread's stack attached as a suppressed
    /// exception, so that its trace shows who asked for the decode. A task of the pool must not
    /// call it: the pool may have no other thread to run the work on.
    ///
    /// @throws RejectedExecutionException if the pool is shut down
    public <T> T decode(Function<DecodeBuffers, T> work) {
        Awaited<T> result = new Awaited<>(() -> {
            DecodeBuffers buffers = take();
            try {
                return work.apply(buffers);
            }
            finally {
                idle.offer(buffers);
            }
        });
        executor.execute(result);
        return await(result);
    }

    /// The work queue for a context's pool: a decode a thread waits for goes to its head, every
    /// other task to its tail.
    public static BlockingQueue<Runnable> newWorkQueue() {
        return new AwaitedFirstQueue();
    }

    /// A decode a thread is blocked on.
    private static final class Awaited<T> extends FutureTask<T> {

        Awaited(Callable<T> work) {
            super(work);
        }
    }

    private static final class AwaitedFirstQueue extends LinkedBlockingDeque<Runnable> {

        @Override
        public boolean offer(Runnable task) {
            return task instanceof Awaited<?> ? offerFirst(task) : offerLast(task);
        }
    }

    private DecodeBuffers take() {
        DecodeBuffers buffers = idle.poll();
        return buffers != null ? buffers : new DecodeBuffers();
    }

    /// Waits for `result` through interrupts, which it restores: the work is one page's decode.
    private static <T> T await(Future<T> result) {
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    return result.get();
                }
                catch (InterruptedException e) {
                    interrupted = true;
                }
                catch (ExecutionException e) {
                    Throwable cause = e.getCause();
                    cause.addSuppressed(new Exception("Waited for on the HardwoodContext's pool"));
                    if (cause instanceof RuntimeException runtime) {
                        throw runtime;
                    }
                    if (cause instanceof Error error) {
                        throw error;
                    }
                    throw new IllegalStateException(cause);
                }
            }
        }
        finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
