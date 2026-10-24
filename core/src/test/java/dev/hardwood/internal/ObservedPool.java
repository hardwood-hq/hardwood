/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.fail;

/// A context's pool whose threads a test sees, so that it can wait until every one of them waits for
/// its next task: with the pool's queue empty and no task running, nothing of a read whose files are
/// in memory moves until the test acts.
public final class ObservedPool {

    private static final long IDLE_DEADLINE_NANOS = TimeUnit.SECONDS.toNanos(5);

    private final Set<Thread> threads = ConcurrentHashMap.newKeySet();
    private final ThreadPoolExecutor executor;

    /// A pool of `size` started threads, named after `name`.
    public ObservedPool(int size, String name) {
        AtomicInteger counter = new AtomicInteger();
        executor = new ThreadPoolExecutor(size, size, 0L, TimeUnit.MILLISECONDS, DecodeExecutor.newWorkQueue(), r -> {
            Thread thread = new Thread(r, name + "-" + counter.getAndIncrement());
            thread.setDaemon(true);
            threads.add(thread);
            return thread;
        });
        executor.prestartAllCoreThreads();
    }

    /// A context running its tasks on this pool, which it shuts down on close.
    public HardwoodContextImpl context() {
        return HardwoodContextImpl.create(executor, executor.getCorePoolSize(), threads);
    }

    /// A context running its tasks on this pool and its I/O on `ioExecutor`.
    public HardwoodContextImpl context(Executor ioExecutor) {
        return HardwoodContextImpl.create(executor, executor.getCorePoolSize(), threads, ioExecutor);
    }

    /// Waits until the pool's queue is empty, no task runs and every thread of the pool waits for
    /// its next task, with no task ending while the threads are looked at, and fails with the stack
    /// of a busy thread by the deadline.
    ///
    /// @param busyMeans what a pool still busy at the deadline means, for the failure message
    public void awaitIdle(String busyMeans) throws InterruptedException {
        ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        long deadline = System.nanoTime() + IDLE_DEADLINE_NANOS;
        while (true) {
            long completed = executor.getCompletedTaskCount();
            ThreadInfo busy = null;
            for (Thread thread : threads) {
                ThreadInfo info = bean.getThreadInfo(thread.threadId(), Integer.MAX_VALUE);
                if (info != null && !idle(info)) {
                    busy = info;
                    break;
                }
            }
            boolean quiet = executor.getQueue().isEmpty() && executor.getActiveCount() == 0
                    && executor.getCompletedTaskCount() == completed;
            if (busy == null && quiet) {
                return;
            }
            if (System.nanoTime() > deadline) {
                fail(busyMeans + ": " + (busy == null
                        ? executor.getQueue().size() + " tasks queued, " + executor.getActiveCount() + " running"
                        : busy.getThreadName() + " " + busy.getThreadState() + "\n"
                                + Arrays.stream(busy.getStackTrace())
                                        .map(frame -> "\tat " + frame)
                                        .collect(Collectors.joining("\n"))));
            }
            Thread.sleep(1);
        }
    }

    /// Whether the thread waits for the pool's next task.
    private static boolean idle(ThreadInfo info) {
        StackTraceElement[] stack = info.getStackTrace();
        return info.getThreadState() != Thread.State.RUNNABLE && Arrays.stream(stack)
                .anyMatch(frame -> frame.getClassName().equals(ThreadPoolExecutor.class.getName())
                        && frame.getMethodName().equals("getTask"))
                && Arrays.stream(stack).noneMatch(frame -> frame.getClassName().startsWith("dev.hardwood"));
    }
}
