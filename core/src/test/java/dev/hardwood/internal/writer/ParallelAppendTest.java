/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/// A slice's columns are appended on a pool's workers and the calling thread together, each
/// exactly once, and a failure reaches the caller only once no thread still appends.
class ParallelAppendTest {

    private static final Duration NO_HANG = Duration.ofSeconds(60);
    private static final int COLUMNS = 12;

    private final ExecutorService pool = Executors.newFixedThreadPool(4);

    @AfterEach
    void shutDown() throws InterruptedException {
        pool.shutdownNow();
        assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }

    /// Counted in a plain array, as a column chunk keeps its state: a column appended by different
    /// threads in consecutive slices sees what the previous slice left, and the caller sees what
    /// every column holds once a slice returns.
    @Test
    void appendsEveryColumnOncePerSlice() {
        ParallelAppend parallel = new ParallelAppend(COLUMNS, pool, 4);
        int[] runs = new int[COLUMNS];
        for (int slice = 1; slice <= 2_000; slice++) {
            parallel.append(c -> runs[c]++);
            for (int c = 0; c < COLUMNS; c++) {
                assertThat(runs[c]).as("slice %d, column %d", slice, c).isEqualTo(slice);
            }
        }
        parallel.retire();
    }

    /// A worker that cannot be started fails the slice, but only once every column claimed has
    /// finished, the caller taking those no worker took.
    @Test
    void finishesTheSliceBeforeReportingAWorkerThatCannotStart() throws Exception {
        AtomicInteger accepted = new AtomicInteger();
        ExecutorService rejecting = new ThreadPoolExecutor(2, 2, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<>()) {
            @Override
            public void execute(Runnable command) {
                if (accepted.getAndIncrement() > 0) {
                    throw new RejectedExecutionException("pool is full");
                }
                super.execute(command);
            }
        };
        try {
            ParallelAppend parallel = new ParallelAppend(COLUMNS, rejecting, 2);
            AtomicIntegerArray runs = new AtomicIntegerArray(COLUMNS);
            AtomicInteger appending = new AtomicInteger();
            assertTimeoutPreemptively(NO_HANG, () -> assertThatThrownBy(() -> parallel.append(c -> {
                appending.incrementAndGet();
                sleep();
                runs.incrementAndGet(c);
                appending.decrementAndGet();
            }))
                    .isInstanceOf(RejectedExecutionException.class)
                    .hasMessage("pool is full"));
            assertThat(appending).hasValue(0);
            for (int c = 0; c < COLUMNS; c++) {
                assertThat(runs.get(c)).as("column %d", c).isEqualTo(1);
            }
            parallel.retire();
        }
        finally {
            rejecting.shutdownNow();
        }
    }

    /// A pool whose one thread is held by other work never runs a worker, so the caller appends
    /// every column itself, and no slice waits for a worker to start.
    @Test
    void appendsOnTheCallerAloneWhileThePoolIsBusy() throws Exception {
        ExecutorService busy = Executors.newFixedThreadPool(1);
        CountDownLatch release = new CountDownLatch(1);
        try {
            busy.submit(() -> {
                release.await();
                return null;
            });
            ParallelAppend parallel = new ParallelAppend(COLUMNS, busy, 1);
            AtomicInteger offCaller = new AtomicInteger();
            assertTimeoutPreemptively(NO_HANG, () -> {
                // Taken here: assertTimeoutPreemptively runs this on a thread of its own.
                Thread caller = Thread.currentThread();
                for (int slice = 0; slice < 100; slice++) {
                    parallel.append(c -> {
                        if (Thread.currentThread() != caller) {
                            offCaller.incrementAndGet();
                        }
                    });
                }
            });
            assertThat(offCaller).hasValue(0);
        }
        finally {
            release.countDown();
            busy.shutdownNow();
        }
    }

    /// Of three columns the last two fail, the third first. Whichever thread appends which, the
    /// caller sees the second column's failure, as a serial append would raise it. Repeated, since
    /// which thread takes which column varies.
    @Test
    void reportsTheFirstFailureInSchemaOrder() {
        for (int r = 0; r < 50; r++) {
            int run = r;
            ParallelAppend parallel = new ParallelAppend(3, pool, 2);
            CountDownLatch thirdFailed = new CountDownLatch(1);
            assertTimeoutPreemptively(NO_HANG, () -> assertThatThrownBy(() -> parallel.append(c -> {
                if (c == 1) {
                    await(thirdFailed);
                    throw new IllegalStateException("column 1 failed");
                }
                if (c == 2) {
                    thirdFailed.countDown();
                    throw new IllegalStateException("column 2 failed");
                }
            }))
                    .as("run %d", run)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("column 1 failed"));
            parallel.retire();
        }
    }

    /// The caller fails while a worker is inside a column; that column must be finished before the
    /// failure leaves, or a worker would still be appending to the row group.
    @Test
    void waitsForClaimedColumnsBeforeReportingAFailure() {
        ParallelAppend parallel = new ParallelAppend(COLUMNS, pool, 4);
        Thread caller = Thread.currentThread();
        CountDownLatch workerEntered = new CountDownLatch(1);
        CountDownLatch callerFailed = new CountDownLatch(1);
        AtomicInteger appending = new AtomicInteger();
        assertThatThrownBy(() -> parallel.append(c -> {
            if (Thread.currentThread() == caller) {
                await(workerEntered);
                callerFailed.countDown();
                throw new IllegalStateException("column failed on the caller's thread");
            }
            appending.incrementAndGet();
            try {
                workerEntered.countDown();
                await(callerFailed);
                sleep();
            }
            finally {
                appending.decrementAndGet();
            }
        }))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("column failed on the caller's thread");
        assertThat(appending).hasValue(0);
        parallel.retire();
    }

    /// A worker waits for the next slice only for a bounded time, so a writer that stops writing
    /// gives the pool its threads back without a call.
    @Test
    void workersLeaveThePoolWhenNoSliceFollows() throws Exception {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) pool;
        ParallelAppend parallel = new ParallelAppend(COLUMNS, pool, 4);
        Thread caller = Thread.currentThread();
        // Every column waits until each of the four workers has taken one, so all four are
        // running when the slice ends.
        CountDownLatch workersIn = new CountDownLatch(4);
        parallel.append(c -> {
            if (Thread.currentThread() != caller) {
                workersIn.countDown();
            }
            await(workersIn);
        });

        assertTimeoutPreemptively(NO_HANG, () -> {
            while (executor.getActiveCount() > 0) {
                Thread.sleep(1);
            }
        });
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(30, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timed out waiting for the other thread");
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static void sleep() {
        try {
            Thread.sleep(50);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
