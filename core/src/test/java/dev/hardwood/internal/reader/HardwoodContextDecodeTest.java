/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import dev.hardwood.InputFile;
import dev.hardwood.internal.DecodeBuffers;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// [HardwoodContextImpl#decode] runs its work on the context's pool, with a set of the context's
/// decode buffers, and hands back the work's result or its failure as thrown.
class HardwoodContextDecodeTest {

    /// A Snappy dictionary page in a file of one row group; the filter's probe reads it.
    private static final Path FILE = Path.of("src/test/resources/dict_compressed_page.parquet");

    /// A set of buffers goes back to the context when its task ends, for the next.
    @Test
    void decodesOnAPoolThreadWithItsBuffers() {
        try (HardwoodContextImpl context = HardwoodContextImpl.create(1)) {
            Thread caller = Thread.currentThread();
            AtomicReference<DecodeBuffers> handed = new AtomicReference<>();

            Thread decoder = context.decode(buffers -> {
                handed.set(buffers);
                return Thread.currentThread();
            });

            assertThat(decoder.getName()).startsWith("hardwood-");
            assertThat(decoder).isNotSameAs(caller);
            DecodeBuffers again = context.decode(buffers -> buffers);
            assertThat(again).as("the set the last task gave back").isSameAs(handed.get());
        }
    }

    /// A computation a thread off the pool hands over runs on a pool thread; one a pool thread asks
    /// for runs in place, as the pool would otherwise need a thread its caller holds.
    @Test
    void computesOnAPoolThreadAndInPlaceOnOne() throws Exception {
        try (HardwoodContextImpl context = HardwoodContextImpl.create(1)) {
            Thread caller = Thread.currentThread();

            Thread runner = context.compute("test", Thread::currentThread);

            assertThat(runner.getName()).startsWith("hardwood-");
            assertThat(runner).isNotSameAs(caller);
            AtomicReference<Thread[]> threads = new AtomicReference<>();
            CountDownLatch done = new CountDownLatch(1);
            context.execute(buffers -> {
                threads.set(new Thread[] {Thread.currentThread(), context.compute("nested", Thread::currentThread)});
                done.countDown();
            });
            assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(threads.get()[1]).isSameAs(threads.get()[0]);
        }
    }

    /// A computation's failure reaches the waiting thread as it was thrown, and a computation
    /// asked for once the context is closed fails as a read on a closed context does.
    @Test
    void computeThrowsTheFailureAndFailsOnAClosedContext() {
        IllegalArgumentException failure = new IllegalArgumentException("malformed index");
        HardwoodContextImpl context = HardwoodContextImpl.create(1);

        assertThatThrownBy(() -> context.compute("test", () -> {
            throw failure;
        })).isSameAs(failure).hasMessage("malformed index");

        context.close();
        assertThatThrownBy(() -> context.compute("test", () -> "late"))
                .isExactlyInstanceOf(IllegalStateException.class)
                .hasMessage("The HardwoodContext was closed while a reader using it was reading")
                .hasCauseInstanceOf(RejectedExecutionException.class);
    }

    /// A decode a thread waits for runs ahead of the tasks queued before it, which keep their
    /// order: they are read-ahead, and the waiting read can go on only once its decode is done.
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aDecodeAThreadWaitsForRunsAheadOfQueuedTasks() throws Exception {
        try (HardwoodContextImpl context = HardwoodContextImpl.create(1)) {
            CountDownLatch release = new CountDownLatch(1);
            CountDownLatch running = new CountDownLatch(1);
            List<String> order = Collections.synchronizedList(new ArrayList<>());
            context.executor().execute(() -> {
                running.countDown();
                awaitQuietly(release);
            });
            assertThat(running.await(10, TimeUnit.SECONDS)).isTrue();
            for (int i = 0; i < 3; i++) {
                String task = "queued " + i;
                context.execute(buffers -> order.add(task));
            }
            Thread waiter = Thread.ofPlatform().start(() -> context.decode(buffers -> order.add("awaited decode")));
            while (((ThreadPoolExecutor) context.executor()).getQueue().size() < 4) {
                Thread.onSpinWait();
            }

            release.countDown();
            waiter.join();
            context.execute(buffers -> order.add("last"));
            while (order.size() < 5) {
                Thread.onSpinWait();
            }

            assertThat(order).containsExactly("awaited decode", "queued 0", "queued 1", "queued 2", "last");
        }
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await(10, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /// Tasks running at once each have a set of their own.
    @Test
    void givesTasksRunningAtOnceSetsOfTheirOwn() throws Exception {
        try (HardwoodContextImpl context = HardwoodContextImpl.create(2)) {
            CountDownLatch bothRunning = new CountDownLatch(2);
            AtomicReference<DecodeBuffers> first = new AtomicReference<>();
            AtomicReference<DecodeBuffers> second = new AtomicReference<>();
            context.execute(buffers -> {
                first.set(buffers);
                awaitBoth(bothRunning);
            });
            context.execute(buffers -> {
                second.set(buffers);
                awaitBoth(bothRunning);
            });

            assertThat(bothRunning.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(first.get()).isNotNull().isNotSameAs(second.get());
        }
    }

    private static void awaitBoth(CountDownLatch bothRunning) {
        bothRunning.countDown();
        try {
            bothRunning.await(10, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /// The failure carries the waiting thread's stack as a suppressed exception, so that its trace
    /// shows who asked for the decode.
    @Test
    void throwsTheWorksFailureAsItWasThrown() {
        try (HardwoodContextImpl context = HardwoodContextImpl.create(1)) {
            IllegalArgumentException failure = new IllegalArgumentException("bad page");
            AssertionError error = new AssertionError("bad value");

            assertThatThrownBy(() -> context.decode(buffers -> {
                throw failure;
            })).isSameAs(failure);
            assertThatThrownBy(() -> context.decode(buffers -> {
                throw error;
            })).isSameAs(error);

            for (Throwable thrown : new Throwable[]{ failure, error }) {
                assertThat(thrown.getSuppressed()).singleElement().satisfies(waiter -> {
                    assertThat(waiter).hasMessage("Waited for on the HardwoodContext's pool");
                    assertThat(waiter.getStackTrace()).anyMatch(frame ->
                            frame.getMethodName().equals("throwsTheWorksFailureAsItWasThrown"));
                });
            }
        }
    }

    /// A read waits for the pool's threads, so on one of them it would wait for itself: building it
    /// there fails. A decode asked for there runs in place.
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aReadOnTheContextsOwnPoolFailsInsteadOfHanging() throws Exception {
        String message = "A read cannot run on a thread of the HardwoodContext it reads with, whose threads "
                + "it waits for: run it on another thread, or read with another context";
        try (HardwoodContextImpl context = HardwoodContextImpl.create(1)) {
            Future<?> read = context.executor().submit(() -> {
                try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE), context);
                     ColumnReader values = reader.buildColumnReader("label")
                             .filter(FilterPredicate.eq("label", "pad_1_" + "z".repeat(40))).build()) {
                    return values.nextBatch();
                }
            });
            Future<Thread> decode = context.executor().submit(() -> context.decode(buffers -> Thread.currentThread()));

            assertThatThrownBy(read::get)
                    .isInstanceOf(ExecutionException.class)
                    .cause()
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage(message);
            assertThat(decode.get().getName()).as("thread running the decode").startsWith("hardwood-");
        }
    }

    @Test
    void aClosedContextFailsTheDecode() {
        HardwoodContextImpl context = HardwoodContextImpl.create(1);
        context.close();

        assertThatThrownBy(() -> context.decode(buffers -> "decoded"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("The HardwoodContext was closed while a reader using it was reading")
                .hasCauseInstanceOf(RejectedExecutionException.class);
    }

    @Test
    void waitsThroughAnInterruptAndRestoresIt() {
        try (HardwoodContextImpl context = HardwoodContextImpl.create(1)) {
            Thread.currentThread().interrupt();
            try {
                String decoded = context.decode(buffers -> "decoded");
                assertThat(decoded).isEqualTo("decoded");
                assertThat(Thread.currentThread().isInterrupted()).as("interrupt restored").isTrue();
            }
            finally {
                Thread.interrupted();
            }
        }
    }
}
