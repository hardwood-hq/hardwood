/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.InputFile;
import dev.hardwood.internal.DecodeExecutor;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/// No thread of a context's pool waits for a read of a file whose bytes are not in memory: not in
/// a read of its own, and not for a work item's planning, which waits for its reads on a virtual
/// thread. Every read is held back, one at a time, until the pool has gone idle; a thread of the
/// pool still busy then waits on the held read. The reads the caller makes on its own thread
/// before the read starts pass.
class PoolWaitTest {

    private static final Path RESOURCES = Path.of("src/test/resources");
    private static final long IDLE_DEADLINE_NANOS = TimeUnit.SECONDS.toNanos(5);

    static Stream<Arguments> reads() {
        return Stream.of(1, 2).flatMap(threads -> scenarios().map(scenario -> Arguments.of(
                scenario.get()[0], scenario.get()[1], scenario.get()[2], threads)));
    }

    private static Stream<Arguments> scenarios() {
        return Stream.of(
                Arguments.of("unfiltered", "filter_pushdown_int.parquet", null),
                Arguments.of("statistics", "filter_pushdown_int.parquet", FilterPredicate.gt("id", 150L)),
                Arguments.of("Bloom filter drops", "bloom_filter_multi_rg_test.parquet", FilterPredicate.eq("code", 5)),
                Arguments.of("Bloom filter keeps", "bloom_filter_multi_rg_test.parquet", FilterPredicate.eq("code", 3)),
                Arguments.of("dictionary drops", "column_index_pushdown_dict.parquet", FilterPredicate.eq("category", "cat_5x")),
                Arguments.of("dictionary keeps", "column_index_pushdown_dict.parquet", FilterPredicate.eq("category", "cat_5")),
                Arguments.of("page index", "column_index_pushdown.parquet", FilterPredicate.lt("id", 1000L)),
                Arguments.of("row groups with a page index", "multi_row_group_page_index.parquet", null),
                Arguments.of("sequential pages", "misaligned_pages_dict_no_index.parquet", null));
    }

    /// Reads three copies of the file as one read, so that later files and their row groups are
    /// planned while the read runs. On one thread of the pool, planning a file, which holds the
    /// planning lock while its CPU work runs on the pool, must not find the thread waiting for it.
    @ParameterizedTest(name = "{0}, {3} threads")
    @MethodSource("reads")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void noThreadOfThePoolWaitsForARead(String name, String file, FilterPredicate filter, int threads)
            throws Exception {
        Path path = RESOURCES.resolve(file);
        long expected = countRows(List.of(InputFile.of(path), InputFile.of(path), InputFile.of(path)),
                HardwoodContextImpl.create(2), filter);

        Set<Thread> pool = ConcurrentHashMap.newKeySet();
        AtomicInteger counter = new AtomicInteger();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(threads, threads, 0L, TimeUnit.MILLISECONDS,
                DecodeExecutor.newWorkQueue(), r -> {
                    Thread t = new Thread(r, "pool-wait-test-" + counter.getAndIncrement());
                    t.setDaemon(true);
                    pool.add(t);
                    return t;
                });
        executor.prestartAllCoreThreads();
        Gate gate = new Gate();
        List<InputFile> files = List.of(new HeldInputFile(path, gate), new HeldInputFile(path, gate),
                new HeldInputFile(path, gate));

        AtomicLong rows = new AtomicLong();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread caller = Thread.ofPlatform().name("pool-wait-test-caller").unstarted(() -> {
            try {
                rows.set(countRows(files, HardwoodContextImpl.create(executor, threads, pool), filter));
            }
            catch (Throwable e) {
                failure.set(e);
            }
        });
        gate.caller = caller;
        caller.start();

        int released = 0;
        while (caller.isAlive()) {
            if (!gate.awaitHeld()) {
                continue;
            }
            awaitIdle(pool);
            gate.releaseOne();
            released++;
        }
        caller.join();
        if (failure.get() != null) {
            throw new AssertionError("The read failed", failure.get());
        }
        assertThat(rows.get()).isEqualTo(expected);
        assertThat(released).as("reads held back").isPositive();
    }

    private static long countRows(List<InputFile> files, HardwoodContextImpl context, FilterPredicate filter)
            throws IOException {
        try (context;
                ParquetFileReader reader = ParquetFileReader.openAll(files, context);
                RowReader rows = filter != null
                        ? reader.buildRowReader().filter(filter).build()
                        : reader.buildRowReader().build()) {
            long count = 0;
            while (rows.hasNext()) {
                rows.next();
                count++;
            }
            return count;
        }
    }

    /// Waits until every thread of `pool` is idle, waiting for its next task, and fails with the
    /// stack of one that is not by the deadline.
    private static void awaitIdle(Set<Thread> pool) throws InterruptedException {
        ThreadMXBean threads = ManagementFactory.getThreadMXBean();
        long deadline = System.nanoTime() + IDLE_DEADLINE_NANOS;
        while (true) {
            ThreadInfo busy = null;
            for (Thread thread : pool) {
                ThreadInfo info = threads.getThreadInfo(thread.threadId(), Integer.MAX_VALUE);
                if (info != null && !idle(info)) {
                    busy = info;
                    break;
                }
            }
            if (busy == null) {
                return;
            }
            if (System.nanoTime() > deadline) {
                fail("A thread of the pool waits while a read is held back: " + busy.getThreadName() + " "
                        + busy.getThreadState() + "\n" + Arrays.stream(busy.getStackTrace())
                                .map(frame -> "\tat " + frame)
                                .collect(Collectors.joining("\n")));
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

    /// Holds every read made off the caller's thread until the test releases it.
    private static final class Gate {

        private final LinkedBlockingDeque<Semaphore> held = new LinkedBlockingDeque<>();
        volatile Thread caller;

        void pass() throws IOException {
            if (Thread.currentThread() == caller) {
                return;
            }
            Semaphore release = new Semaphore(0);
            held.add(release);
            try {
                release.acquire();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while held", e);
            }
        }

        /// Whether a read is held, waiting briefly for one.
        boolean awaitHeld() throws InterruptedException {
            Semaphore first = held.peekFirst();
            if (first == null) {
                Thread.sleep(1);
                return held.peekFirst() != null;
            }
            return true;
        }

        void releaseOne() {
            held.removeFirst().release();
        }
    }

    /// A file not in memory whose every read passes [Gate].
    private static final class HeldInputFile implements InputFile {

        private final InputFile delegate;
        private final Gate gate;

        HeldInputFile(Path path, Gate gate) {
            this.delegate = InputFile.of(path);
            this.gate = gate;
        }

        @Override
        public void open() throws IOException {
            delegate.open();
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            gate.pass();
            return delegate.readRange(offset, length);
        }

        @Override
        public long length() throws IOException {
            return delegate.length();
        }

        @Override
        public String name() {
            return delegate.name();
        }

        @Override
        public Optional<String> identity() throws IOException {
            return delegate.identity();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
