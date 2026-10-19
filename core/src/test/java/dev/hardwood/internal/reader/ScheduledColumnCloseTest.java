/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import dev.hardwood.InputFile;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// A caller that is interrupted when it closes a [ScheduledColumn], as a cancelled task is when
/// its try-with-resources block runs, still gets the guarantee of [ScheduledColumn#close()]: the
/// fetcher has stopped reading the `InputFile` by the time `close()` returns, and the caller's
/// interrupt is restored.
class ScheduledColumnCloseTest {

    private static final Path FILE = Path.of("src/test/resources/page_index_test.parquet");

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void closeWaitsForTheFetcherWhenTheCallerIsInterrupted() throws Exception {
        try (HardwoodContextImpl context = HardwoodContextImpl.create();
             ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE))) {
            FileSchema schema = reader.getFileSchema();
            ColumnSchema column = schema.getColumn(0);
            InputFile inputFile = InputFile.of(FILE);
            inputFile.open();
            RowGroupIterator iterator = new RowGroupIterator(List.of(inputFile), context, 0);
            iterator.setFirstFile(schema, reader.getFileMetaData().rowGroups());
            iterator.initialize(ProjectedSchema.create(schema, ColumnProjection.all()), null);
            int batchCapacity = 64;
            iterator.planBatches(batchCapacity, 0);

            // The fetcher stalls in its first page read, so close() can only return early by
            // abandoning its wait for the fetcher.
            CountDownLatch readStalled = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            AtomicBoolean readReturned = new AtomicBoolean();
            PageSource stallingSource = new PageSource(iterator, 0) {
                private boolean first = true;

                @Override
                public PageInfo next() throws IOException {
                    if (first) {
                        first = false;
                        readStalled.countDown();
                        try {
                            release.await();
                        }
                        catch (InterruptedException e) {
                            throw new InterruptedIOException();
                        }
                        finally {
                            readReturned.set(true);
                        }
                    }
                    return super.next();
                }
            };
            BatchExchange<BatchExchange.Batch> exchange = BatchExchange.detachingFlat(column.name(), batchCapacity);
            ReadScheduler scheduler = new ReadScheduler(context, batchCapacity, batchCapacity);
            ScheduledFlatColumn scheduled = new ScheduledFlatColumn(scheduler, stallingSource, exchange, column,
                    batchCapacity, context.decompressorFactory(), null);
            scheduler.start();
            assertThat(readStalled.await(5, TimeUnit.SECONDS)).as("the fetcher is held inside a page read").isTrue();

            AtomicBoolean readReturnedOnClose = new AtomicBoolean();
            AtomicBoolean interruptedOnClose = new AtomicBoolean();
            Thread closer = Thread.ofVirtual().start(() -> {
                Thread.currentThread().interrupt();
                scheduled.close();
                readReturnedOnClose.set(readReturned.get());
                interruptedOnClose.set(Thread.currentThread().isInterrupted());
            });
            try {
                awaitParkedOrExited(closer);
            }
            finally {
                release.countDown();
            }
            closer.join(TimeUnit.SECONDS.toMillis(10));

            assertThat(closer.isAlive()).as("close() returns once the fetcher exits").isFalse();
            assertThat(readReturnedOnClose.get()).as("close() does not return while the fetcher reads").isTrue();
            assertThat(interruptedOnClose.get()).as("close() restores the caller's interrupt").isTrue();
            iterator.close();
            inputFile.close();
        }
    }

    /// Closing one column of a read stops the read's other columns before it waits for its own,
    /// so that a reader closing its columns one after another waits for all of them at once
    /// rather than for each in turn.
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void closingOneColumnStopsEveryColumnOfTheRead() throws Exception {
        try (HardwoodContextImpl context = HardwoodContextImpl.create();
             ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE))) {
            FileSchema schema = reader.getFileSchema();
            InputFile inputFile = InputFile.of(FILE);
            inputFile.open();
            RowGroupIterator iterator = new RowGroupIterator(List.of(inputFile), context, 0);
            iterator.setFirstFile(schema, reader.getFileMetaData().rowGroups());
            iterator.initialize(ProjectedSchema.create(schema, ColumnProjection.all()), null);
            int batchCapacity = 64;
            iterator.planBatches(batchCapacity, 0);

            // Both fetchers are held in their first page read, so neither column ends on its own
            // and the first close cannot return while its fetcher is held.
            CountDownLatch readsStalled = new CountDownLatch(2);
            CountDownLatch release = new CountDownLatch(1);
            ReadScheduler scheduler = new ReadScheduler(context, batchCapacity, batchCapacity);
            ScheduledFlatColumn first = stalledColumn(scheduler, iterator, 0, schema.getColumn(0),
                    BatchExchange.detachingFlat(schema.getColumn(0).name(), batchCapacity), batchCapacity, readsStalled,
                    release, context);
            BatchExchange<BatchExchange.Batch> secondExchange = BatchExchange.detachingFlat(
                    schema.getColumn(1).name(), batchCapacity);
            ScheduledFlatColumn second = stalledColumn(scheduler, iterator, 1, schema.getColumn(1), secondExchange,
                    batchCapacity, readsStalled, release, context);
            scheduler.start();
            assertThat(readsStalled.await(5, TimeUnit.SECONDS)).as("both fetchers are held inside a page read")
                    .isTrue();

            Thread closer = Thread.ofVirtual().start(first::close);
            try {
                awaitParkedOrExited(closer);
                assertThat(closer.isAlive()).as("the first close waits for its held fetcher").isTrue();
                assertThat(second.isDone()).as("the second column is stopped by the first close").isTrue();
                assertThat(secondExchange.isFinished())
                        .as("the second column's consumer sees the end while the first close waits")
                        .isTrue();
            }
            finally {
                release.countDown();
            }
            closer.join(TimeUnit.SECONDS.toMillis(10));
            assertThat(closer.isAlive()).as("close() returns once the fetcher exits").isFalse();
            second.close();
            iterator.close();
            inputFile.close();
        }
    }

    /// A stop that reaches a fetcher waiting for room while the fetcher takes the column lock still
    /// ends the fetcher. The lock acquire parks, and the stop's unpark wakes that park rather than
    /// the fetcher's own wait, so the fetcher has to read the stop after the lock, not before.
    ///
    /// The single decode thread is held, so the column's first page task never runs, the
    /// read-ahead depth submits no other page, and the fetcher fills its queue and waits for room.
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void stopReachesAFetcherTakingTheColumnLock() throws Exception {
        try (HardwoodContextImpl context = HardwoodContextImpl.create(1);
             ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE))) {
            CountDownLatch releaseWorker = holdTheDecodeThread(context);
            AtomicReference<Thread> fetcherThread = new AtomicReference<>();
            try (HeldRead read = HeldRead.start(context, reader, iterator -> new PageSource(iterator, 0) {
                @Override
                public PageInfo next() throws IOException {
                    fetcherThread.set(Thread.currentThread());
                    return super.next();
                }
            })) {
                ScheduledFlatColumn scheduled = read.column();
                try {
                    awaitTrue(() -> scheduled.queued() == ScheduledColumn.FETCH_AHEAD && fetcherThread.get() != null
                            && fetcherThread.get().getState() == Thread.State.WAITING
                            && LockSupport.getBlocker(fetcherThread.get()) == scheduled,
                            "the fetcher waits for room in its full queue");
                    Thread fetcher = fetcherThread.get();

                    scheduled.lock.lock();
                    try {
                        scheduled.unparkFetcher();
                        awaitTrue(() -> parkedOnTheColumnLock(fetcher, scheduled),
                                "the fetcher re-checks for room and waits for the column lock");
                        scheduled.fail(new IllegalStateException("stopped"));
                        // The stop unparked the fetcher inside the lock acquire. An unparked
                        // virtual thread reports RUNNABLE until it parks again, so once it waits
                        // on the lock again, the acquire has consumed the stop's unpark and no
                        // permit is pending.
                        awaitTrue(() -> parkedOnTheColumnLock(fetcher, scheduled),
                                "the fetcher waits for the column lock again after the stop");
                    }
                    finally {
                        scheduled.lock.unlock();
                    }
                    fetcher.join(TimeUnit.SECONDS.toMillis(5));

                    assertThat(fetcher.isAlive()).as("the fetcher sees the stop once it has the lock").isFalse();
                }
                finally {
                    releaseWorker.countDown();
                    scheduled.close();
                }
            }
        }
    }

    /// Two threads closing one column at once, as a reader and its parent can, both return, and
    /// only once the column's page task has finished. The single decode thread is held, so the
    /// column's first page task is submitted and cannot run until it is released.
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void concurrentClosesBothReturnOnceThePageTaskFinishes() throws Exception {
        try (HardwoodContextImpl context = HardwoodContextImpl.create(1);
             ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE))) {
            CountDownLatch releaseWorker = holdTheDecodeThread(context);
            try (HeldRead read = HeldRead.start(context, reader, iterator -> new PageSource(iterator, 0))) {
                ScheduledFlatColumn scheduled = read.column();
                Thread first;
                Thread second;
                try {
                    awaitTrue(() -> scheduled.pagesAhead() > 0, "a page task is submitted");
                    first = Thread.ofPlatform().start(scheduled::close);
                    awaitTrue(() -> LockSupport.getBlocker(first) == scheduled,
                            "the first close claimed the column and waits for its page task");
                    second = Thread.ofVirtual().start(scheduled::close);
                    awaitParkedOrExited(second);
                    assertThat(second.isAlive()).as("the second close waits for the claimed close").isTrue();
                }
                finally {
                    releaseWorker.countDown();
                }
                first.join(TimeUnit.SECONDS.toMillis(5));
                second.join(TimeUnit.SECONDS.toMillis(5));

                assertThat(first.isAlive()).as("the first close returns once the page task finished").isFalse();
                assertThat(second.isAlive()).as("the second close returns once the page task finished").isFalse();
            }
        }
    }

    /// A close interrupted while it waits for the column's page task goes on waiting without
    /// spinning, and returns with the interrupt restored once the task has finished.
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void interruptedCloseWaitsWithoutSpinning() throws Exception {
        try (HardwoodContextImpl context = HardwoodContextImpl.create(1);
             ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE))) {
            CountDownLatch releaseWorker = holdTheDecodeThread(context);
            try (HeldRead read = HeldRead.start(context, reader, iterator -> new PageSource(iterator, 0))) {
                ScheduledFlatColumn scheduled = read.column();
                AtomicBoolean interruptedOnReturn = new AtomicBoolean();
                Thread closer;
                try {
                    awaitTrue(() -> scheduled.pagesAhead() > 0, "a page task is submitted");
                    closer = Thread.ofPlatform().start(() -> {
                        scheduled.close();
                        interruptedOnReturn.set(Thread.currentThread().isInterrupted());
                    });
                    awaitTrue(() -> LockSupport.getBlocker(closer) == scheduled,
                            "the close waits for the column's page task");
                    closer.interrupt();
                    awaitTrue(() -> closer.getState() == Thread.State.WAITING
                            && LockSupport.getBlocker(closer) == scheduled && !closer.isInterrupted(),
                            "the close takes the interrupt and waits again");

                    ThreadMXBean threads = ManagementFactory.getThreadMXBean();
                    long cpuBefore = threads.getThreadCpuTime(closer.threadId());
                    Thread.sleep(200);
                    long cpuSpent = threads.getThreadCpuTime(closer.threadId()) - cpuBefore;
                    assertThat(cpuSpent).as("CPU time the waiting close spends in 200 ms, in ns")
                            .isLessThan(TimeUnit.MILLISECONDS.toNanos(20));
                }
                finally {
                    releaseWorker.countDown();
                }
                closer.join(TimeUnit.SECONDS.toMillis(5));

                assertThat(closer.isAlive()).as("the close returns once the page task finished").isFalse();
                assertThat(interruptedOnReturn.get()).as("the close restores the interrupt").isTrue();
            }
        }
    }

    /// One column of the first file's first leaf, its pipeline started on `context`, reading its
    /// pages through the [PageSource] `source` builds.
    private record HeldRead(ScheduledFlatColumn column, RowGroupIterator iterator, InputFile inputFile)
            implements AutoCloseable {

        private static final int BATCH_CAPACITY = 64;

        static HeldRead start(HardwoodContextImpl context, ParquetFileReader reader,
                              Function<RowGroupIterator, PageSource> source) throws IOException {
            FileSchema schema = reader.getFileSchema();
            ColumnSchema column = schema.getColumn(0);
            InputFile inputFile = InputFile.of(FILE);
            inputFile.open();
            RowGroupIterator iterator = new RowGroupIterator(List.of(inputFile), context, 0);
            iterator.setFirstFile(schema, reader.getFileMetaData().rowGroups());
            iterator.initialize(ProjectedSchema.create(schema, ColumnProjection.all()), null);
            iterator.planBatches(BATCH_CAPACITY, 0);
            BatchExchange<BatchExchange.Batch> exchange = BatchExchange.detachingFlat(column.name(), BATCH_CAPACITY);
            ReadScheduler scheduler = new ReadScheduler(context, BATCH_CAPACITY, BATCH_CAPACITY);
            ScheduledFlatColumn scheduled = new ScheduledFlatColumn(scheduler, source.apply(iterator), exchange,
                    column, BATCH_CAPACITY, context.decompressorFactory(), null);
            scheduler.start();
            return new HeldRead(scheduled, iterator, inputFile);
        }

        @Override
        public void close() throws IOException {
            iterator.close();
            inputFile.close();
        }
    }

    /// Whether `fetcher` is parked in an acquire of `scheduled`'s column lock.
    private static boolean parkedOnTheColumnLock(Thread fetcher, ScheduledColumn<?> scheduled) {
        return fetcher.getState() == Thread.State.WAITING
                && LockSupport.getBlocker(fetcher) instanceof AbstractQueuedSynchronizer
                && scheduled.lock.hasQueuedThread(fetcher);
    }

    /// Occupies the context's single decode thread until the returned latch is counted down.
    private static CountDownLatch holdTheDecodeThread(HardwoodContextImpl context) throws InterruptedException {
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch held = new CountDownLatch(1);
        context.executor().execute(() -> {
            held.countDown();
            try {
                release.await();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        assertThat(held.await(5, TimeUnit.SECONDS)).as("the decode thread is held").isTrue();
        return release;
    }

    /// A column of `scheduler` whose fetcher's first page read counts `stalled` down and is held
    /// until `release`.
    private static ScheduledFlatColumn stalledColumn(ReadScheduler scheduler, RowGroupIterator iterator, int index,
                                                     ColumnSchema column, BatchExchange<BatchExchange.Batch> exchange,
                                                     int batchCapacity, CountDownLatch stalled, CountDownLatch release,
                                                     HardwoodContextImpl context) {
        PageSource source = new PageSource(iterator, index) {
            private boolean first = true;

            @Override
            public PageInfo next() throws IOException {
                if (first) {
                    first = false;
                    stalled.countDown();
                    try {
                        release.await();
                    }
                    catch (InterruptedException e) {
                        throw new InterruptedIOException();
                    }
                }
                return super.next();
            }
        };
        return new ScheduledFlatColumn(scheduler, source, exchange, column, batchCapacity,
                context.decompressorFactory(), null);
    }

    private static void awaitParkedOrExited(Thread thread) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            if (!thread.isAlive() || thread.getState() == Thread.State.WAITING
                    || thread.getState() == Thread.State.TIMED_WAITING) {
                return;
            }
            Thread.sleep(1);
        }
        throw new AssertionError("thread neither exited nor parked");
    }

    private static void awaitTrue(BooleanSupplier condition, String what) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting until " + what);
            }
            Thread.sleep(1);
        }
    }
}
