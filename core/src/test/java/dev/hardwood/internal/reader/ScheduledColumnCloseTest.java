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
/// column's fetch steps have stopped reading the `InputFile` by the time `close()` returns, and
/// the caller's interrupt is restored.
class ScheduledColumnCloseTest {

    private static final Path FILE = Path.of("src/test/resources/page_index_test.parquet");

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void closeWaitsForTheFetchStepWhenTheCallerIsInterrupted() throws Exception {
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

            // The fetch step stalls in its first page read, so close() can only return early by
            // abandoning its wait for the step.
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
            assertThat(readStalled.await(5, TimeUnit.SECONDS)).as("the fetch step is held inside a page read").isTrue();

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

            assertThat(closer.isAlive()).as("close() returns once the fetch step returns").isFalse();
            assertThat(readReturnedOnClose.get()).as("close() does not return while the fetch step reads").isTrue();
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
        try (HardwoodContextImpl context = HardwoodContextImpl.create(2);
             ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE))) {
            FileSchema schema = reader.getFileSchema();
            InputFile inputFile = InputFile.of(FILE);
            inputFile.open();
            RowGroupIterator iterator = new RowGroupIterator(List.of(inputFile), context, 0);
            iterator.setFirstFile(schema, reader.getFileMetaData().rowGroups());
            iterator.initialize(ProjectedSchema.create(schema, ColumnProjection.all()), null);
            int batchCapacity = 64;
            iterator.planBatches(batchCapacity, 0);

            // Both fetch steps are held in their first page read, each on a thread of the pool, so
            // neither column ends on its own and the first close cannot return while its step is
            // held.
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
            assertThat(readsStalled.await(5, TimeUnit.SECONDS)).as("both fetch steps are held inside a page read")
                    .isTrue();

            Thread closer = Thread.ofVirtual().start(first::close);
            try {
                awaitParkedOrExited(closer);
                assertThat(closer.isAlive()).as("the first close waits for its held fetch step").isTrue();
                assertThat(second.isDone()).as("the second column is stopped by the first close").isTrue();
                assertThat(secondExchange.isFinished())
                        .as("the second column's consumer sees the end while the first close waits")
                        .isTrue();
            }
            finally {
                release.countDown();
            }
            closer.join(TimeUnit.SECONDS.toMillis(10));
            assertThat(closer.isAlive()).as("close() returns once the fetch step returns").isFalse();
            second.close();
            iterator.close();
            inputFile.close();
        }
    }

    /// A stop that arrives while a fetch step stops for room, after it looked at the stop and
    /// before it releases its claim, still ends the fetch: the stop finds the claim held and queues
    /// no step, so the step has to look again once it has released the claim.
    ///
    /// The single decode thread runs the column's first step, so no page task runs while it
    /// fetches: the pages in flight per thread admit two, and the step fills its queue and stops
    /// for room.
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void stopReachesAFetchStepStoppingForRoom() throws Exception {
        try (HardwoodContextImpl context = HardwoodContextImpl.create(1);
             ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE))) {
            CountDownLatch stoppedForRoom = new CountDownLatch(1);
            ScheduledColumn.roomWaitObserver = column -> {
                if (stoppedForRoom.getCount() > 0) {
                    stoppedForRoom.countDown();
                    column.fail(new IllegalStateException("stopped"));
                }
            };
            try (HeldRead read = HeldRead.start(context, reader, iterator -> new PageSource(iterator, 0))) {
                ScheduledFlatColumn scheduled = read.column();
                try {
                    assertThat(stoppedForRoom.await(5, TimeUnit.SECONDS)).as("the fetch step stops for room").isTrue();
                    awaitTrue(() -> scheduled.fetchEnded, "the fetch ends after the stop");
                }
                finally {
                    scheduled.close();
                }
            }
            finally {
                ScheduledColumn.roomWaitObserver = null;
            }
        }
    }

    /// Two threads closing one column at once, as a reader and its parent can, both return, and
    /// only once the column's queued fetch step has finished. The single decode thread is held,
    /// so the column's first fetch step is queued and cannot run until it is released.
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void concurrentClosesBothReturnOnceTheQueuedStepFinishes() throws Exception {
        try (HardwoodContextImpl context = HardwoodContextImpl.create(1);
             ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE))) {
            try (HeldRead read = HeldRead.startHoldingTheDecodeThread(context, reader, iterator -> new PageSource(iterator, 0))) {
                CountDownLatch releaseWorker = read.releaseWorker();
                ScheduledFlatColumn scheduled = read.column();
                Thread first;
                Thread second;
                try {
                    awaitTrue(() -> scheduled.tasksInFlight() > 0, "a fetch step is queued");
                    first = Thread.ofPlatform().start(scheduled::close);
                    awaitTrue(() -> LockSupport.getBlocker(first) == scheduled,
                            "the first close claimed the column and waits for its fetch step");
                    second = Thread.ofVirtual().start(scheduled::close);
                    awaitParkedOrExited(second);
                    assertThat(second.isAlive()).as("the second close waits for the claimed close").isTrue();
                }
                finally {
                    releaseWorker.countDown();
                }
                first.join(TimeUnit.SECONDS.toMillis(5));
                second.join(TimeUnit.SECONDS.toMillis(5));

                assertThat(first.isAlive()).as("the first close returns once the fetch step finished").isFalse();
                assertThat(second.isAlive()).as("the second close returns once the fetch step finished").isFalse();
            }
        }
    }

    /// A close interrupted while it waits for the column's queued fetch step goes on waiting
    /// without spinning, and returns with the interrupt restored once the step has finished.
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void interruptedCloseWaitsWithoutSpinning() throws Exception {
        try (HardwoodContextImpl context = HardwoodContextImpl.create(1);
             ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE))) {
            try (HeldRead read = HeldRead.startHoldingTheDecodeThread(context, reader, iterator -> new PageSource(iterator, 0))) {
                CountDownLatch releaseWorker = read.releaseWorker();
                ScheduledFlatColumn scheduled = read.column();
                AtomicBoolean interruptedOnReturn = new AtomicBoolean();
                Thread closer;
                try {
                    awaitTrue(() -> scheduled.tasksInFlight() > 0, "a fetch step is queued");
                    closer = Thread.ofPlatform().start(() -> {
                        scheduled.close();
                        interruptedOnReturn.set(Thread.currentThread().isInterrupted());
                    });
                    awaitTrue(() -> LockSupport.getBlocker(closer) == scheduled,
                            "the close waits for the column's fetch step");
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

                assertThat(closer.isAlive()).as("the close returns once the fetch step finished").isFalse();
                assertThat(interruptedOnReturn.get()).as("the close restores the interrupt").isTrue();
            }
        }
    }

    /// One column of the first file's first leaf, its pipeline started on `context`, reading its
    /// pages through the [PageSource] `source` builds.
    private record HeldRead(ScheduledFlatColumn column, RowGroupIterator iterator, InputFile inputFile,
                            CountDownLatch releaseWorker) implements AutoCloseable {

        private static final int BATCH_CAPACITY = 64;

        static HeldRead start(HardwoodContextImpl context, ParquetFileReader reader,
                              Function<RowGroupIterator, PageSource> source) throws IOException, InterruptedException {
            return start(context, reader, source, false);
        }

        /// As [#start], with the context's single decode thread held until [#releaseWorker()] is
        /// counted down, so that the column's first fetch step stays queued. The first row group
        /// is planned before, as planning parses its offset indexes on the pool.
        static HeldRead startHoldingTheDecodeThread(HardwoodContextImpl context, ParquetFileReader reader,
                                                    Function<RowGroupIterator, PageSource> source)
                throws IOException, InterruptedException {
            return start(context, reader, source, true);
        }

        private static HeldRead start(HardwoodContextImpl context, ParquetFileReader reader,
                                      Function<RowGroupIterator, PageSource> source, boolean holdDecodeThread)
                throws IOException, InterruptedException {
            FileSchema schema = reader.getFileSchema();
            ColumnSchema column = schema.getColumn(0);
            InputFile inputFile = InputFile.of(FILE);
            inputFile.open();
            RowGroupIterator iterator = new RowGroupIterator(List.of(inputFile), context, 0);
            iterator.setFirstFile(schema, reader.getFileMetaData().rowGroups());
            iterator.initialize(ProjectedSchema.create(schema, ColumnProjection.all()), null);
            iterator.planBatches(BATCH_CAPACITY, 0);
            CountDownLatch releaseWorker = null;
            if (holdDecodeThread) {
                iterator.getColumnPlan(iterator.workItemAt(0), 0);
                releaseWorker = holdTheDecodeThread(context);
            }
            BatchExchange<BatchExchange.Batch> exchange = BatchExchange.detachingFlat(column.name(), BATCH_CAPACITY);
            ReadScheduler scheduler = new ReadScheduler(context, BATCH_CAPACITY, BATCH_CAPACITY);
            ScheduledFlatColumn scheduled = new ScheduledFlatColumn(scheduler, source.apply(iterator), exchange,
                    column, BATCH_CAPACITY, context.decompressorFactory(), null);
            scheduler.start();
            return new HeldRead(scheduled, iterator, inputFile, releaseWorker);
        }

        @Override
        public void close() throws IOException {
            iterator.close();
            inputFile.close();
        }
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

    /// A column of `scheduler` whose fetch step's first page read counts `stalled` down and is held
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
