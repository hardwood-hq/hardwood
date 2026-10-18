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
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
}
