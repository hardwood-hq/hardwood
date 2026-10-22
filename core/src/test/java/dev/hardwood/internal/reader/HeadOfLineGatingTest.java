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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import dev.hardwood.InMemoryFiles;
import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ColumnEncoding;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/// [ReadScheduler] submits the queued page with the lowest row of the read, once no column with
/// nothing queued may yet queue a page below it; while that page is past the read-ahead cap and
/// the read leaves a decode thread idle, it submits a page of another column that holds rows of
/// the batch that column publishes next, which every page below the waiting one being submitted
/// makes read-ahead.
///
/// The reads have flat columns: `small`, an `INT64` column cut into pages of 16 rows, and
/// `large`, a `BOOLEAN` column cut into pages of 200 or 600 rows. The consumer takes nothing from
/// them until the read reaches the state the test expects, so each column publishes the two
/// batches its exchange holds and the read's first unpublished row stops at 128. The cap is eight
/// batches past that row, on a pool of four threads:
///
/// - `small`'s last page submitted starts at 624, and its page at 640 waits.
/// - `large`'s page of 600 rows at 600 is within the cap and submitted; its page at 1200 waits,
///   and the pages of a third column, `ahead`, whose consumer takes every batch, are submitted
///   past it.
///
/// Every page submitted past a lower waiting page is checked to be submitted while that page is
/// past the cap the choice was made against. Each read then takes every batch, which a read that
/// is not live never ends.
@Tag("pool")
class HeadOfLineGatingTest {

    private static final int THREADS = 4;
    private static final int BATCH = 64;
    private static final int ROWS = 40 * BATCH;
    private static final int SMALL_PAGE = 16;
    private static final int LARGE_PAGE = 200;
    /// The batch byte budget the scheduler is given, in rows.
    private static final long BUDGET_ROWS = BATCH;
    /// The cap: eight batch byte budgets.
    private static final long CAP_ROWS = 8 * BUDGET_ROWS;
    /// How long the read may take to reach the state the test expects.
    private static final long STATE_NANOS = TimeUnit.SECONDS.toNanos(30);
    /// How long no page may be submitted, once the read is in that state, for it to count as
    /// settled there.
    private static final long QUIET_NANOS = TimeUnit.MILLISECONDS.toNanos(200);

    @AfterEach
    void removeObserver() {
        ReadScheduler.submissionObserver = null;
    }

    /// A column whose page is past the cap does not hold back a later column whose page holds rows
    /// of the batch it publishes next: `ahead`, its batches taken as they are published, has
    /// published far past the read's first unpublished row, and each of its pages starts within a
    /// batch of its own first unpublished row.
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void aPagePastTheCapHoldsBackNoLaterColumnsNextBatch() throws Exception {
        int largePage = 3 * LARGE_PAGE;
        byte[] file = write(largePage, true);
        List<String> violations = Collections.synchronizedList(new ArrayList<>());
        AtomicLong lastSubmission = new AtomicLong(System.nanoTime());
        AtomicLong lastSmall = new AtomicLong(-1);
        AtomicLong lastLarge = new AtomicLong(-1);
        observe(largePage, violations, lastSubmission, lastSmall, lastLarge);

        long[] read;
        long[] aheadRead;
        try (HardwoodContextImpl context = HardwoodContextImpl.create(THREADS);
             RowGroupIterator iterator = iterator(context, file)) {
            iterator.planBatches(BATCH, 0);
            ReadScheduler scheduler = new ReadScheduler(context, iterator.readQueue(), BATCH, BUDGET_ROWS);
            BatchExchange<BatchExchange.Batch> smallExchange = BatchExchange.detachingFlat("small", BATCH);
            BatchExchange<BatchExchange.Batch> largeExchange = BatchExchange.detachingFlat("large", BATCH);
            BatchExchange<BatchExchange.Batch> aheadExchange = BatchExchange.detachingFlat("ahead", BATCH);
            FutureTask<long[]> takeAhead = new FutureTask<>(() -> takeAll(aheadExchange));
            try (ScheduledFlatColumn small = column(scheduler, iterator, 0, context, smallExchange);
                 ScheduledFlatColumn large = column(scheduler, iterator, 1, context, largeExchange);
                 ScheduledFlatColumn ahead = column(scheduler, iterator, 2, context, aheadExchange)) {
                Thread.ofVirtual().start(takeAhead);
                scheduler.start();
                awaitState(() -> takeAhead.isDone() && lastSmall.get() == 624 && lastLarge.get() == largePage
                        && small.publishedRow() == 2 * BATCH && large.publishedRow() == 2 * BATCH
                        && queuedAt(small, 640) && queuedAt(large, 2 * largePage),
                        () -> observed(lastSmall, lastLarge, small, large) + "; " + observed("ahead", ahead)
                                + ", taken: " + takeAhead.isDone(),
                        lastSubmission);

                assertThat(lastSmall.get()).as("last page of small submitted").isEqualTo(624);
                assertThat(small.peekReady().firstRow).as("small's waiting page").isEqualTo(640);
                assertThat(lastLarge.get()).as("last page of large submitted").isEqualTo(largePage);
                assertThat(large.peekReady().firstRow).as("large's waiting page").isEqualTo(2 * largePage);
                aheadRead = takeAhead.get();
                read = takeAll(smallExchange, largeExchange);
            }
        }
        assertThat(aheadRead[0]).as("rows of ahead").isEqualTo(ROWS);
        assertThat(aheadRead[1]).as("values of ahead off their row").isZero();
        assertThat(read[0]).isEqualTo(ROWS);
        assertThat(read[1]).as("values off their row").isZero();
        assertThat(read[2]).as("booleans off their row").isZero();
        assertThat(violations).isEmpty();
    }

    /// No page is submitted while a column with nothing queued may yet queue a page below it:
    /// `large`'s fetch step is held before its first page, so its next page may start at row 0,
    /// and `small`'s page at 16 waits for it. The pool runs each task on the thread submitting it,
    /// so every choice the scheduler makes after `small` queues that page is made before
    /// `small`'s fetch step asks for its next page. The scheduler starts on a thread of its own,
    /// which runs both columns' first steps and is held in `large`'s.
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void aPageWaitsForAColumnWithNothingQueuedThatMayStartBelowIt() throws Exception {
        byte[] file = write(LARGE_PAGE, false);
        List<String> violations = Collections.synchronizedList(new ArrayList<>());
        List<ScheduledColumn<?>> columns = new CopyOnWriteArrayList<>();
        AtomicLong lastSmall = new AtomicLong(-1);
        ReadScheduler.submissionObserver = (column, page, inFlight, lowestRow, readPublishedRow) -> {
            for (ScheduledColumn<?> other : columns) {
                // The queue is read before the next row, which only grows as pages are queued
                if (other != column && other.peekReady() == null && other.nextRow() < page.firstRow) {
                    violations.add("page of " + column.column.name() + " at row " + page.firstRow + " with "
                            + other.column.name() + "'s next page, at or after row " + other.nextRow()
                            + ", not queued");
                }
            }
            if (column.column.name().equals("small")) {
                lastSmall.set(page.firstRow);
            }
        };
        CountDownLatch largeHeld = new CountDownLatch(1);
        // Counted down as the fetch step asks for each of small's first three pages
        CountDownLatch smallAsked = new CountDownLatch(3);

        TaskOrderExecutor executor = TaskOrderExecutor.inline(null);
        long[] read;
        try (HardwoodContextImpl context = executor.context(1);
             RowGroupIterator iterator = iterator(context, file)) {
            iterator.planBatches(BATCH, 0);
            ReadScheduler scheduler = new ReadScheduler(context, iterator.readQueue(), BATCH, BUDGET_ROWS);
            BatchExchange<BatchExchange.Batch> smallExchange = BatchExchange.detachingFlat("small", BATCH);
            BatchExchange<BatchExchange.Batch> largeExchange = BatchExchange.detachingFlat("large", BATCH);
            PageSource smallSource = new PageSource(iterator, 0) {
                @Override
                public PageInfo next() throws IOException {
                    smallAsked.countDown();
                    return super.next();
                }
            };
            PageSource largeSource = new PageSource(iterator, 1) {
                @Override
                public PageInfo next() throws IOException {
                    try {
                        largeHeld.await();
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new InterruptedIOException("Interrupted while held");
                    }
                    return super.next();
                }
            };
            try (ScheduledFlatColumn small = column(scheduler, iterator, smallSource, 0, context, smallExchange);
                 ScheduledFlatColumn large = column(scheduler, iterator, largeSource, 1, context, largeExchange)) {
                columns.add(small);
                columns.add(large);
                Thread starter = Thread.ofVirtual().start(scheduler::start);
                try {
                    assertThat(smallAsked.await(STATE_NANOS, TimeUnit.NANOSECONDS))
                            .as("small's fetch step asked for its third page").isTrue();
                    assertThat(lastSmall.get()).as("last page of small submitted").isEqualTo(0);
                    assertThat(small.peekReady().firstRow).as("small's waiting page").isEqualTo(SMALL_PAGE);
                }
                finally {
                    largeHeld.countDown();
                }
                starter.join();
                read = takeAll(smallExchange, largeExchange);
            }
        }
        assertThat(executor.failure()).isNull();
        assertThat(read[0]).isEqualTo(ROWS);
        assertThat(read[1]).as("values off their row").isZero();
        assertThat(read[2]).as("booleans off their row").isZero();
        assertThat(violations).isEmpty();
    }

    /// Checks each page submitted and records the last page of `small` and of `large` submitted:
    /// a page is submitted past a lower queued page only while that page is past the cap, and
    /// each page holds the rows its column's pages are cut into.
    private static void observe(int largePage, List<String> violations, AtomicLong lastSubmission,
                                AtomicLong lastSmall, AtomicLong lastLarge) {
        ReadScheduler.submissionObserver = (column, page, inFlight, lowestRow, readPublishedRow) -> {
            if (page.firstRow != lowestRow && lowestRow - readPublishedRow < CAP_ROWS) {
                violations.add("page at row " + page.firstRow + " past one at row " + lowestRow
                        + " with the read published to row " + readPublishedRow);
            }
            String name = column.column.name();
            boolean large = name.equals("large");
            if (page.info.records() != Math.min(large ? largePage : SMALL_PAGE, ROWS - page.firstRow)) {
                violations.add("page of " + name + " at row " + page.firstRow + " of " + page.info.records()
                        + " rows");
            }
            if (name.equals("small")) {
                lastSmall.set(page.firstRow);
            }
            else if (large) {
                lastLarge.set(page.firstRow);
            }
            lastSubmission.set(System.nanoTime());
        };
    }

    /// Takes every batch of `small` and `large` together.
    ///
    /// @return the rows taken, the sum of `small`'s values less their rows, and the rows whose
    ///     `large` value is not the one written there
    private static long[] takeAll(BatchExchange<BatchExchange.Batch> smallExchange,
                                  BatchExchange<BatchExchange.Batch> largeExchange) throws Exception {
        long records = 0;
        long longSum = 0;
        long misplaced = 0;
        BatchExchange.Batch smallBatch;
        while ((smallBatch = smallExchange.poll()) != null) {
            BatchExchange.Batch largeBatch = largeExchange.poll();
            assertThat(largeBatch).isNotNull();
            assertThat(largeBatch.recordCount).isEqualTo(smallBatch.recordCount);
            long[] longs = (long[]) smallBatch.values;
            boolean[] booleans = (boolean[]) largeBatch.values;
            for (int i = 0; i < smallBatch.recordCount; i++) {
                longSum += longs[i] - records - i;
                if (booleans[i] != ((records + i) % 3 == 0)) {
                    misplaced++;
                }
            }
            records += smallBatch.recordCount;
        }
        assertThat(largeExchange.poll()).isNull();
        smallExchange.checkError();
        largeExchange.checkError();
        return new long[]{ records, longSum, misplaced };
    }

    /// Takes every batch of an `INT64` column holding its row.
    ///
    /// @return the rows taken and the sum of the values less their rows
    private static long[] takeAll(BatchExchange<BatchExchange.Batch> exchange) throws Exception {
        long records = 0;
        long sum = 0;
        BatchExchange.Batch batch;
        while ((batch = exchange.poll()) != null) {
            long[] longs = (long[]) batch.values;
            for (int i = 0; i < batch.recordCount; i++) {
                sum += longs[i] - records - i;
            }
            records += batch.recordCount;
        }
        exchange.checkError();
        return new long[]{ records, sum };
    }

    private static ScheduledFlatColumn column(ReadScheduler scheduler, RowGroupIterator iterator, int index,
                                              HardwoodContextImpl context,
                                              BatchExchange<BatchExchange.Batch> exchange) {
        return column(scheduler, iterator, new PageSource(iterator, index), index, context, exchange);
    }

    private static ScheduledFlatColumn column(ReadScheduler scheduler, RowGroupIterator iterator, PageSource source,
                                              int index, HardwoodContextImpl context,
                                              BatchExchange<BatchExchange.Batch> exchange) {
        return new ScheduledFlatColumn(scheduler, source, exchange, iterator.referenceSchema().getColumn(index),
                BATCH, context.decompressorFactory(), null);
    }

    /// Whether `column`'s next queued page starts at `row`.
    private static boolean queuedAt(ScheduledColumn<?> column, long row) {
        PageTask head = column.peekReady();
        return head != null && head.firstRow == row;
    }

    /// The rows the tests' expected states compare: the last pages submitted, and each column's
    /// first unpublished row and waiting page.
    private static String observed(AtomicLong lastSmall, AtomicLong lastLarge, ScheduledFlatColumn small,
                                   ScheduledFlatColumn large) {
        return "last page submitted: small " + lastSmall.get() + ", large " + lastLarge.get()
                + "; " + observed("small", small) + "; " + observed("large", large);
    }

    private static String observed(String name, ScheduledColumn<?> column) {
        PageTask head = column.peekReady();
        return name + ": published to " + column.publishedRow() + ", waiting page "
                + (head == null ? "none" : String.valueOf(head.firstRow));
    }

    /// Waits until `reached` holds, then until no page was submitted for [#QUIET_NANOS]; fails with
    /// the `observed` state if `reached` does not hold within [#STATE_NANOS].
    private static void awaitState(BooleanSupplier reached, Supplier<String> observed, AtomicLong lastSubmission)
            throws InterruptedException {
        long deadline = System.nanoTime() + STATE_NANOS;
        while (!reached.getAsBoolean()) {
            if (System.nanoTime() - deadline >= 0) {
                fail("Expected state not reached within " + TimeUnit.NANOSECONDS.toSeconds(STATE_NANOS)
                        + " s; observed " + observed.get());
            }
            Thread.sleep(10);
        }
        while (System.nanoTime() - lastSubmission.get() < QUIET_NANOS) {
            Thread.sleep(10);
        }
    }

    /// One row group of `ROWS` rows: a required `INT64` column `small` holding its row, cut into
    /// pages of 16 rows by a 128-byte page target, a required `BOOLEAN` column `large`, true on
    /// every third row, whose bit-packed pages the `largePage`-row page target cuts first, and,
    /// with `ahead`, a required `INT64` column `ahead` alike `small`.
    private static byte[] write(int largePage, boolean ahead) throws Exception {
        FileSchema.Builder builder = FileSchema.builder("skewed")
                .addColumn("small", PhysicalType.INT64, RepetitionType.REQUIRED)
                .addColumn("large", PhysicalType.BOOLEAN, RepetitionType.REQUIRED);
        if (ahead) {
            builder.addColumn("ahead", PhysicalType.INT64, RepetitionType.REQUIRED);
        }
        FileSchema schema = builder.build();
        long[] longs = new long[ROWS];
        boolean[] booleans = new boolean[ROWS];
        for (int i = 0; i < ROWS; i++) {
            longs[i] = i;
            booleans[i] = i % 3 == 0;
        }
        WriterConfig config = WriterConfig.builder()
                .encoding(ColumnEncoding.PLAIN)
                .pageTargetBytes(SMALL_PAGE * Long.BYTES)
                .pageTargetRows(largePage)
                .build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, config)) {
            writer.columnWriter().writeBatch(batch -> {
                batch.longs("small", longs).booleans("large", booleans);
                if (ahead) {
                    batch.longs("ahead", longs);
                }
            });
        }
        return InMemoryFiles.toByteArray(out);
    }

    private static RowGroupIterator iterator(HardwoodContextImpl context, byte[] file) throws Exception {
        InputFile input = InputFile.of(ByteBuffer.wrap(file));
        input.open();
        RowGroupIterator iterator = new RowGroupIterator(List.of(input), context, 0);
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)))) {
            FileSchema schema = reader.getFileSchema();
            iterator.setFirstFile(schema, reader.getFileMetaData().rowGroups());
            iterator.initialize(ProjectedSchema.create(schema, ColumnProjection.all()), null);
        }
        return iterator;
    }
}
