/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.AfterEach;
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

/// [ReadScheduler] submits the queued page with the lowest row of the read; while that page is
/// past its column's read-ahead depth and the read leaves a decode thread idle, it submits a page
/// of another column that is within its own depth, which every page below the waiting one being
/// submitted makes read-ahead.
///
/// The read has two flat columns on a pool of four threads: `small`, an `INT64` column cut into
/// pages of 16 rows, and `large`, a `BOOLEAN` column cut into pages of 200 rows, a little over
/// three batches. The consumer takes nothing until the read settles, so each column publishes
/// the two batches its exchange holds and the read's first unpublished row stops at 128.
///
/// - `small` is admitted up to fewer than four batches past that row: its last page submitted
///   starts at 368, and its page at 384 waits.
/// - `large`'s pages at 400 and 600 start past `small`'s waiting page and are submitted, being
///   fewer pages ahead than the pool has threads and within the eight batches of the cap; its page
///   at 800 is past the cap and waits.
///
/// Every page submitted past a lower waiting page is checked to start at least four batches past
/// the read's first unpublished row the choice was made against. Each read then takes every batch,
/// which a read that is not live never ends.
class HeadOfLineGatingTest {

    private static final int THREADS = 4;
    private static final int BATCH = 64;
    private static final int ROWS = 40 * BATCH;
    private static final int SMALL_PAGE = 16;
    private static final int LARGE_PAGE = 200;
    /// The batch byte budget the scheduler is given, in rows: the cap is eight batches.
    private static final long BUDGET_ROWS = BATCH;
    /// How long no page may be submitted for the read to count as settled.
    private static final long SETTLED_NANOS = TimeUnit.MILLISECONDS.toNanos(500);

    @AfterEach
    void removeObserver() {
        ReadScheduler.submissionObserver = null;
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void aPageWithinItsDepthIsSubmittedPastALowerPageBeyondItsDepth() throws Exception {
        byte[] file = write();
        List<String> violations = Collections.synchronizedList(new ArrayList<>());
        AtomicLong submissions = new AtomicLong();
        AtomicLong lastSubmission = new AtomicLong(System.nanoTime());
        AtomicLong lastSmall = new AtomicLong(-1);
        AtomicLong lastLarge = new AtomicLong(-1);
        ReadScheduler.submissionObserver = (column, page, inFlight, lowestRow, readPublishedRow) -> {
            if (page.firstRow != lowestRow && lowestRow - readPublishedRow < (long) THREADS * BATCH) {
                violations.add("page at row " + page.firstRow + " past one at row " + lowestRow
                        + " with the read published to row " + readPublishedRow);
            }
            boolean small = column.column.name().equals("small");
            if (page.info.records() != Math.min(small ? SMALL_PAGE : LARGE_PAGE, ROWS - page.firstRow)) {
                violations.add("page at row " + page.firstRow + " of " + page.info.records() + " rows");
            }
            (small ? lastSmall : lastLarge).set(page.firstRow);
            submissions.incrementAndGet();
            lastSubmission.set(System.nanoTime());
        };

        long longSum = 0;
        long misplaced = 0;
        long records = 0;
        try (HardwoodContextImpl context = HardwoodContextImpl.create(THREADS);
             RowGroupIterator iterator = iterator(context, file)) {
            iterator.planBatches(BATCH, 0);
            ReadScheduler scheduler = new ReadScheduler(context, BATCH, BUDGET_ROWS);
            BatchExchange<BatchExchange.Batch> smallExchange = BatchExchange.detachingFlat("small", BATCH);
            BatchExchange<BatchExchange.Batch> largeExchange = BatchExchange.detachingFlat("large", BATCH);
            try (ScheduledFlatColumn small = column(scheduler, iterator, context, 0, smallExchange);
                 ScheduledFlatColumn large = column(scheduler, iterator, context, 1, largeExchange)) {
                scheduler.start();
                awaitSettled(submissions, lastSubmission);

                assertThat(Math.min(small.publishedRow(), large.publishedRow()))
                        .as("the read's first unpublished row").isEqualTo(2 * BATCH);
                assertThat(lastSmall.get()).as("last page of small submitted").isEqualTo(368);
                assertThat(small.peekReady().firstRow).as("small's waiting page").isEqualTo(384);
                assertThat(lastLarge.get()).as("last page of large submitted").isEqualTo(3 * LARGE_PAGE);
                assertThat(large.peekReady().firstRow).as("large's waiting page").isEqualTo(4 * LARGE_PAGE);
                assertThat(large.pagesAhead()).as("large's pages ahead").isEqualTo(4);

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
            }
        }
        assertThat(records).isEqualTo(ROWS);
        assertThat(longSum).as("values off their row").isZero();
        assertThat(misplaced).as("booleans off their row").isZero();
        assertThat(violations).isEmpty();
    }

    private static ScheduledFlatColumn column(ReadScheduler scheduler, RowGroupIterator iterator,
                                              HardwoodContextImpl context, int index,
                                              BatchExchange<BatchExchange.Batch> exchange) {
        return new ScheduledFlatColumn(scheduler, new PageSource(iterator, index), exchange,
                iterator.referenceSchema().getColumn(index), BATCH, context.decompressorFactory(), null);
    }

    /// Waits until no page was submitted for [#SETTLED_NANOS], after the first.
    private static void awaitSettled(AtomicLong submissions, AtomicLong lastSubmission) throws InterruptedException {
        while (submissions.get() == 0 || System.nanoTime() - lastSubmission.get() < SETTLED_NANOS) {
            Thread.sleep(20);
        }
    }

    /// One row group of `ROWS` rows: a required `INT64` column `small` holding its row, cut into
    /// pages of 16 rows by a 128-byte page target, and a required `BOOLEAN` column `large`, true
    /// on every third row, whose bit-packed pages the 200-row page target cuts first.
    private static byte[] write() throws Exception {
        FileSchema schema = FileSchema.builder("skewed")
                .addColumn("small", PhysicalType.INT64, RepetitionType.REQUIRED)
                .addColumn("large", PhysicalType.BOOLEAN, RepetitionType.REQUIRED)
                .build();
        long[] longs = new long[ROWS];
        boolean[] booleans = new boolean[ROWS];
        for (int i = 0; i < ROWS; i++) {
            longs[i] = i;
            booleans[i] = i % 3 == 0;
        }
        WriterConfig config = WriterConfig.builder()
                .encoding(ColumnEncoding.PLAIN)
                .pageTargetBytes(SMALL_PAGE * Long.BYTES)
                .pageTargetRows(LARGE_PAGE)
                .build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, config)) {
            writer.columnWriter().writeBatch(batch -> batch.longs("small", longs).booleans("large", booleans));
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
