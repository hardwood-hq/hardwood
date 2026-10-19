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
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.InMemoryFiles;
import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;

/// The read-ahead depth of [ReadScheduler]: how far past its first unpublished row a column's
/// pages are submitted while the consumer takes nothing, on pools of one, two and twelve threads,
/// for a flat column and a column with repetition whose pages start on batch boundaries.
///
/// The read has one column, so the column's first unpublished row is the read's. The scheduler
/// is given a batch byte budget of one batch, which puts the cap at eight batches. Each read first
/// runs until the column's ready queue is full and the scheduler submits nothing more, and then
/// takes every batch, which a read that is not live never ends.
class ReadAheadDepthTest {

    private static final int BATCH = 64;
    private static final int ROWS = 40 * BATCH;
    /// The batch byte budget the scheduler is given, in rows.
    private static final long BUDGET_ROWS = BATCH;
    /// The cap: eight batch byte budgets.
    private static final long CAP_ROWS = 8 * BUDGET_ROWS;
    /// Elements per record of the column with repetition.
    private static final int K = 3;
    /// How long no page may be submitted for the read to count as settled.
    private static final long SETTLED_NANOS = TimeUnit.MILLISECONDS.toNanos(500);

    @AfterEach
    void removeObserver() {
        ReadScheduler.submissionObserver = null;
    }

    /// For the flat column, the lead of the last page submitted over the first unpublished row
    /// once the read settles, `-1` for the column with repetition, whose pages carry a lower
    /// bound of their first row until they are placed.
    static Stream<Arguments> reads() {
        int smallPage = BATCH / 2;
        int largePage = 2 * BATCH;
        return Stream.of(
                // The always-admitted pages: those starting at most one batch past the first
                // unpublished row
                Arguments.of(1, false, smallPage, BATCH),
                // Fewer batches past it than the pool has threads
                Arguments.of(2, false, smallPage, 3 * smallPage),
                // Up to the cap, short of the twelve batches the threads would admit
                Arguments.of(12, false, smallPage, 15 * smallPage),
                // A page spanning two batches: nothing but the page holding the first
                // unpublished row
                Arguments.of(1, false, largePage, 0),
                // Fewer pages ahead than the pool has threads
                Arguments.of(2, false, largePage, largePage),
                // Up to the cap, short of the eleven pages the threads would admit
                Arguments.of(12, false, largePage, 3 * largePage),
                Arguments.of(1, true, smallPage, -1),
                Arguments.of(2, true, smallPage, -1),
                Arguments.of(12, true, smallPage, -1),
                Arguments.of(1, true, BATCH, -1),
                Arguments.of(2, true, BATCH, -1),
                Arguments.of(12, true, BATCH, -1));
    }

    @ParameterizedTest(name = "{0} threads, repeated {1}, pages of {2} rows")
    @MethodSource("reads")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void pagesAreSubmittedWithinTheDepth(int threads, boolean repeated, int pageRows, long expectedLead)
            throws Exception {
        byte[] file = write(repeated, pageRows);
        List<String> violations = Collections.synchronizedList(new ArrayList<>());
        AtomicLong submissions = new AtomicLong();
        AtomicLong lastSubmission = new AtomicLong(System.nanoTime());
        AtomicLong maxFirstRow = new AtomicLong(-1);
        ReadScheduler.submissionObserver = (column, page, inFlight, lowestRow, readPublishedRow) -> {
            // Both only grow past the scheduler's decision: the published row as cells publish,
            // and the pages ahead by the page just taken alone
            long ahead = page.firstRow - column.publishedRow();
            int pagesBefore = column.pagesAhead() - 1;
            boolean admitted = ahead <= BATCH
                    || ahead < CAP_ROWS && (pagesBefore < threads || ahead < (long) threads * BATCH);
            if (!admitted) {
                violations.add("page at row " + page.firstRow + ", " + ahead + " rows and " + pagesBefore
                        + " pages ahead");
            }
            if (inFlight > 2 * threads) {
                violations.add("page at row " + page.firstRow + " with " + inFlight + " pages in flight");
            }
            maxFirstRow.accumulateAndGet(page.firstRow, Math::max);
            submissions.incrementAndGet();
            lastSubmission.set(System.nanoTime());
        };

        long values = 0;
        long sum = 0;
        long records = 0;
        try (HardwoodContextImpl context = HardwoodContextImpl.create(threads);
             RowGroupIterator iterator = iterator(context, file)) {
            iterator.planBatches(BATCH, 0);
            ColumnSchema column = iterator.referenceSchema().getColumn(0);
            PageSource source = new PageSource(iterator, 0);
            ReadScheduler scheduler = new ReadScheduler(context, BATCH, BUDGET_ROWS);
            if (repeated) {
                BatchExchange<NestedBatch> exchange = BatchExchange.detachingNested(column.name(), BATCH);
                try (ScheduledNestedColumn scheduled = new ScheduledNestedColumn(scheduler, source, exchange,
                        column, BATCH, context.decompressorFactory(),
                        NestedLevelComputer.computeLayers(iterator.referenceSchema())[column.columnIndex()],
                        NestedIndexMode.ALL_ITEMS, false)) {
                    scheduler.start();
                    awaitSettled(submissions, lastSubmission);
                    NestedBatch batch;
                    while ((batch = exchange.poll()) != null) {
                        long[] longs = (long[]) batch.values;
                        for (int i = 0; i < batch.valueCount; i++) {
                            assertThat(longs[i]).as("value %d", values).isEqualTo(values);
                            values++;
                        }
                        records += batch.recordCount;
                    }
                    exchange.checkError();
                }
                assertThat(values).isEqualTo((long) ROWS * K);
            }
            else {
                BatchExchange<BatchExchange.Batch> exchange = BatchExchange.detachingFlat(column.name(), BATCH);
                try (ScheduledFlatColumn scheduled = new ScheduledFlatColumn(scheduler, source, exchange, column,
                        BATCH, context.decompressorFactory(), null)) {
                    scheduler.start();
                    awaitSettled(submissions, lastSubmission);
                    assertThat(maxFirstRow.get() - scheduled.publishedRow())
                            .as("lead of the last page submitted over the first unpublished row")
                            .isEqualTo(expectedLead);
                    BatchExchange.Batch batch;
                    while ((batch = exchange.poll()) != null) {
                        long[] longs = (long[]) batch.values;
                        for (int i = 0; i < batch.recordCount; i++) {
                            sum += longs[i] - records - i;
                        }
                        records += batch.recordCount;
                    }
                    exchange.checkError();
                }
                assertThat(sum).as("values off their row").isZero();
            }
        }
        assertThat(records).isEqualTo(ROWS);
        assertThat(violations).isEmpty();
    }

    /// Waits until no page was submitted for [#SETTLED_NANOS], after the first.
    private static void awaitSettled(AtomicLong submissions, AtomicLong lastSubmission) throws InterruptedException {
        while (submissions.get() == 0 || System.nanoTime() - lastSubmission.get() < SETTLED_NANOS) {
            Thread.sleep(20);
        }
    }

    /// One row group of `ROWS` rows, cut into pages of `pageRows` records: a
    /// required `INT64` column `v` holding its row, or a `REQUIRED list<REQUIRED int64>` `v` of
    /// `K` elements per record holding `0..ROWS*K-1`.
    private static byte[] write(boolean repeated, int pageRows) throws Exception {
        FileSchema schema = repeated
                ? FileSchema.builder("lists")
                        .list("v", RepetitionType.REQUIRED,
                                el -> el.primitive(PhysicalType.INT64, RepetitionType.REQUIRED))
                        .build()
                : FileSchema.builder("longs").addColumn("v", PhysicalType.INT64, RepetitionType.REQUIRED).build();
        long[] longs = new long[repeated ? ROWS * K : ROWS];
        for (int i = 0; i < longs.length; i++) {
            longs[i] = i;
        }
        int[] offsets = new int[ROWS + 1];
        for (int i = 0; i <= ROWS; i++) {
            offsets[i] = i * K;
        }
        WriterConfig config = WriterConfig.builder()
                .pageTargetRows(pageRows)
                .pageTargetBytes(Integer.MAX_VALUE)
                .build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, config)) {
            writer.columnWriter().writeBatch(batch -> {
                if (repeated) {
                    batch.list("v", offsets).longs("v.list.element", longs);
                }
                else {
                    batch.longs("v", longs);
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
