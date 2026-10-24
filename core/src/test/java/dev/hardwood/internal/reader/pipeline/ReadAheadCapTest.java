/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.pipeline;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.InMemoryFiles;
import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.reader.batch.BatchExchange;
import dev.hardwood.internal.reader.batch.NestedBatch;
import dev.hardwood.internal.reader.batch.NestedIndexMode;
import dev.hardwood.internal.reader.batch.NestedLevelComputer;
import dev.hardwood.internal.reader.plan.PageSource;
import dev.hardwood.internal.reader.plan.RowGroupIterator;
import dev.hardwood.internal.reader.plan.RowGroupIterators;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.internal.schema.ReadProjection;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/// The read-ahead cap of [ReadScheduler]: how far past its first unpublished row a column's pages
/// are submitted while the consumer takes nothing, on pools of one, two and twelve threads, for a
/// flat column and a column with repetition whose pages start on batch boundaries. The cap does
/// not depend on the pool's threads.
///
/// The read has one column, so the column's first unpublished row is the read's. The scheduler
/// is given a batch byte budget of one batch, which puts the cap at eight batches, or of a
/// sixteenth of one, which puts it below one batch and leaves the column only its next batch,
/// through the pages always admitted. Each read first
/// runs until the column has published the two batches its exchange holds and, for the flat
/// column, has submitted its pages up to the expected lead, and then until no page is submitted
/// for a moment; it then takes every batch, which a read that is not live never ends.
@Tag("pool")
class ReadAheadCapTest {

    private static final int BATCH = 64;
    private static final int ROWS = 40 * BATCH;
    /// Elements per record of the column with repetition.
    private static final int K = 3;
    /// The column's first unpublished row once its exchange is full.
    private static final long PUBLISHED_ROW = 2 * BATCH;
    /// How long the read may take to reach the state the test expects.
    private static final long STATE_NANOS = TimeUnit.SECONDS.toNanos(30);
    /// How long no page may be submitted, once the read is in that state, for it to count as
    /// settled there.
    private static final long QUIET_NANOS = TimeUnit.MILLISECONDS.toNanos(200);

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
                // Up to the cap: the last page starting fewer than eight batches past the first
                // unpublished row
                Arguments.of(1, false, smallPage, BATCH, 15 * smallPage),
                Arguments.of(2, false, smallPage, BATCH, 15 * smallPage),
                Arguments.of(12, false, smallPage, BATCH, 15 * smallPage),
                // Pages spanning two batches, up to the cap alike
                Arguments.of(1, false, largePage, BATCH, 3 * largePage),
                Arguments.of(2, false, largePage, BATCH, 3 * largePage),
                Arguments.of(12, false, largePage, BATCH, 3 * largePage),
                // A cap below one batch: the pages starting at most one batch past the first
                // unpublished row
                Arguments.of(12, false, smallPage, BATCH / 16, BATCH),
                Arguments.of(1, true, smallPage, BATCH, -1),
                Arguments.of(2, true, smallPage, BATCH, -1),
                Arguments.of(12, true, smallPage, BATCH, -1),
                Arguments.of(1, true, BATCH, BATCH, -1),
                Arguments.of(2, true, BATCH, BATCH, -1),
                Arguments.of(12, true, BATCH, BATCH, -1));
    }

    @ParameterizedTest(name = "{0} threads, repeated {1}, pages of {2} rows, budget of {3} rows")
    @MethodSource("reads")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void pagesAreSubmittedWithinTheCap(int threads, boolean repeated, int pageRows, long budgetRows,
                                       long expectedLead) throws Exception {
        long capRows = 8 * budgetRows;
        byte[] file = write(repeated, pageRows);
        List<String> violations = Collections.synchronizedList(new ArrayList<>());
        AtomicLong lastSubmission = new AtomicLong(System.nanoTime());
        AtomicLong maxFirstRow = new AtomicLong(-1);
        ReadScheduler.submissionObserver = (column, page, inFlight, lowestRow, readPublishedRow) -> {
            long ahead = page.firstRow - readPublishedRow;
            if (ahead > BATCH && ahead >= capRows) {
                violations.add("page at row " + page.firstRow + ", " + ahead + " rows ahead");
            }
            if (inFlight > 2 * threads) {
                violations.add("page at row " + page.firstRow + " with " + inFlight + " pages in flight");
            }
            maxFirstRow.accumulateAndGet(page.firstRow, Math::max);
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
            ReadScheduler scheduler = new ReadScheduler(iterator.readExecutor(), BATCH, budgetRows);
            if (repeated) {
                BatchExchange<NestedBatch> exchange = BatchExchange.detachingNested(column.name(), BATCH);
                try (ScheduledNestedColumn scheduled = new ScheduledNestedColumn(scheduler, source, exchange,
                        column, BATCH, context.decompressorFactory(),
                        NestedLevelComputer.computeLayers(iterator.referenceSchema())[column.columnIndex()],
                        NestedIndexMode.ALL_ITEMS, false)) {
                    scheduler.start();
                    awaitState(() -> scheduled.publishedRow() == PUBLISHED_ROW,
                            () -> "published to " + scheduled.publishedRow(), lastSubmission);
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
                    awaitState(() -> scheduled.publishedRow() == PUBLISHED_ROW
                            && maxFirstRow.get() == PUBLISHED_ROW + expectedLead,
                            () -> "published to " + scheduled.publishedRow() + ", last page submitted at "
                                    + maxFirstRow.get(),
                            lastSubmission);
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
        RowGroupIterator iterator = RowGroupIterators.standalone(List.of(input), context);
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)))) {
            FileSchema schema = reader.getFileSchema();
            iterator.setFirstFile(schema, reader.getFileMetaData().rowGroups());
            iterator.initialize(ReadProjection.of(ProjectedSchema.create(schema, ColumnProjection.all())), null, true);
        }
        return iterator;
    }
}
