/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.plan;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import dev.hardwood.InMemoryFiles;
import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.ObservedPool;
import dev.hardwood.internal.io.CountingInputFile;
import dev.hardwood.internal.reader.engine.RowGroupIterator;
import dev.hardwood.internal.reader.plan.GatedReads.GatedFile;
import dev.hardwood.internal.reader.plan.GatedReads.ReadGate;
import dev.hardwood.internal.reader.plan.GatedReads.RemoteLike;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

import static dev.hardwood.internal.reader.plan.GatedReads.awaitSettled;
import static org.assertj.core.api.Assertions.assertThat;

/// How far a read fetches ahead of its consumer (READ_PIPELINE.md, Back-pressure chain;
/// FETCH_PLANNING.md, Requests): the next unit's first request starts before any column
/// reaches its row group, also where F leaves no room, and a consumer that stops reading stops the
/// fetching once the read holds what the read-ahead cap and F allow.
class ReadAheadTest {

    @AfterEach
    void clearProperties() {
        System.clearProperty(RowGroupIterator.FETCH_AHEAD_BYTES_PROPERTY);
    }

    /// Four row groups of 50 rows, with a page index.
    private static final Path FOUR_ROW_GROUPS = Path.of("src/test/resources/differential/diff_order_multi.parquet");

    /// With F of zero and the first row group's data held back, so that no column can leave it, the
    /// read starts the first request of the second row group, its next unit, and nothing past it.
    @Test
    void theNextUnitStartsBeforeAnyColumnReachesItWithoutFetchAheadRoom() throws Exception {
        System.setProperty(RowGroupIterator.FETCH_AHEAD_BYTES_PROPERTY, "0");
        ReadGate firstData = new ReadGate(reason -> reason.startsWith("rg=0 col="));
        CountingInputFile file = new CountingInputFile(
                new GatedFile(InputFile.of(ByteBuffer.wrap(Files.readAllBytes(FOUR_ROW_GROUPS))), firstData));
        ObservedPool pool = new ObservedPool(4, "read-ahead-test");
        QuiescentExecutor io = new QuiescentExecutor();
        AtomicLong rows = new AtomicLong();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        try (HardwoodContextImpl context = pool.context(io);
             ParquetFileReader reader = ParquetFileReader.open(file, context)) {
            assertThat(reader.getFileMetaData().rowGroups()).hasSize(4);
            firstData.arm();
            Thread consumer = Thread.ofPlatform().name("consumer").start(() -> {
                try (ColumnReaders columns = reader.buildColumnReaders(ColumnProjection.all()).build()) {
                    while (columns.nextBatch()) {
                        rows.addAndGet(columns.getRecordCount());
                    }
                }
                catch (Throwable e) {
                    failure.set(e);
                }
            });

            awaitTrue(() -> !dataReads(file, 1).isEmpty(), "the second row group's first request starts");
            awaitSettled(io, firstData, pool);
            assertThat(firstData.inFlight()).as("first row group's reads held").isPositive();
            assertThat(dataReads(file, 1)).as("data reads of the next unit").hasSize(1);
            assertThat(dataReads(file, 2)).as("data reads of the third row group").isEmpty();
            assertThat(dataReads(file, 3)).as("data reads of the fourth row group").isEmpty();

            firstData.release();
            consumer.join(TimeUnit.SECONDS.toMillis(30));
            assertThat(consumer.isAlive()).as("consumer still reading").isFalse();
        }
        assertThat(failure.get()).isNull();
        assertThat(rows.get()).isEqualTo(200);
    }

    private static final int ROW_GROUPS = 40;
    private static final int ROWS_PER_ROW_GROUP = 400_000;
    private static final int BATCH = 50_000;
    private static final long FETCH_AHEAD = 1024;

    /// A consumer that takes one batch and stops: the read settles with every row group it fetched
    /// within the cap of the consumer's first unpublished row, the next unit, and F.
    @Test
    void aConsumerThatStopsReadingStopsTheFetching() throws Exception {
        System.setProperty(RowGroupIterator.FETCH_AHEAD_BYTES_PROPERTY, Long.toString(FETCH_AHEAD));
        byte[] bytes = constantRowGroups();
        CountingInputFile file = new CountingInputFile(new RemoteLike(InputFile.of(ByteBuffer.wrap(bytes))));
        ObservedPool pool = new ObservedPool(4, "read-ahead-test");
        QuiescentExecutor io = new QuiescentExecutor();
        try (HardwoodContextImpl context = pool.context(io);
             ParquetFileReader reader = ParquetFileReader.open(file, context)) {
            FileMetaData metaData = reader.getFileMetaData();
            assertThat(metaData.rowGroups()).hasSize(ROW_GROUPS);
            long capRows = RowGroupIterator.CAP_BUDGETS * BatchSizing.readAheadBudgetRows(
                    ProjectedSchema.create(reader.getFileSchema(), ColumnProjection.all()), metaData.rowGroups());
            try (ColumnReader column = reader.buildColumnReader("v").batchSize(BATCH).build()) {
                assertThat(column.nextBatch()).isTrue();

                awaitSettled(io, new ReadGate(reason -> false), pool);

                // The consumer holds one batch and the exchange two: no row from the fourth batch
                // on is published, and no page starting a batch past the cap from there is placed
                long walkLimit = 3L * BATCH + capRows + BATCH;
                int lastWalked = Math.toIntExact((walkLimit - 1) / ROWS_PER_ROW_GROUP);
                int nextUnit = lastWalked + 1;
                assertThat(nextUnit + 2).as("row groups the bound leaves unread").isLessThan(ROW_GROUPS);
                long speculative = 0;
                for (int rg = nextUnit + 1; rg < ROW_GROUPS; rg++) {
                    speculative += dataReads(file, rg).stream().mapToLong(CountingInputFile.Read::length).sum();
                }
                assertThat(speculative).as("bytes fetched past the next unit").isLessThanOrEqualTo(FETCH_AHEAD);
                assertThat(dataReads(file, ROW_GROUPS - 1)).as("data reads of the last row group").isEmpty();
            }
        }
    }

    /// The data reads of row group `rowGroup`.
    private static List<CountingInputFile.Read> dataReads(CountingInputFile file, int rowGroup) {
        return file.reads().stream().filter(read -> read.reason().startsWith("rg=" + rowGroup + " col=")).toList();
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

    /// [#ROW_GROUPS] row groups of [#ROWS_PER_ROW_GROUP] rows of one required `INT64` column `v`
    /// holding one value, which encodes to a few bytes per row group while each row group decodes
    /// to its full width: the cap, counted in decoded bytes, ends inside the file.
    private static byte[] constantRowGroups() throws Exception {
        long[] values = new long[ROWS_PER_ROW_GROUP];
        Arrays.fill(values, 42L);
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out,
                FileSchema.builder("constant").addColumn("v", PhysicalType.INT64, RepetitionType.REQUIRED).build(),
                WriterConfig.builder().rowGroupTargetRows(ROWS_PER_ROW_GROUP).build())) {
            for (int rg = 0; rg < ROW_GROUPS; rg++) {
                writer.columnWriter().writeBatch(batch -> batch.longs("v", values));
            }
        }
        return InMemoryFiles.toByteArray(out);
    }
}
