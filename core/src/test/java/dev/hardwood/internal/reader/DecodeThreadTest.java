/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.SchemaIncompatibleException;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A read decodes on its context's pool only. Dictionary pages are handed to the pool by whichever
/// thread needs them: the calling thread probing them for a filter, a fetcher starting a column
/// chunk, and a planning read on the common pool. A flat column's cell its fetcher seals after its
/// last fill is completed by a [SealedFillTask], which the scheduler submits like a page: a
/// completion takes the buffers only a task of the pool holds ([ScheduledColumn#completed]).
/// These tests run the read's tasks on [TaskOrderExecutor#inline()], so that every page is filled
/// before the next is placed and the fetcher's seal comes last.
///
/// The fixture holds three row groups of 1000 rows, `id` running `0..2999`; `bucket` is `0`
/// below 1500 and `1` from there, dictionary-encoded, so `bucket = 0` is decided by the
/// dictionaries. `id < 1000` proves the first row group matches in full, so a read of `amount`
/// filtered on it reads no `id` there: the column's fetcher places a boundary marker.
class DecodeThreadTest {

    /// A Snappy dictionary page in a file of one row group, which the filter's probe reads.
    private static final Path SINGLE_ROW_GROUP = Path.of("src/test/resources/dict_compressed_page.parquet");

    private static final int ROWS = 3000;
    private static final int ROWS_PER_GROUP = 1000;

    @TempDir
    static Path dir;

    private static Path file;
    /// `amount` required rather than optional, so a read of both files fails between them.
    private static Path incompatibleFile;

    private final ConcurrentLinkedQueue<Thread> callers = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<Thread> offPool = new ConcurrentLinkedQueue<>();
    private final AtomicInteger flatSealedFillTasks = new AtomicInteger();

    @BeforeAll
    static void writeFixtures() throws IOException {
        WriterConfig config = WriterConfig.builder().rowGroupTargetRows(ROWS_PER_GROUP).build();
        file = dir.resolve("decode_thread.parquet");
        write(file, RepetitionType.OPTIONAL, config);
        incompatibleFile = dir.resolve("decode_thread_required.parquet");
        write(incompatibleFile, RepetitionType.REQUIRED, config);
    }

    private static void write(Path path, RepetitionType amount, WriterConfig config) throws IOException {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT64, RepetitionType.REQUIRED)
                .addColumn("amount", PhysicalType.DOUBLE, amount)
                .addColumn("bucket", PhysicalType.INT64, RepetitionType.REQUIRED)
                .build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(path), schema, config)) {
            for (int i = 0; i < ROWS; i++) {
                final long id = i;
                writer.rowWriter().writeRow(row -> row.setLong("id", id).setDouble("amount", id * 0.5)
                        .setLong("bucket", id < 1500 ? 0 : 1));
            }
        }
    }

    @BeforeEach
    void observe() {
        HardwoodContextImpl.decodeObserver = caller -> {
            callers.add(caller);
            if (!Thread.currentThread().getName().startsWith("hardwood-")) {
                offPool.add(Thread.currentThread());
            }
        };
        ReadScheduler.submissionObserver = (column, page, inFlight, lowestRow, readPublishedRow) -> {
            if (page instanceof SealedFillTask && column instanceof ScheduledFlatColumn) {
                flatSealedFillTasks.incrementAndGet();
            }
        };
    }

    @AfterEach
    void stopObserving() {
        HardwoodContextImpl.decodeObserver = null;
        ReadScheduler.submissionObserver = null;
    }

    /// The calling thread probes the dictionary of the file's one row group before the read
    /// starts.
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void theCallingThreadsDictionaryProbeDecodesOnThePool() throws Exception {
        try (HardwoodContextImpl context = HardwoodContextImpl.create(2);
             ParquetFileReader reader = ParquetFileReader.open(InputFile.of(SINGLE_ROW_GROUP), context);
             ColumnReader values = reader.buildColumnReader("label")
                     .filter(FilterPredicate.eq("label", "pad_0_" + "z".repeat(40))).build()) {
            assertThat(read(values)).isEqualTo(79);
        }

        assertThat(offPool).isEmpty();
        assertThat(callers).as("the calling thread's probe").contains(Thread.currentThread());
    }

    /// Each column's fetcher parses the dictionary of every chunk it reads, and under a filter the
    /// next row group's planning reads parse its dictionaries on the common pool: every one is
    /// decoded on the context's pool.
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aReadDecodesEveryDictionaryOnThePool() throws Exception {
        try (HardwoodContextImpl context = HardwoodContextImpl.create(2);
             ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file), context)) {
            try (ColumnReader values = reader.buildColumnReader("bucket").build()) {
                assertThat(read(values)).isEqualTo(ROWS);
            }
            assertThat(callers).as("a fetcher's chunk dictionary").anyMatch(Thread::isVirtual);
            try (ColumnReader values = reader.buildColumnReader("bucket")
                    .filter(FilterPredicate.eq("bucket", 0L)).build()) {
                assertThat(read(values)).isEqualTo(1500);
            }
        }

        assertThat(callers).hasSizeGreaterThan(ROWS / ROWS_PER_GROUP);
        assertThat(offPool).isEmpty();
    }

    /// The next row group's planning reads run on the common pool, its dictionaries under a filter
    /// among them.
    @Test
    void aDictionaryParsedOnTheCommonPoolDecodesOnThePool() throws Exception {
        try (HardwoodContextImpl context = HardwoodContextImpl.create(1);
             ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
             InputFile input = InputFile.of(file)) {
            ColumnSchema bucket = reader.getFileSchema().getColumn("bucket");
            ColumnChunk chunk = reader.getFileMetaData().rowGroups().get(1).columns().get(bucket.columnIndex());
            input.open();
            ByteBuffer page = DictionaryParser.readPage(input, chunk, "");

            Thread planner = CompletableFuture.supplyAsync(() -> {
                assertThat(DictionaryParser.parse(page, bucket, chunk.metaData(), context)).isNotNull();
                return Thread.currentThread();
            }, ForkJoinPool.commonPool()).join();

            assertThat(offPool).isEmpty();
            assertThat(callers).containsExactly(planner);
        }
    }

    /// The fetcher of `id`, a filter-only column, places a boundary marker for the first row
    /// group, which seals the column's open cell once every page in it has been filled: the read's
    /// tasks run as they are submitted, so each page is filled before the next is placed.
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aSealedFillTaskCompletesTheCellABoundaryMarkerSeals() throws Exception {
        TaskOrderExecutor executor = TaskOrderExecutor.inline(null);
        try (HardwoodContextImpl context = executor.context(2);
             ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file), context);
             ColumnReader values = reader.buildColumnReader("amount")
                     .filter(FilterPredicate.lt("id", 1000L)).batchSize(64).build()) {
            assertThat(read(values)).isEqualTo(1000);
        }

        assertThat(executor.failure()).isNull();
        assertThat(flatSealedFillTasks).hasPositiveValue();
    }

    /// The second file fails its schema check as the fetcher reaches it, between two work items:
    /// the fetcher seals the open cell, which spans the whole first file, once every page in it
    /// has been filled, and it is published before the failure is raised.
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aSealedFillTaskCompletesTheCellAFetchFailureBetweenWorkItemsSeals() throws Exception {
        TaskOrderExecutor executor = TaskOrderExecutor.inline(null);
        AtomicInteger records = new AtomicInteger();
        try (HardwoodContextImpl context = executor.context(2);
             ParquetFileReader reader = ParquetFileReader.openAll(
                     List.of(InputFile.of(file), InputFile.of(incompatibleFile)), context);
             ColumnReaders values = reader.buildColumnReaders(ColumnProjection.columns("amount"))
                     .batchSize(4096).build()) {
            assertThatThrownBy(() -> {
                while (values.nextBatch()) {
                    records.addAndGet(values.getRecordCount());
                }
            }).isInstanceOf(SchemaIncompatibleException.class)
                    .hasMessage("[decode_thread_required.parquet] Column 'amount' has incompatible repetition type: "
                            + "expected OPTIONAL but found REQUIRED");
        }

        assertThat(records).as("records published before the failure").hasValue(ROWS);
        assertThat(executor.failure()).isNull();
        assertThat(flatSealedFillTasks).hasPositiveValue();
    }

    private static int read(ColumnReader values) throws IOException {
        int rows = 0;
        while (values.nextBatch()) {
            rows += values.getRecordCount();
        }
        return rows;
    }
}
