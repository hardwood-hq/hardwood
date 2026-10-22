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
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import dev.hardwood.HardwoodContext;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A context of zero threads runs a read on the thread calling the reader: every parse and decode
/// of a read runs there, a read of files in memory starts no thread at all, and a read of files
/// that are not in memory reads on virtual threads, which do nothing else. A writer on such a
/// context writes without asking for a pool.
///
/// The fixture is three files of three row groups of 1000 rows: `id` runs on across the files,
/// `bucket` is `id % 4`, dictionary-encoded, and `v` is a list of two `int32` per row, `id` and
/// `-id`.
class ZeroThreadContextTest {

    private static final int FILES = 3;
    private static final int ROWS_PER_GROUP = 1000;
    private static final int ROWS_PER_FILE = 3 * ROWS_PER_GROUP;
    private static final int ROWS = FILES * ROWS_PER_FILE;

    @TempDir
    static Path dir;

    private static final List<Path> files = new ArrayList<>();

    /// The threads every parse and decode ran on.
    private final ConcurrentLinkedQueue<Thread> workThreads = new ConcurrentLinkedQueue<>();
    /// Tasks started on the context's I/O executor.
    private final AtomicInteger ioTasks = new AtomicInteger();

    @BeforeAll
    static void writeFixtures() throws IOException {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT64, RepetitionType.REQUIRED)
                .addColumn("bucket", PhysicalType.INT64, RepetitionType.REQUIRED)
                .list("v", RepetitionType.REQUIRED, el -> el.primitive(PhysicalType.INT32, RepetitionType.REQUIRED))
                .build();
        WriterConfig config = WriterConfig.builder().rowGroupTargetRows(ROWS_PER_GROUP).build();
        for (int f = 0; f < FILES; f++) {
            Path path = dir.resolve("zero_" + f + ".parquet");
            long[] ids = new long[ROWS_PER_FILE];
            long[] buckets = new long[ROWS_PER_FILE];
            int[] offsets = new int[ROWS_PER_FILE + 1];
            int[] elements = new int[2 * ROWS_PER_FILE];
            for (int i = 0; i < ROWS_PER_FILE; i++) {
                long id = (long) f * ROWS_PER_FILE + i;
                ids[i] = id;
                buckets[i] = id % 4;
                offsets[i + 1] = 2 * (i + 1);
                elements[2 * i] = (int) id;
                elements[2 * i + 1] = (int) -id;
            }
            offsets[0] = 0;
            try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(path), schema, config)) {
                writer.columnWriter().writeBatch(batch -> batch.longs("id", ids).longs("bucket", buckets)
                        .list("v", offsets).ints("v.list.element", elements));
            }
            files.add(path);
        }
    }

    @BeforeEach
    void observe() {
        HardwoodContextImpl.decodeObserver = caller -> workThreads.add(Thread.currentThread());
        HardwoodContextImpl.computeObserver = (what, caller) -> workThreads.add(Thread.currentThread());
    }

    @AfterEach
    void stopObserving() {
        HardwoodContextImpl.decodeObserver = null;
        HardwoodContextImpl.computeObserver = null;
    }

    /// The reads of the fixture each test runs, each returning a checksum of what it read.
    enum Read {
        FLAT_ROWS {
            @Override
            long run(ParquetFileReader reader) throws IOException {
                long sum = 0;
                try (RowReader rows = reader.buildRowReader().projection(ColumnProjection.columns("id", "bucket")).build()) {
                    while (rows.hasNext()) {
                        rows.next();
                        sum += rows.getLong("id") + rows.getLong("bucket");
                    }
                }
                return sum;
            }

            @Override
            long expected() {
                return idSum() + bucketSum();
            }
        },
        NESTED_ROWS {
            @Override
            long run(ParquetFileReader reader) throws IOException {
                long sum = 0;
                try (RowReader rows = reader.rowReader()) {
                    while (rows.hasNext()) {
                        rows.next();
                        sum += rows.getLong("id") + rows.getList("v").size();
                    }
                }
                return sum;
            }

            @Override
            long expected() {
                return idSum() + 2L * ROWS;
            }
        },
        COLUMNS {
            @Override
            long run(ParquetFileReader reader) throws IOException {
                long sum = 0;
                try (ColumnReaders columns = reader.buildColumnReaders(ColumnProjection.columns("id", "bucket")).build()) {
                    ColumnReader id = columns.getColumnReader("id");
                    ColumnReader bucket = columns.getColumnReader("bucket");
                    while (columns.nextBatch()) {
                        long[] ids = id.getLongs();
                        long[] buckets = bucket.getLongs();
                        for (int i = 0; i < columns.getRecordCount(); i++) {
                            sum += ids[i] + buckets[i];
                        }
                    }
                }
                return sum;
            }

            @Override
            long expected() {
                return idSum() + bucketSum();
            }
        },
        FILTERED_COLUMN {
            @Override
            long run(ParquetFileReader reader) throws IOException {
                long sum = 0;
                try (ColumnReaders columns = reader.buildColumnReaders(ColumnProjection.columns("id"))
                        .filter(FilterPredicate.eq("bucket", 3L)).build()) {
                    ColumnReader id = columns.getColumnReader("id");
                    while (columns.nextBatch()) {
                        long[] ids = id.getLongs();
                        for (int i = 0; i < columns.getRecordCount(); i++) {
                            sum += ids[i];
                        }
                    }
                }
                return sum;
            }

            @Override
            long expected() {
                long sum = 0;
                for (long id = 3; id < ROWS; id += 4) {
                    sum += id;
                }
                return sum;
            }
        };

        abstract long run(ParquetFileReader reader) throws IOException;

        abstract long expected();

        private static long idSum() {
            return (long) ROWS * (ROWS - 1) / 2;
        }

        private static long bucketSum() {
            return (long) ROWS / 4 * (0 + 1 + 2 + 3);
        }
    }

    @ParameterizedTest
    @EnumSource(Read.class)
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aReadOfFilesInMemoryRunsOnTheCallingThreadAlone(Read read) throws Exception {
        try (HardwoodContextImpl context = HardwoodContextImpl.withIoExecutor(0, countingIoExecutor());
             ParquetFileReader reader = ParquetFileReader.openAll(inputFiles(), context)) {
            assertThat(read.run(reader)).isEqualTo(read.expected());
        }

        assertThat(workThreads).as("threads parsing and decoding").isNotEmpty().containsOnly(Thread.currentThread());
        assertThat(ioTasks).as("tasks on the I/O executor").hasValue(0);
    }

    @ParameterizedTest
    @EnumSource(Read.class)
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aReadOfFilesNotInMemoryReadsOnVirtualThreadsThatDoNothingElse(Read read) throws Exception {
        List<ThreadRecordingInputFile> inputs = new ArrayList<>();
        for (Path path : files) {
            inputs.add(new ThreadRecordingInputFile(InputFile.of(path)));
        }
        try (HardwoodContextImpl context = HardwoodContextImpl.withIoExecutor(0, countingIoExecutor());
             ParquetFileReader reader = ParquetFileReader.openAll(List.copyOf(inputs), context)) {
            assertThat(read.run(reader)).isEqualTo(read.expected());
        }

        assertThat(workThreads).as("threads parsing and decoding").isNotEmpty().containsOnly(Thread.currentThread());
        for (ThreadRecordingInputFile input : inputs) {
            assertThat(input.readers).as("threads reading").allMatch(
                    thread -> thread == Thread.currentThread() || thread.isVirtual());
        }
        assertThat(ioTasks).as("tasks on the I/O executor").hasPositiveValue();
    }

    /// The parent reader closed on another thread while the consumer waits for a held read: the
    /// close waits for the read, and the consumer's poll returns. Whether it ends the stream or
    /// fails is not asserted, as for a read on a pool ([dev.hardwood.reader.ParentCloseTest]).
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aCloseOnAnotherThreadWhileTheConsumerWaitsWaitsForTheReadInFlight() throws Exception {
        try (HardwoodContextImpl context = HardwoodContextImpl.create(0);
             HeldRead read = HeldRead.start(context)) {
            Thread closer = Thread.ofPlatform().start(read::closeReader);

            assertThat(closer.join(Duration.ofMillis(300))).as("the close returned while a read was held").isFalse();
            read.release();
            closer.join();
            read.consumer.join();

            assertThat(read.closeFailure).isNull();
            assertThat(read.outcome).isNotNull();
            read.assertFilesClosedOnce();
        }
    }

    /// The column group and its parent reader closed on two threads at once while a read is
    /// held: both closes return once it is released.
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void twoClosesAtOnceBothReturnOnceTheReadInFlightCompletes() throws Exception {
        try (HardwoodContextImpl context = HardwoodContextImpl.create(0);
             HeldRead read = HeldRead.start(context)) {
            AtomicReference<Throwable> columnsCloseFailure = new AtomicReference<>();
            Thread columnsCloser = Thread.ofPlatform().start(() -> {
                try {
                    read.columns.close();
                }
                catch (Throwable e) {
                    columnsCloseFailure.set(e);
                }
            });
            Thread readerCloser = Thread.ofPlatform().start(read::closeReader);

            assertThat(columnsCloser.join(Duration.ofMillis(300))).as("the columns' close returned while a read was held")
                    .isFalse();
            read.release();
            columnsCloser.join();
            readerCloser.join();
            read.consumer.join();

            assertThat(columnsCloseFailure.get()).isNull();
            assertThat(read.closeFailure).isNull();
            read.assertFilesClosedOnce();
        }
    }

    /// A close interrupted while it waits for a held read goes on waiting, and returns with the
    /// interrupt restored once the read completes.
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void anInterruptedCloseWaitsForTheReadInFlight() throws Exception {
        try (HardwoodContextImpl context = HardwoodContextImpl.create(0);
             HeldRead read = HeldRead.start(context)) {
            AtomicBoolean interruptedAfterClose = new AtomicBoolean();
            Thread closer = Thread.ofPlatform().start(() -> {
                read.closeReader();
                interruptedAfterClose.set(Thread.currentThread().isInterrupted());
            });
            assertThat(closer.join(Duration.ofMillis(300))).isFalse();

            closer.interrupt();

            assertThat(closer.join(Duration.ofMillis(300))).as("the interrupted close returned while a read was held")
                    .isFalse();
            read.release();
            closer.join();
            read.consumer.join();
            assertThat(read.closeFailure).isNull();
            assertThat(interruptedAfterClose).isTrue();
            read.assertFilesClosedOnce();
        }
    }

    /// A consumer interrupted while it waits for a held read ends its poll with an
    /// [InterruptedIOException], and the read closes afterwards.
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void anInterruptedConsumerStopsWaiting() throws Exception {
        try (HardwoodContextImpl context = HardwoodContextImpl.create(0);
             HeldRead read = HeldRead.start(context)) {
            read.consumer.interrupt();
            read.consumer.join();

            assertThat(read.outcome).isInstanceOfSatisfying(InterruptedIOException.class,
                    e -> assertThat(e).hasMessage("Interrupted while waiting for the next batch of column 'element'"));
            read.release();
            read.closeReader();
            assertThat(read.closeFailure).isNull();
            read.assertFilesClosedOnce();
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aReadWhoseContextIsClosedFailsAtItsNextStep() throws Exception {
        HardwoodContextImpl context = HardwoodContextImpl.create(0);
        try (ParquetFileReader reader = ParquetFileReader.openAll(inputFiles(), context);
             ColumnReaders columns = reader.buildColumnReaders(ColumnProjection.columns("id")).batchSize(100).build()) {
            assertThat(columns.nextBatch()).isTrue();
            context.close();
            assertThatThrownBy(() -> {
                while (columns.nextBatch()) {
                    // reads until the read fails
                }
            }).isInstanceOf(IllegalStateException.class)
                    .hasMessage("The HardwoodContext was closed while a reader using it was reading");
        }
    }

    /// A poll that finds no batch, nothing queued and no read in flight fails rather than parks:
    /// nothing could publish a batch any more.
    @Test
    void aPollNothingCanServeFails() throws Exception {
        try (HardwoodContextImpl context = HardwoodContextImpl.create(0)) {
            BatchExchange<BatchExchange.Batch> exchange = BatchExchange.detachingFlat("c", 10);
            exchange.setQueue(new ReadQueue(context));

            assertThatThrownBy(exchange::poll)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("The read of column 'c' stalled: no batch is ready, "
                            + "and no task is queued or running and no read in flight");
        }
    }

    @Test
    void aContextOfZeroThreadsHasNoExecutor() {
        try (HardwoodContext context = HardwoodContext.builder().threads(0).build()) {
            assertThatThrownBy(context::executor)
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessage("A HardwoodContext of zero threads has no executor");
        }
    }

    /// A compressed write of several columns, which a context with a pool appends and encodes
    /// beside the caller, writes without one: the writer never asks for the context's executor,
    /// which would throw, and the file reads back.
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aWriterOfSeveralCompressedColumnsWritesWithoutAPool() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("a", PhysicalType.INT64, RepetitionType.REQUIRED)
                .addColumn("b", PhysicalType.INT64, RepetitionType.REQUIRED)
                .addColumn("c", PhysicalType.DOUBLE, RepetitionType.OPTIONAL)
                .build();
        WriterConfig config = WriterConfig.builder().codec(CompressionCodec.ZSTD).rowGroupTargetRows(ROWS_PER_GROUP)
                .build();
        Path path = dir.resolve("written_on_zero_threads.parquet");
        try (HardwoodContextImpl context = HardwoodContextImpl.create(0)) {
            try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(path), schema, config, context)) {
                for (int i = 0; i < ROWS; i++) {
                    long value = i;
                    writer.rowWriter().writeRow(row -> row.setLong("a", value).setLong("b", -value)
                            .setDouble("c", value * 0.5));
                }
            }
            long sum = 0;
            try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path), context);
                 RowReader rows = reader.rowReader()) {
                while (rows.hasNext()) {
                    rows.next();
                    sum += rows.getLong("a") + rows.getLong("b") + 1;
                }
            }
            assertThat(sum).isEqualTo(ROWS);
        }
    }

    private Executor countingIoExecutor() {
        return task -> {
            ioTasks.incrementAndGet();
            Thread.ofVirtual().start(task);
        };
    }

    private static List<InputFile> inputFiles() {
        List<InputFile> inputs = new ArrayList<>();
        for (Path path : files) {
            inputs.add(InputFile.of(path));
        }
        return inputs;
    }

    /// A read of the fixture's files, not in memory, over `v` in batches of 100, whose
    /// consumer runs on a thread of its own and waits for a read held after the first batch.
    private static final class HeldRead implements AutoCloseable {

        final CountDownLatch held = new CountDownLatch(1);
        final CountDownLatch released = new CountDownLatch(1);
        final AtomicBoolean armed = new AtomicBoolean();
        final List<HeldInputFile> inputs = new ArrayList<>();
        ParquetFileReader reader;
        ColumnReaders columns;
        Thread consumer;
        /// `Boolean.FALSE` once the consumer reached the end of the stream, or what it threw;
        /// `null` while it reads.
        volatile Object outcome;
        volatile Throwable closeFailure;

        /// Opens the read, takes its first batch, arms the hold and starts the consumer, and
        /// returns once a read is held and the consumer waits for it.
        static HeldRead start(HardwoodContextImpl context) throws Exception {
            HeldRead read = new HeldRead();
            for (Path path : files) {
                read.inputs.add(new HeldInputFile(InputFile.of(path), read));
            }
            read.reader = ParquetFileReader.openAll(List.copyOf(read.inputs), context);
            read.columns = read.reader.buildColumnReaders(ColumnProjection.columns("v")).batchSize(100).build();
            assertThat(read.columns.nextBatch()).isTrue();
            read.armed.set(true);
            read.consumer = Thread.ofPlatform().start(() -> {
                try {
                    while (read.columns.nextBatch()) {
                        // reads until the hold stops it
                    }
                    read.outcome = Boolean.FALSE;
                }
                catch (Throwable e) {
                    read.outcome = e;
                }
            });
            assertThat(read.held.await(10, TimeUnit.SECONDS)).as("a read held").isTrue();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (read.consumer.getState() != Thread.State.WAITING) {
                assertThat(System.nanoTime() - deadline).as("the consumer parked on the held read").isNegative();
                Thread.onSpinWait();
            }
            return read;
        }

        void release() {
            released.countDown();
        }

        void closeReader() {
            try {
                reader.close();
            }
            catch (Throwable e) {
                closeFailure = e;
            }
        }

        void assertFilesClosedOnce() {
            for (HeldInputFile input : inputs) {
                assertThat(input.closeCount()).isEqualTo(1);
            }
        }

        @Override
        public void close() {
            release();
            closeReader();
        }
    }

    /// A file not in memory whose reads, once its [HeldRead] is armed, wait until it is released.
    private static final class HeldInputFile extends CountingInputFile {

        private final HeldRead read;

        HeldInputFile(InputFile delegate, HeldRead read) {
            super(delegate);
            this.read = read;
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            if (read.armed.get()) {
                read.held.countDown();
                boolean interrupted = false;
                while (true) {
                    try {
                        read.released.await();
                        break;
                    }
                    catch (InterruptedException e) {
                        interrupted = true;
                    }
                }
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
            return super.readRange(offset, length);
        }
    }

    /// A file not in memory, which records the threads reading it.
    private static final class ThreadRecordingInputFile extends CountingInputFile {

        final ConcurrentLinkedQueue<Thread> readers = new ConcurrentLinkedQueue<>();

        ThreadRecordingInputFile(InputFile delegate) {
            super(delegate);
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            readers.add(Thread.currentThread());
            return super.readRange(offset, length);
        }
    }
}
