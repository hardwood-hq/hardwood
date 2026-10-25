/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.hardwood.InMemoryFiles;
import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.internal.FetchReason;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.io.HeldBytes;
import dev.hardwood.internal.reader.fetch.CoalescingPolicy;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A read never waits for the bytes of a page on a thread of its pool: the read of a chunk or region
/// whose bytes are not in memory runs on a virtual thread, and the walk that stopped on it resumes
/// where it stopped once it has arrived. A read that fails is read once more, and its second
/// failure fails the read, naming the file, offset and length.
class ReadYieldTest {

    /// Four row groups of 50 rows, with a page index.
    private static final Path INDEXED_FILE = Path.of("src/test/resources/differential/diff_order_multi.parquet");
    /// 2,000 rows in pages of about 512 bytes, without a page index.
    private static final Path SEQUENTIAL_FILE = Path.of("src/test/resources/misaligned_pages_no_index.parquet");
    /// As [#SEQUENTIAL_FILE], with a dictionary page of about 6.7 KiB.
    private static final Path DICTIONARY_FILE = Path.of("src/test/resources/misaligned_pages_dict_no_index.parquet");

    @AfterEach
    void clearPieceSize() {
        System.clearProperty(CoalescingPolicy.MAX_SPAN_BYTES_PROPERTY);
    }

    @Test
    void indexedColumnReadsRunOnVirtualThreads() throws Exception {
        ThreadRecordingInputFile file = new ThreadRecordingInputFile(InputFile.of(INDEXED_FILE));
        long rows = readAllRows(file);

        assertThat(rows).isEqualTo(200);
        assertColumnReadsOnVirtualThreads(file);
    }

    /// With 300-byte windows, page headers and pages straddle windows, so a walk stops in the middle
    /// of a page as well as between pages.
    @ParameterizedTest
    @ValueSource(ints = { 300, 4 * 1024 })
    void sequentialColumnReadsRunOnVirtualThreads(int pieceSize) throws Exception {
        System.setProperty(CoalescingPolicy.MAX_SPAN_BYTES_PROPERTY, String.valueOf(pieceSize));
        ThreadRecordingInputFile file = new ThreadRecordingInputFile(InputFile.of(SEQUENTIAL_FILE));
        long rows = readAllRows(file);

        assertThat(rows).isEqualTo(2_000);
        assertColumnReadsOnVirtualThreads(file);
    }

    /// A file whose bytes are in memory is read in place, wherever the walk needs them.
    @Test
    void inMemoryFileIsReadWithoutYielding() throws Exception {
        assertThat(HeldBytes.inMemory(InputFile.of(INDEXED_FILE))).isTrue();
        assertThat(HeldBytes.inMemory(InputFile.of(ByteBuffer.allocate(1)))).isTrue();
        assertThat(HeldBytes.inMemory(new ThreadRecordingInputFile(InputFile.of(INDEXED_FILE)))).isFalse();
    }

    /// A filtered read whose walks stop on reads resumes each masked page where it stopped: with
    /// 300-byte windows, the pages an inline-statistics drop or a row mask keeps or trims straddle
    /// windows, and the rows read match those of the same read over the file in memory.
    @Test
    void maskedSequentialWalkResumesWhereItStopped() throws Exception {
        System.setProperty(CoalescingPolicy.MAX_SPAN_BYTES_PROPERTY, "300");
        FilterPredicate filter = FilterPredicate.and(
                FilterPredicate.gtEq("narrow", 400),
                FilterPredicate.lt("narrow", 1600));
        ThreadRecordingInputFile remote = new ThreadRecordingInputFile(InputFile.of(SEQUENTIAL_FILE));

        List<String> expected = readFilteredRows(InputFile.of(SEQUENTIAL_FILE), filter);
        List<String> read = readFilteredRows(remote, filter);

        assertThat(expected).hasSize(1_200);
        assertThat(read).isEqualTo(expected);
        assertColumnReadsOnVirtualThreads(remote);
    }

    /// As [#maskedSequentialWalkResumesWhereItStopped], over the page groups a page index keeps.
    @Test
    void maskedIndexedWalkResumesWhereItStopped() throws Exception {
        FilterPredicate filter = FilterPredicate.gt("u32", Integer.parseUnsignedInt("3000000000"));
        ThreadRecordingInputFile remote = new ThreadRecordingInputFile(InputFile.of(INDEXED_FILE));

        List<Integer> expected = readFilteredInts(InputFile.of(INDEXED_FILE), filter);
        List<Integer> read = readFilteredInts(remote, filter);

        assertThat(expected).isNotEmpty();
        assertThat(read).isEqualTo(expected);
        assertColumnReadsOnVirtualThreads(remote);
    }

    /// While a column's read is held, the read holds no thread of the pool: the pool's one thread
    /// runs other work. A close waits for the held read, and returns once it completes.
    @Tag("pool")
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aReadWaitingForBytesHoldsNoThreadAndItsCloseWaitsForTheRead() throws Exception {
        HeldInputFile file = new HeldInputFile(InputFile.of(INDEXED_FILE));
        try (HardwoodContextImpl context = HardwoodContextImpl.create(1)) {
            ParquetFileReader reader = ParquetFileReader.open(file, context);
            ColumnReader column = reader.buildColumnReader("__row__").build();
            Thread consumer = Thread.ofPlatform().start(() -> drainQuietly(column));
            assertThat(file.entered.await(10, TimeUnit.SECONDS)).as("a column read is held").isTrue();

            assertThat(context.executor().submit(() -> 42).get(10, TimeUnit.SECONDS)).isEqualTo(42);

            Thread closer = Thread.ofPlatform().start(() -> closeQuietly(reader));
            closer.join(300);
            assertThat(closer.isAlive()).as("close waiting for the held read").isTrue();

            file.release.countDown();
            closer.join(10_000);
            assertThat(closer.isAlive()).as("close after the read completed").isFalse();
            consumer.join(10_000);
            assertThat(consumer.isAlive()).isFalse();
        }
    }

    /// A read whose bytes arrive after its context was closed fails with the context's closing
    /// rather than waiting for a pool that refuses it.
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aReadCompletingOnAClosedContextFailsTheRead() throws Exception {
        HeldInputFile file = new HeldInputFile(InputFile.of(INDEXED_FILE));
        HardwoodContextImpl context = HardwoodContextImpl.create(1);
        try (ParquetFileReader reader = ParquetFileReader.open(file, context);
             ColumnReader column = reader.buildColumnReader("__row__").build()) {
            Thread releaser = Thread.ofPlatform().start(() -> {
                try {
                    file.entered.await();
                    context.close();
                    file.release.countDown();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            assertThatThrownBy(() -> {
                while (column.nextBatch()) {
                    // reads on
                }
            })
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("The HardwoodContext was closed while a reader using it was reading")
                    .cause()
                    .isInstanceOf(RejectedExecutionException.class);
            releaser.join();
        }
    }

    /// A column read that fails is read again, and the read goes on.
    @Test
    void aFailedColumnReadIsReadAgain() throws Exception {
        FailingColumnReads file = new FailingColumnReads(InputFile.of(INDEXED_FILE), 1,
                new IOException("connection reset"));

        assertThat(readAllRows(file)).isEqualTo(200);
        assertThat(file.failed).hasSize(1);
        assertThat(file.reads).contains(file.failed.getFirst());
    }

    /// A column read that fails twice fails the read, naming the file, offset and length.
    @Test
    void aColumnReadThatFailsAgainFailsTheRead() throws Exception {
        FailingColumnReads file = new FailingColumnReads(singleChunkFile(), Integer.MAX_VALUE,
                new IOException("connection reset"));

        assertThatThrownBy(() -> readAllRows(file))
                .isInstanceOf(IOException.class)
                .satisfies(e -> assertThat(e).hasMessage("[single_chunk.parquet] Failed to fetch data at offset "
                        + file.failed.getFirst().offset() + " (length " + file.failed.getFirst().length() + ")"))
                .cause()
                .hasMessage("connection reset");
        assertThat(file.failed).hasSize(2).containsOnly(file.failed.getFirst());
    }

    /// A checked exception an `InputFile` throws undeclared fails the read as one it declares,
    /// instead of escaping it unplaced.
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aCheckedExceptionThrownUndeclaredFailsTheRead() throws Exception {
        FailingColumnReads file = new FailingColumnReads(singleChunkFile(), Integer.MAX_VALUE,
                new TimeoutException("too slow"));

        assertThatThrownBy(() -> readAllRows(file))
                .isInstanceOf(IOException.class)
                .satisfies(e -> assertThat(e).hasMessage("[single_chunk.parquet] Failed to fetch data at offset "
                        + file.failed.getFirst().offset() + " (length " + file.failed.getFirst().length() + ")"))
                .cause()
                .isInstanceOf(TimeoutException.class)
                .hasMessage("too slow");
    }

    private static void drainQuietly(ColumnReader column) {
        try {
            while (column.nextBatch()) {
                // reads on
            }
        }
        catch (Exception e) {
            // The reader is closed under it
        }
    }

    private static void closeQuietly(ParquetFileReader reader) {
        try {
            reader.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static long readAllRows(InputFile file) throws Exception {
        long rows = 0;
        try (HardwoodContextImpl context = HardwoodContextImpl.create(2);
             ParquetFileReader reader = ParquetFileReader.open(file, context);
             RowReader rowReader = reader.rowReader()) {
            while (rowReader.hasNext()) {
                rowReader.next();
                rows++;
            }
        }
        return rows;
    }

    private static List<String> readFilteredRows(InputFile file, FilterPredicate filter) throws Exception {
        List<String> rows = new ArrayList<>();
        try (HardwoodContextImpl context = HardwoodContextImpl.create(2);
             ParquetFileReader reader = ParquetFileReader.open(file, context);
             RowReader rowReader = reader.buildRowReader().filter(filter).build()) {
            while (rowReader.hasNext()) {
                rowReader.next();
                rows.add(rowReader.getInt("narrow") + ":" + new String(rowReader.getBinary("wide"), StandardCharsets.UTF_8));
            }
        }
        return rows;
    }

    private static List<Integer> readFilteredInts(InputFile file, FilterPredicate filter) throws Exception {
        List<Integer> values = new ArrayList<>();
        try (HardwoodContextImpl context = HardwoodContextImpl.create(2);
             ParquetFileReader reader = ParquetFileReader.open(file, context);
             ColumnReader column = reader.buildColumnReader("u32").filter(filter).build()) {
            while (column.nextBatch()) {
                for (int value : column.getInts()) {
                    values.add(value);
                }
            }
        }
        return values;
    }

    /// Every read of a column's pages ran on a virtual thread.
    private static void assertColumnReadsOnVirtualThreads(ThreadRecordingInputFile file) {
        List<ThreadRecordingInputFile.Read> columnReads = file.reads.stream()
                .filter(read -> read.reason().contains(" col="))
                .toList();
        assertThat(columnReads).isNotEmpty();
        assertThat(columnReads).allSatisfy(read -> assertThat(read.virtual())
                .as("read for %s on a virtual thread", read.reason())
                .isTrue());
    }

    /// One required `INT64` column of 100 rows in one row group: its data is one request.
    private static InputFile singleChunkFile() throws IOException {
        FileSchema schema = FileSchema.builder("single_chunk")
                .addColumn("v", PhysicalType.INT64, RepetitionType.REQUIRED)
                .build();
        long[] values = new long[100];
        for (int i = 0; i < values.length; i++) {
            values[i] = i;
        }
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, WriterConfig.builder().build())) {
            writer.columnWriter().writeBatch(batch -> batch.longs("v", values));
        }
        byte[] bytes = InMemoryFiles.toByteArray(out);
        return new NamedInputFile(InputFile.of(ByteBuffer.wrap(bytes)), "single_chunk.parquet");
    }

    /// A read of `length` bytes at `offset`.
    record Range(long offset, int length) {
    }

    /// Fails the first `failures` reads of a column's pages with `failure`, thrown whether the
    /// method declares it or not, recording the ranges it failed. Not a file whose bytes are in
    /// memory, so every read of a column's pages goes to a virtual thread.
    private static final class FailingColumnReads implements InputFile {

        private final InputFile delegate;
        private final int failures;
        private final Throwable failure;
        final List<Range> failed = new CopyOnWriteArrayList<>();
        final List<Range> reads = new CopyOnWriteArrayList<>();

        FailingColumnReads(InputFile delegate, int failures, Throwable failure) {
            this.delegate = delegate;
            this.failures = failures;
            this.failure = failure;
        }

        @Override
        public void open() throws IOException {
            delegate.open();
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            Range range = new Range(offset, length);
            if (FetchReason.current().contains(" col=")) {
                synchronized (this) {
                    if (failed.size() < failures) {
                        failed.add(range);
                        throw FailingColumnReads.<RuntimeException>sneaky(failure);
                    }
                }
                reads.add(range);
            }
            return delegate.readRange(offset, length);
        }

        @SuppressWarnings("unchecked")
        private static <T extends Throwable> T sneaky(Throwable e) throws T {
            throw (T) e;
        }

        @Override
        public long length() throws IOException {
            return delegate.length();
        }

        @Override
        public String name() {
            return delegate.name();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    /// `delegate` under `name`, not a file whose bytes are in memory.
    private record NamedInputFile(InputFile delegate, String name) implements InputFile {

        @Override
        public void open() throws IOException {
            delegate.open();
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            return delegate.readRange(offset, length);
        }

        @Override
        public long length() throws IOException {
            return delegate.length();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    /// Holds every read of a column's pages until released, and signals the first.
    private static final class HeldInputFile implements InputFile {

        private final InputFile delegate;
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);

        HeldInputFile(InputFile delegate) {
            this.delegate = delegate;
        }

        @Override
        public void open() throws IOException {
            delegate.open();
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            String reason = FetchReason.current();
            if (reason.contains(" col=")) {
                entered.countDown();
                try {
                    release.await();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted", e);
                }
            }
            return delegate.readRange(offset, length);
        }

        @Override
        public long length() throws IOException {
            return delegate.length();
        }

        @Override
        public String name() {
            return delegate.name();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    /// Records the thread and the [FetchReason] of every read. Not a file whose bytes are in
    /// memory, so every read of a running scan goes to a virtual thread.
    private static final class ThreadRecordingInputFile implements InputFile {

        record Read(String reason, boolean virtual) {
        }

        private final InputFile delegate;
        private final List<Read> reads = new CopyOnWriteArrayList<>();

        ThreadRecordingInputFile(InputFile delegate) {
            this.delegate = delegate;
        }

        @Override
        public void open() throws IOException {
            delegate.open();
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            reads.add(new Read(FetchReason.current(), Thread.currentThread().isVirtual()));
            return delegate.readRange(offset, length);
        }

        @Override
        public long length() throws IOException {
            return delegate.length();
        }

        @Override
        public String name() {
            return delegate.name();
        }

        @Override
        public Optional<String> identity() throws IOException {
            return delegate.identity();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
