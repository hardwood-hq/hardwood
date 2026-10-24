/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.fetch;

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

import dev.hardwood.InputFile;
import dev.hardwood.internal.FetchReason;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.io.CountingInputFile;
import dev.hardwood.internal.reader.decode.PageInfo;
import dev.hardwood.internal.reader.footer.ParquetMetadataReader;
import dev.hardwood.internal.reader.memory.ReadAheadBuffer;
import dev.hardwood.internal.reader.step.Need;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A column's fetch step never waits for the bytes of a page: the read of a chunk or region whose
/// bytes are not in memory runs on a virtual thread, the step yields on it, and its completion
/// queues the step again. A walk that stops on a read resumes where it stopped, reading no byte
/// twice.
class FetchStepYieldTest {

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
        assertThat(RangeRead.inMemory(InputFile.of(INDEXED_FILE))).isTrue();
        assertThat(RangeRead.inMemory(InputFile.of(ByteBuffer.allocate(1)))).isTrue();
        assertThat(RangeRead.inMemory(new ThreadRecordingInputFile(InputFile.of(INDEXED_FILE)))).isFalse();
    }

    /// Walks a column whose dictionary page and data pages straddle 300-byte windows, stopping
    /// wherever bytes are not present: the read issued for each stop is demand, and the walk yields
    /// the pages a walk over the file in memory yields, reading every byte once.
    @Test
    void walkResumesWhereItStoppedAndReadsEachByteOnce() throws Exception {
        System.setProperty(CoalescingPolicy.MAX_SPAN_BYTES_PROPERTY, "300");
        InputFile local = InputFile.of(DICTIONARY_FILE);
        local.open();
        CountingInputFile remote = new CountingInputFile(InputFile.of(DICTIONARY_FILE));
        remote.open();
        try (HardwoodContextImpl context = HardwoodContextImpl.create(1)) {
            FileMetaData meta = ParquetMetadataReader.readMetadata(local);
            FileSchema schema = FileSchema.fromSchemaElements(meta.schema());
            ColumnSchema column = schema.getColumn("label");
            RowGroup rowGroup = meta.rowGroups().get(0);

            List<Integer> expected = new ArrayList<>();
            PageWalks.Pages inMemory = PageWalks.of(SequentialFetchPlan.build(local, column,
                    rowGroup.columns().get(column.columnIndex()), context, 0, local.name(), 0));
            while (inMemory.hasNext()) {
                expected.add(inMemory.next().pageData().remaining());
            }

            PageWalk walk = SequentialFetchPlan.build(remote, column,
                    rowGroup.columns().get(column.columnIndex()), context, 0, remote.name(), 0).walk();
            List<Integer> walked = new ArrayList<>();
            int stops = 0;
            while (true) {
                PageInfo page = walk.advance();
                if (page != null) {
                    walked.add(page.pageData().remaining());
                    continue;
                }
                Need pending = walk.pending();
                if (pending == null) {
                    break;
                }
                stops++;
                // A walk outside a read has no read-ahead buffer to stop on
                ((ByteNeed) pending).issue().await();
            }

            assertThat(walked).isEqualTo(expected);
            assertThat(stops).as("stops on a read under way").isPositive();
            assertThat(remote.reads()).extracting(CountingInputFile.Read::offset).doesNotHaveDuplicates();
        }
        finally {
            local.close();
            remote.close();
        }
    }

    /// A walk issues no read: polling an entry of a file not in memory starts nothing, and the read
    /// the step issues joins a speculative read under way rather than starting another.
    @Test
    void pollIssuesNoReadAndIssueJoinsASpeculativeRead() throws Exception {
        CountingInputFile file = new CountingInputFile(ByteBuffer.allocate(100));
        PrefetchTasks tasks = new PrefetchTasks(Runnable::run);
        FetchSequence.Entry entry = entry(sequence(tasks, false), file, 0);

        assertThat(entry.poll()).isNull();
        assertThat(file.readCount()).isZero();

        entry.issue();
        assertThat(entry.poll()).isNotNull();
        assertThat(file.readCount()).isEqualTo(1);

        FetchSequence.Entry speculative = entry(sequence(tasks, true), file, 10);
        assertThat(file.readCount()).isEqualTo(2);
        RangeRead joined = speculative.issue();
        assertThat(speculative.issue()).isSameAs(joined);
        assertThat(speculative.poll()).isNotNull();
        assertThat(file.readCount()).as("no read issued past the speculative one").isEqualTo(2);
    }

    /// A speculative read that failed leaves the bytes not present, and the read the step issues
    /// for them reads them again.
    @Test
    void aFailedSpeculativeReadIsReadAgainOnDemand() throws Exception {
        FailingInputFile file = new FailingInputFile(1, new IOException("connection reset"));
        FetchSequence.Entry entry = entry(sequence(new PrefetchTasks(Runnable::run), true), file, 0);

        assertThat(entry.poll()).isNull();
        assertThat(fetch(entry).remaining()).isEqualTo(10);
        assertThat(file.reads).isEqualTo(2);
    }

    /// A demand read that fails raises its failure, naming the file, offset and length.
    @Test
    void aFailedDemandReadIsRaised() {
        FailingInputFile file = new FailingInputFile(Integer.MAX_VALUE, new IOException("connection reset"));
        FetchSequence.Entry entry = entry(sequence(new PrefetchTasks(Runnable::run), false), file, 20);

        assertThatThrownBy(() -> fetch(entry))
                .isInstanceOf(IOException.class)
                .hasMessage("[failing] Failed to fetch data at offset 20 (length 10)")
                .cause()
                .hasMessage("connection reset");
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

    /// An `Error` a speculative read fails with is thrown by the close of its tasks, not by the
    /// thread that started it, and the step reads the bytes again on demand.
    @Test
    void anErrorOfASpeculativeReadIsThrownByTheClose() throws Exception {
        AssertionError error = new AssertionError("prefetch broke");
        FailingInputFile file = new FailingInputFile(1, error);
        PrefetchTasks tasks = new PrefetchTasks(Runnable::run);
        FetchSequence.Entry entry = entry(sequence(tasks, true), file, 0);

        assertThat(entry.poll()).isNull();
        assertThat(fetch(entry).remaining()).isEqualTo(10);
        assertThatThrownBy(tasks::awaitAll).isSameAs(error);
    }

    /// An `Error` a demand read fails with reaches the step that takes it, and the close does not
    /// throw it a second time.
    @Test
    void anErrorOfADemandReadReachesTheStepOnly() {
        AssertionError error = new AssertionError("read broke");
        FailingInputFile file = new FailingInputFile(Integer.MAX_VALUE, error);
        PrefetchTasks tasks = new PrefetchTasks(Runnable::run);
        FetchSequence.Entry entry = entry(sequence(tasks, false), file, 0);

        assertThatThrownBy(() -> fetch(entry)).isSameAs(error);
        tasks.awaitAll();
    }

    /// An `Error` a demand read fails with that no step takes, as when the read's column stopped
    /// while it waited, is thrown by the close.
    @Test
    void anErrorNoStepTookIsThrownByTheClose() {
        AssertionError error = new AssertionError("read broke");
        FailingInputFile file = new FailingInputFile(Integer.MAX_VALUE, error);
        PrefetchTasks tasks = new PrefetchTasks(Runnable::run);
        FetchSequence.Entry entry = entry(sequence(tasks, false), file, 0);

        entry.issue();
        assertThatThrownBy(tasks::awaitAll).isSameAs(error);
    }

    /// A checked exception an `InputFile` throws undeclared completes the read with it, so the step
    /// waiting for the read resumes and fails instead of waiting forever.
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aCheckedExceptionThrownUndeclaredCompletesTheRead() {
        FailingInputFile file = new FailingInputFile(Integer.MAX_VALUE, new TimeoutException("too slow"));
        FetchSequence.Entry entry = entry(sequence(new PrefetchTasks(task -> Thread.ofVirtual().start(task)), false),
                file, 0);

        assertThatThrownBy(() -> fetch(entry))
                .isInstanceOf(IOException.class)
                .hasMessage("[failing] Failed to fetch data at offset 0 (length 10)")
                .cause()
                .isInstanceOf(TimeoutException.class)
                .hasMessage("too slow");
    }

    /// A sequence of one read's entries on `tasks`, with room for any entry, whose entries start
    /// speculatively when `speculates`, and on demand only otherwise.
    private static FetchSequence sequence(PrefetchTasks tasks, boolean speculates) {
        return new FetchSequence(tasks, new ReadAheadBuffer(1 << 20), 1 << 20, speculates, () -> {});
    }

    /// The entry of ten bytes at `offset` of `file` in `sequence`, held by one column.
    private static FetchSequence.Entry entry(FetchSequence sequence, InputFile file, long offset) {
        return sequence.insert(0, file, List.of(new FetchSequence.Span(offset, 10, "chunk", 1))).getFirst();
    }

    /// `entry`'s bytes, demanding them and waiting for them as a thread off the pool does.
    private static ByteBuffer fetch(FetchSequence.Entry entry) throws IOException {
        while (true) {
            ByteBuffer bytes = entry.poll();
            if (bytes != null) {
                return bytes;
            }
            entry.issue().await();
        }
    }

    /// While a column's read is held, its step holds no thread of the pool: the pool's one thread
    /// runs other work. A close waits for the held read, and returns once it completes.
    @Tag("pool")
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aStepWaitingForAReadHoldsNoThreadAndItsCloseWaitsForTheRead() throws Exception {
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

    /// A step whose read completes after its context was closed fails the read with the context's
    /// closing rather than waiting for a pool that refuses it.
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

    /// Fails its first `failures` reads with `failure`, thrown whether the method declares it or
    /// not, and serves ten zero bytes after that.
    private static final class FailingInputFile implements InputFile {

        private final int failures;
        private final Throwable failure;
        private int reads;

        FailingInputFile(int failures, Throwable failure) {
            this.failures = failures;
            this.failure = failure;
        }

        @Override
        public void open() {
        }

        @Override
        public synchronized ByteBuffer readRange(long offset, int length) throws IOException {
            reads++;
            if (reads <= failures) {
                throw FailingInputFile.<RuntimeException>sneaky(failure);
            }
            return ByteBuffer.allocate(length);
        }

        @SuppressWarnings("unchecked")
        private static <T extends Throwable> T sneaky(Throwable e) throws T {
            throw (T) e;
        }

        @Override
        public long length() {
            return 100;
        }

        @Override
        public String name() {
            return "failing";
        }

        @Override
        public void close() {
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
