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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import dev.hardwood.InMemoryFiles;
import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.internal.FetchReason;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;

/// A multi-file read over files whose bytes are not in memory opens up to
/// `hardwood.internal.maxFilesAhead` files ahead of the one it waits for: their footers are in
/// flight together, a file statistics keep has its first data fetched ahead in one request, a
/// run of files statistics prune is opened ahead rather than one file after another, and what is
/// fetched beyond the next file stays within the context's read-ahead buffer (#1665).
class FileOpenAheadTest {

    private static final int ROWS = 4_000;
    private static final String MAX_FILES_AHEAD = "hardwood.internal.maxFilesAhead";
    private static final String READ_AHEAD_BYTES = "hardwood.internal.readAheadBytes";

    @AfterEach
    void clearProperties() {
        System.clearProperty(MAX_FILES_AHEAD);
        System.clearProperty(READ_AHEAD_BYTES);
    }

    @Test
    void footersOfFilesAheadAreInFlightTogether() throws Exception {
        System.setProperty(MAX_FILES_AHEAD, "4");
        ReadGate gate = footerGate();
        List<InputFile> files = gatedFiles(12, gate);
        try (HardwoodContextImpl context = HardwoodContextImpl.create(4);
             ParquetFileReader reader = ParquetFileReader.openAll(files, context)) {
            // Building the readers plans the first file, which opens the files after it
            gate.arm();
            Consumer consumer = Consumer.start(() -> reader.buildColumnReaders(ColumnProjection.all()).build());

            // The file the read waits for, and the four after it
            assertThat(gate.awaitInFlight(5)).as("footer reads in flight at once").isTrue();
            Thread.sleep(200);
            assertThat(gate.maxInFlight()).as("most footer reads in flight at once").isEqualTo(5);

            gate.release();
            assertThat(consumer.rows()).isEqualTo(12L * ROWS);
        }
    }

    /// A file that holds the tail its open fetches (#1399) is charged that tail while its footer is
    /// read ahead, so the buffer bounds how many such opens are in flight.
    @Test
    void aFooterReadAheadIsChargedTheTailItsOpenFetches() throws Exception {
        int tail = 256 * 1024;
        System.setProperty(MAX_FILES_AHEAD, "4");
        // Room for two tails; charged a fixed footer reservation, all four files ahead would fit
        System.setProperty(READ_AHEAD_BYTES, Long.toString(2L * tail));
        ReadGate opens = new ReadGate(reason -> true);
        List<InputFile> files = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            files.add(new GatedOpenTailFile(writeFile(i), "file-" + i, tail, opens));
        }
        try (HardwoodContextImpl context = HardwoodContextImpl.create(4);
             ParquetFileReader reader = ParquetFileReader.openAll(files, context)) {
            opens.arm();
            Consumer consumer = Consumer.start(() -> reader.buildColumnReaders(ColumnProjection.all()).build());

            // The next file, which is not charged, and the two whose tails fit
            assertThat(opens.awaitInFlight(3)).as("opens in flight at once").isTrue();
            Thread.sleep(200);
            assertThat(opens.maxInFlight()).as("most opens in flight at once").isEqualTo(3);

            opens.release();
            assertThat(consumer.rows()).isEqualTo(12L * ROWS);
            assertThat(context.readAheadBuffer().held()).as("read-ahead held once read").isZero();
        }
    }

    @Test
    void aRunOfPrunedFilesIsOpenedAhead() throws Exception {
        System.setProperty(MAX_FILES_AHEAD, "4");
        ReadGate gate = footerGate();
        List<InputFile> files = gatedFiles(12, gate);
        // Only the last file holds ids from 11 * ROWS on
        FilterPredicate lastFileOnly = FilterPredicate.gtEq("id", 11L * ROWS);
        try (HardwoodContextImpl context = HardwoodContextImpl.create(4);
             ParquetFileReader reader = ParquetFileReader.openAll(files, context)) {
            // Building the readers plans as far as the first row group the filter keeps
            gate.arm();
            Consumer consumer = Consumer.start(() -> reader.buildColumnReaders(ColumnProjection.all())
                    .filter(lastFileOnly).build());

            assertThat(gate.awaitInFlight(5)).as("footer reads of pruned files in flight at once").isTrue();
            Thread.sleep(200);
            assertThat(gate.maxInFlight()).as("most footer reads in flight at once").isEqualTo(5);

            gate.release();
            assertThat(consumer.rows()).isEqualTo(ROWS);
        }
    }

    /// On a context of zero threads the held page-index read holds the read's only thread, which
    /// plans the files ahead too.
    @Tag("pool")
    @Test
    void aFileOpenedAheadIsReadInOneDataRequest() throws Exception {
        ReadGate firstIndex = new ReadGate(reason -> reason.endsWith(" indexes"));
        List<CountingInputFile> files = smallFilesHoldingTheFirstIndex(firstIndex);
        try (HardwoodContextImpl context = HardwoodContextImpl.create(4);
             ParquetFileReader reader = ParquetFileReader.openAll(files, context)) {
            assertThat(readWhileOpeningAhead(reader, files, firstIndex)).isEqualTo(8L * ROWS);
            assertThat(context.readAheadBuffer().held()).as("read-ahead held once read").isZero();
        }
        // File 1 is the next file, read as before; every later one was opened ahead
        for (int i = 2; i < files.size(); i++) {
            assertThat(dataReads(files.get(i)))
                    .as("data reads of file %d", i)
                    .singleElement()
                    .satisfies(read -> assertThat(read.reason()).startsWith("open-ahead "));
        }
    }

    /// A second read through the same reader finds every footer loaded by the first, and opens the
    /// files ahead all the same. On a context of zero threads the held page-index read holds the
    /// read's only thread.
    @Tag("pool")
    @Test
    void aSecondReadOfTheSameFilesOpensThemAhead() throws Exception {
        ReadGate firstIndex = new ReadGate(reason -> reason.endsWith(" indexes"));
        List<CountingInputFile> files = smallFilesHoldingTheFirstIndex(firstIndex);
        try (HardwoodContextImpl context = HardwoodContextImpl.create(4);
             ParquetFileReader reader = ParquetFileReader.openAll(files, context)) {
            assertThat(readWhileOpeningAhead(reader, files, firstIndex)).isEqualTo(8L * ROWS);
            assertThat(readWhileOpeningAhead(reader, files, firstIndex)).isEqualTo(8L * ROWS);
            assertThat(context.readAheadBuffer().held()).as("read-ahead held once read").isZero();
        }
        for (int i = 2; i < files.size(); i++) {
            assertThat(dataReads(files.get(i)))
                    .as("data reads of file %d", i)
                    .hasSize(2)
                    .allSatisfy(read -> assertThat(read.reason()).startsWith("open-ahead "));
        }
    }

    /// Eight small files, the first of which holds its page-index read at `firstIndex`.
    private static List<CountingInputFile> smallFilesHoldingTheFirstIndex(ReadGate firstIndex) throws IOException {
        List<CountingInputFile> files = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            InputFile bytes = InputFile.of(ByteBuffer.wrap(writeFile(i)));
            files.add(new CountingInputFile(i == 0 ? new GatedFile(bytes, firstIndex) : new RemoteLike(bytes)));
        }
        return files;
    }

    /// Reads every column of `files` with the read held at its first row group, whose page index
    /// `firstIndex` holds, until every file past the next one has had its data fetched ahead.
    ///
    /// @return the rows read
    private static long readWhileOpeningAhead(ParquetFileReader reader, List<CountingInputFile> files,
                                              ReadGate firstIndex) throws Exception {
        int openedAhead = (int) files.stream().mapToLong(file -> openAheadReads(file)).sum();
        firstIndex.arm();
        Consumer consumer = Consumer.start(() -> reader.buildColumnReaders(ColumnProjection.all()).build());
        for (int i = 0; i < 100 && files.stream().mapToLong(file -> openAheadReads(file)).sum()
                < openedAhead + files.size() - 2; i++) {
            Thread.sleep(50);
        }
        firstIndex.release();
        return consumer.rows();
    }

    private static long openAheadReads(CountingInputFile file) {
        return file.reads().stream().filter(read -> read.reason().startsWith("open-ahead ")).count();
    }

    /// On a context of zero threads the held page-index read holds the read's only thread, which
    /// plans the files ahead too.
    @Tag("pool")
    @Test
    void whatIsFetchedAheadStaysWithinTheReadAheadBuffer() throws Exception {
        // Files of about 1.6 MB; room for about two of them ahead of the next file
        long limit = 4_000_000;
        System.setProperty(READ_AHEAD_BYTES, Long.toString(limit));
        // Holding every page-index read holds the read's own planning of each row group, so the
        // read stops at its first row group as a read whose consumer stops does, and whatever is
        // fetched past it is fetched by opening files ahead
        ReadGate indexes = new ReadGate(reason -> reason.endsWith(" indexes"));
        List<CountingInputFile> files = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            files.add(new CountingInputFile(new GatedFile(InputFile.of(ByteBuffer.wrap(writeLargeFile(i))), indexes)));
        }
        try (HardwoodContextImpl context = HardwoodContextImpl.create(4);
             ParquetFileReader reader = ParquetFileReader.openAll(files, context);
             ColumnReaders cols = reader.buildColumnReaders(ColumnProjection.all()).build()) {
            indexes.arm();
            Consumer consumer = Consumer.start(() -> cols);
            awaitQuiescence(files);

            long fetchedPastNext = 0;
            for (int i = 2; i < files.size(); i++) {
                fetchedPastNext += files.get(i).bytesRead();
            }
            assertThat(files.get(2).reads()).as("reads of file 2")
                    .anySatisfy(read -> assertThat(read.reason()).startsWith("open-ahead "));
            assertThat(fetchedPastNext).as("bytes fetched past the next file").isLessThanOrEqualTo(limit);
            assertThat(dataReads(files.getLast())).as("data reads of the last file").isEmpty();

            indexes.release();
            assertThat(consumer.rows()).isEqualTo(30L * 100_000);
            assertThat(context.readAheadBuffer().held()).as("read-ahead held once read").isZero();
        }
    }

    /// On a context of zero threads the held page-index read holds the read's only thread.
    @Tag("pool")
    @Test
    void closingAReadReleasesWhatItFetchedAhead() throws Exception {
        ReadGate indexes = new ReadGate(reason -> reason.endsWith(" indexes"));
        List<CountingInputFile> files = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            files.add(new CountingInputFile(new GatedFile(InputFile.of(ByteBuffer.wrap(writeLargeFile(i))), indexes)));
        }
        try (HardwoodContextImpl context = HardwoodContextImpl.create(4)) {
            ParquetFileReader reader = ParquetFileReader.openAll(files, context);
            ColumnReaders cols = reader.buildColumnReaders(ColumnProjection.all()).build();
            indexes.arm();
            Consumer consumer = Consumer.start(() -> cols);
            awaitQuiescence(files);
            assertThat(context.readAheadBuffer().held()).as("read-ahead held while the read waits").isPositive();

            // The close waits for the held planning, which the release lets end
            Thread closer = Thread.ofPlatform().name("closer").start(() -> {
                try {
                    cols.close();
                    reader.close();
                }
                catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            });
            indexes.release();
            closer.join(TimeUnit.SECONDS.toMillis(30));
            assertThat(closer.isAlive()).as("close still running").isFalse();
            consumer.thread().join(TimeUnit.SECONDS.toMillis(30));
            assertThat(context.readAheadBuffer().held()).as("read-ahead held once closed").isZero();
        }
    }

    /// The reads of `file` inside its row group's column chunks: all but its footer reads, whose
    /// reasons start with `footer-`, and its page-index reads, whose reasons end in `indexes`.
    private static List<CountingInputFile.Read> dataReads(CountingInputFile file) {
        return file.reads().stream()
                .filter(read -> !read.reason().startsWith("footer-") && !read.reason().endsWith(" indexes"))
                .toList();
    }

    /// Waits until no file has been read for half a second.
    private static void awaitQuiescence(List<CountingInputFile> files) throws InterruptedException {
        long last = -1;
        for (int i = 0; i < 50; i++) {
            long reads = files.stream().mapToLong(CountingInputFile::readCount).sum();
            if (reads == last) {
                return;
            }
            last = reads;
            Thread.sleep(500);
        }
        throw new AssertionError("Reads did not stop");
    }

    private static ReadGate footerGate() {
        return new ReadGate(reason -> reason.startsWith("footer-"));
    }

    private static List<InputFile> gatedFiles(int count, ReadGate gate) throws IOException {
        List<InputFile> files = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            files.add(new GatedFile(InputFile.of(ByteBuffer.wrap(writeFile(i))), gate));
        }
        return files;
    }

    /// One row group of [#ROWS] rows: `id` counts on from the rows of the files before, so a range
    /// predicate on it prunes whole files, and `v` holds values that do not repeat.
    private static byte[] writeFile(int fileIndex) throws IOException {
        long[] ids = new long[ROWS];
        double[] values = new double[ROWS];
        for (int i = 0; i < ROWS; i++) {
            ids[i] = (long) fileIndex * ROWS + i;
            values[i] = ids[i] * 1.5 + 0.25;
        }
        return write(ids, values, WriterConfig.builder().build());
    }

    /// One row group of 100,000 rows in pages of 1,000, whose values do not compress: about 1.6 MB.
    private static byte[] writeLargeFile(int fileIndex) throws IOException {
        int rows = 100_000;
        Random random = new Random(fileIndex);
        long[] ids = new long[rows];
        double[] values = new double[rows];
        for (int i = 0; i < rows; i++) {
            ids[i] = random.nextLong();
            values[i] = random.nextDouble();
        }
        return write(ids, values, WriterConfig.builder().pageTargetRows(1_000).build());
    }

    private static byte[] write(long[] ids, double[] values, WriterConfig config) throws IOException {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT64, RepetitionType.REQUIRED)
                .addColumn("v", PhysicalType.DOUBLE, RepetitionType.REQUIRED)
                .build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, config)) {
            writer.columnWriter().writeBatch(batch -> batch.longs("id", ids).doubles("v", values));
        }
        return InMemoryFiles.toByteArray(out);
    }

    /// Reads every batch on a thread of its own and counts the rows.
    private record Consumer(Thread thread, AtomicLong rowCount, AtomicInteger failures) {

        /// Builds the readers with `build` on the consumer's thread, then reads them.
        static Consumer start(Build build) {
            AtomicLong rows = new AtomicLong();
            AtomicInteger failures = new AtomicInteger();
            Thread thread = Thread.ofPlatform().name("consumer").start(() -> {
                try (ColumnReaders cols = build.build()) {
                    while (cols.nextBatch()) {
                        rows.addAndGet(cols.getRecordCount());
                    }
                }
                catch (Throwable e) {
                    failures.incrementAndGet();
                    throw new IllegalStateException(e);
                }
            });
            return new Consumer(thread, rows, failures);
        }

        long rows() throws InterruptedException {
            thread.join(TimeUnit.SECONDS.toMillis(30));
            assertThat(thread.isAlive()).as("consumer still reading").isFalse();
            assertThat(failures.get()).as("consumer failures").isZero();
            return rowCount.get();
        }
    }

    @FunctionalInterface
    private interface Build {
        ColumnReaders build() throws IOException;
    }

    /// Holds back every read whose [FetchReason] it holds once armed until released, and counts how
    /// many are in flight.
    private static final class ReadGate {

        private final Predicate<String> holds;
        private volatile CountDownLatch released = new CountDownLatch(1);
        private final AtomicInteger inFlight = new AtomicInteger();
        private final AtomicInteger maxInFlight = new AtomicInteger();
        private volatile boolean armed;

        ReadGate(Predicate<String> holds) {
            this.holds = holds;
        }

        /// Holds the reads from now on, until the next [#release].
        void arm() {
            released = new CountDownLatch(1);
            armed = true;
        }

        void release() {
            released.countDown();
        }

        int maxInFlight() {
            return maxInFlight.get();
        }

        /// Waits at most five seconds for `count` held reads to be in flight at once.
        boolean awaitInFlight(int count) throws InterruptedException {
            for (int i = 0; i < 100; i++) {
                if (inFlight.get() >= count) {
                    return true;
                }
                Thread.sleep(50);
            }
            return false;
        }

        void pass() throws IOException {
            if (!armed || !holds.test(FetchReason.current())) {
                return;
            }
            CountDownLatch latch = released;
            maxInFlight.accumulateAndGet(inFlight.incrementAndGet(), Math::max);
            try {
                if (!latch.await(30, TimeUnit.SECONDS)) {
                    throw new IOException("Read gate never released");
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
            finally {
                inFlight.decrementAndGet();
            }
        }
    }

    /// A file of bytes in memory that the reader treats as remote, so that it reads ahead.
    private static class RemoteLike implements InputFile {

        private final InputFile delegate;

        RemoteLike(InputFile delegate) {
            this.delegate = delegate;
        }

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
        public String name() {
            return delegate.name();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    /// A [TailFile] whose open passes through a [ReadGate].
    private static final class GatedOpenTailFile extends TailFile {

        private final ReadGate gate;

        GatedOpenTailFile(byte[] bytes, String name, int tailSize, ReadGate gate) {
            super(bytes, name, tailSize, tailSize);
            this.gate = gate;
        }

        @Override
        public void open(boolean footerSupplied) throws IOException {
            gate.pass();
            super.open(footerSupplied);
        }
    }

    /// A remote-like file whose reads pass through a [ReadGate].
    private static final class GatedFile extends RemoteLike {

        private final ReadGate gate;

        GatedFile(InputFile delegate, ReadGate gate) {
            super(delegate);
            this.gate = gate;
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            gate.pass();
            return super.readRange(offset, length);
        }
    }
}
