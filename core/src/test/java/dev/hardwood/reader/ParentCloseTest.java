/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import dev.hardwood.HardwoodContext;
import dev.hardwood.InMemoryFiles;
import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.internal.reader.step.ReadTasks;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// [ParquetFileReader#close()] closes the child readers the caller left open, and waits
/// for the reads they started, before it closes the input files: no read is in flight when
/// a file closes, and none starts afterwards.
///
/// Each test holds one read inside the [InputFile] and closes the parent on another
/// thread. The read is released once that thread waits where the close must wait for it;
/// a close that does not wait there closes the file with the read still held, or blocks
/// behind it until the timeout.
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class ParentCloseTest {

    /// Wide enough that a row reader's batch holds 1,536 rows, so a batch is a small
    /// fraction of the file and the row reader's pipeline stops well short of its end.
    private static final int FIXED_WIDTH = 4096;
    private static final int FIXED_ROWS = 40_000;
    private static final int FIXED_ROW_GROUP_ROWS = 2_000;

    /// Three row groups of about 60 pages each, with an offset index, so planning a row
    /// group reads from the file.
    private static final Path INDEXED_FILE = Paths.get("src/test/resources/prefetch_page_index.parquet");
    /// A small fraction of a page, so a column reader's read stays in the first row group.
    private static final int INDEXED_BATCH_SIZE = 16;

    /// Where a close waits for the jobs of a column or row read in flight, its reads included.
    private static final String READ_CLOSE = ReadTasks.class.getName() + "#await";
    /// Where a close waits for the child builds already under way.
    private static final String AWAIT_BUILDS = ReentrantReadWriteLock.WriteLock.class.getName() + "#lock";

    /// Serves a file's bytes. Once armed, holds the first read made on a thread the
    /// arming predicate accepts until [#release()] or [#close()]. Records a read that is in
    /// flight when the file closes, and one that starts after it closed.
    private static final class GatedInputFile implements InputFile {

        private final ByteBuffer data;
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch released = new CountDownLatch(1);
        private final AtomicReference<Predicate<Thread>> gate = new AtomicReference<>();
        private final AtomicInteger readsInFlight = new AtomicInteger();
        private final List<String> violations = new CopyOnWriteArrayList<>();
        private volatile boolean closed;

        GatedInputFile(byte[] bytes) {
            this.data = ByteBuffer.wrap(bytes);
        }

        @Override
        public void open() {
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            if (closed) {
                violations.add("read at offset " + offset + " after close");
                throw new IOException("gated.parquet is closed");
            }
            readsInFlight.incrementAndGet();
            try {
                Predicate<Thread> held = gate.get();
                if (held != null && held.test(Thread.currentThread()) && gate.compareAndSet(held, null)) {
                    entered.countDown();
                    released.await();
                }
                return data.slice(Math.toIntExact(offset), length);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new InterruptedIOException("interrupted while held");
            }
            finally {
                readsInFlight.decrementAndGet();
            }
        }

        @Override
        public long length() {
            return data.capacity();
        }

        @Override
        public String name() {
            return "gated.parquet";
        }

        @Override
        public void close() {
            if (readsInFlight.get() > 0) {
                violations.add("closed with a read in flight");
            }
            closed = true;
            released.countDown();
        }

        /// Holds the next read made on a thread `held` accepts.
        void arm(Predicate<Thread> held) {
            gate.set(held);
        }

        void awaitHeldRead() throws InterruptedException {
            entered.await();
        }

        void release() {
            released.countDown();
        }
    }

    /// Building a row reader takes its first batch, so the gate is armed afterwards; with no room
    /// for speculative data (F of zero), the read has fetched no further than its next unit by then,
    /// and the consumer's read of a later row group is the one held.
    @Test
    void closeStopsAnUnclosedRowReaderBeforeClosingTheFile() throws Exception {
        System.setProperty("hardwood.internal.fetchAheadBytes", "0");
        try {
            closeAnUnclosedRowReaderWhileARowGroupIsHeld();
        }
        finally {
            System.clearProperty("hardwood.internal.fetchAheadBytes");
        }
    }

    private void closeAnUnclosedRowReaderWhileARowGroupIsHeld() throws Exception {
        GatedInputFile file = new GatedInputFile(fixedWidthFile());
        ParquetFileReader reader = ParquetFileReader.open(file);
        RowReader rows = reader.rowReader();
        Thread testThread = Thread.currentThread();
        Thread consumer = Thread.ofPlatform().unstarted(() -> drain(rows));
        file.arm(t -> t != testThread && t != consumer);
        consumer.start();
        file.awaitHeldRead();

        closeWhileHeld(reader, file, READ_CLOSE);
        consumer.join();

        assertThat(file.violations).isEmpty();
        rows.close();
    }

    /// The gate is armed before the readers are built: their reads start as they are built, and
    /// armed afterwards, the gate could miss every read the read makes. Building
    /// column readers waits for none of those reads, so the held one does not hold the build.
    @Test
    void closeStopsUnclosedColumnReadersBeforeClosingTheFile() throws Exception {
        GatedInputFile file = new GatedInputFile(Files.readAllBytes(INDEXED_FILE));
        ParquetFileReader reader = ParquetFileReader.open(file);
        AtomicReference<ColumnReaders> built = new AtomicReference<>();
        Thread testThread = Thread.currentThread();
        Thread consumer = Thread.ofPlatform().unstarted(() -> drain(built.get()));
        file.arm(t -> t != testThread && t != consumer);
        ColumnReaders columns = reader.buildColumnReaders(ColumnProjection.all())
                .batchSize(INDEXED_BATCH_SIZE)
                .build();
        built.set(columns);
        consumer.start();
        file.awaitHeldRead();

        closeWhileHeld(reader, file, READ_CLOSE);
        consumer.join();

        assertThat(file.violations).isEmpty();
        columns.close();
    }

    /// The read held is the first the read makes on the context's I/O executor, where every read of
    /// a file not in memory runs, while nothing consumes. The close waits for it before it closes
    /// the file.
    @Tag("pool")
    @Test
    void closeWaitsForAReadOnTheIoExecutorBeforeClosingTheFile() throws Exception {
        GatedInputFile file = new GatedInputFile(Files.readAllBytes(INDEXED_FILE));
        try (HardwoodContext context = HardwoodContext.create(2)) {
            ParquetFileReader reader = ParquetFileReader.open(file, context);
            file.arm(t -> t.getName().equals("hardwood-io"));
            ColumnReaders columns = reader.buildColumnReaders(ColumnProjection.all())
                    .batchSize(INDEXED_BATCH_SIZE)
                    .build();
            file.awaitHeldRead();

            closeWhileHeld(reader, file, READ_CLOSE);

            assertThat(file.violations).isEmpty();
            columns.close();
        }
    }

    @Test
    void buildingAReaderAfterCloseFails() throws Exception {
        GatedInputFile file = new GatedInputFile(Files.readAllBytes(INDEXED_FILE));
        ParquetFileReader reader = ParquetFileReader.open(file);
        reader.close();

        assertThatThrownBy(reader::rowReader)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("ParquetFileReader is closed");
        assertThatThrownBy(() -> reader.columnReaders(ColumnProjection.all()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("ParquetFileReader is closed");
        assertThatThrownBy(() -> reader.columnReader("id"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("ParquetFileReader is closed");
        assertThatThrownBy(() -> reader.buildRowReader().tail(10).build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("ParquetFileReader is closed");
        assertThat(file.violations).isEmpty();
    }

    /// A tail read probes the row groups' offset indexes, which the building thread waits for; the
    /// test holds that read while the parent closes on another thread. The close waits for the
    /// build before closing the file; the build, finding the parent closed, closes the child it
    /// made and fails.
    @Test
    void closeDuringABuildClosesTheChildAndFailsTheBuild() throws Exception {
        GatedInputFile file = new GatedInputFile(Files.readAllBytes(INDEXED_FILE));
        ParquetFileReader reader = ParquetFileReader.open(file);
        AtomicReference<Throwable> buildFailure = new AtomicReference<>();
        Thread builder = Thread.ofPlatform().unstarted(() -> {
            try (RowReader rows = reader.buildRowReader().tail(10).build()) {
                buildFailure.set(new AssertionError("build returned " + rows));
            }
            catch (Throwable t) {
                buildFailure.set(t);
            }
        });
        // Every read from here on is the build's: its planning reads them as entries, off the
        // building thread
        file.arm(t -> true);
        builder.start();
        file.awaitHeldRead();

        closeWhileHeld(reader, file, AWAIT_BUILDS);
        builder.join();

        assertThat(buildFailure.get())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("ParquetFileReader is closed");
        assertThat(file.violations).isEmpty();
    }

    /// A child whose close fails stops neither the other children nor the files from
    /// closing, and its failure is what the parent's close throws.
    @Test
    void closeFinishesTeardownWhenAChildFailsToClose() throws Exception {
        GatedInputFile file = new GatedInputFile(fixedWidthFile());
        ParquetFileReader reader = ParquetFileReader.open(file);
        RowReader first = reader.rowReader();
        RowReader second = reader.rowReader();
        AtomicInteger failing = new AtomicInteger();
        AtomicInteger closedCleanly = new AtomicInteger();
        reader.wrapTrackedChildren(child -> failing.getAndIncrement() == 0
                ? () -> {
                    child.close();
                    throw new IllegalStateException("child close failed");
                }
                : () -> {
                    child.close();
                    closedCleanly.incrementAndGet();
                });

        assertThatThrownBy(reader::close)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("child close failed");

        assertThat(closedCleanly).hasValue(1);
        assertThat(file.closed).isTrue();
        assertThat(file.violations).isEmpty();
        first.close();
        second.close();
    }

    /// A child whose close throws an `Error`, as one whose speculative work raised it does, stops
    /// neither the other children nor the files from closing, and the parent's close throws it.
    @Test
    void closeFinishesTeardownWhenAChildsCloseThrowsAnError() throws Exception {
        GatedInputFile file = new GatedInputFile(fixedWidthFile());
        ParquetFileReader reader = ParquetFileReader.open(file);
        RowReader first = reader.rowReader();
        RowReader second = reader.rowReader();
        AtomicInteger failing = new AtomicInteger();
        AtomicInteger closedCleanly = new AtomicInteger();
        AssertionError error = new AssertionError("prefetch failed");
        reader.wrapTrackedChildren(child -> failing.getAndIncrement() == 0
                ? () -> {
                    child.close();
                    throw error;
                }
                : () -> {
                    child.close();
                    closedCleanly.incrementAndGet();
                });

        assertThatThrownBy(reader::close)
                .isSameAs(error)
                .hasMessage("prefetch failed");

        assertThat(closedCleanly).hasValue(1);
        assertThat(file.closed).isTrue();
        assertThat(file.violations).isEmpty();
        first.close();
        second.close();
    }

    /// Closes `reader` on another thread while `file` holds a read, and releases the read
    /// once that thread is in one of `waitPoints` or has finished closing.
    private static void closeWhileHeld(ParquetFileReader reader, GatedInputFile file, String... waitPoints)
            throws Exception {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread closer = Thread.ofPlatform().start(() -> {
            try {
                reader.close();
            }
            catch (Throwable t) {
                failure.set(t);
            }
        });
        try {
            while (closer.isAlive() && !isInAny(closer, waitPoints)) {
                if (Thread.interrupted()) {
                    throw new InterruptedException("close did not reach " + String.join(" or ", waitPoints));
                }
                Thread.onSpinWait();
            }
        }
        finally {
            file.release();
        }
        closer.join();
        assertThat(failure.get()).isNull();
    }

    private static boolean isInAny(Thread thread, String... waitPoints) {
        for (StackTraceElement frame : thread.getStackTrace()) {
            String point = frame.getClassName() + "#" + frame.getMethodName();
            for (String waitPoint : waitPoints) {
                if (waitPoint.equals(point)) {
                    return true;
                }
            }
        }
        return false;
    }

    /// Reads until the reader ends or fails. Either is expected once the parent closes, so
    /// the outcome is not asserted; what the reads did to the file is.
    private static void drain(RowReader rows) {
        try {
            while (rows.hasNext()) {
                rows.next();
            }
        }
        catch (Exception ignored) {
            // ended by the parent's close
        }
    }

    private static void drain(ColumnReaders columns) {
        try {
            while (columns.nextBatch()) {
                // drain
            }
        }
        catch (Exception ignored) {
            // ended by the parent's close
        }
    }

    /// One `FIXED_LEN_BYTE_ARRAY` column whose rows all share one value, so the file is
    /// small while a row reader sizes its batches by the column's width.
    private static byte[] fixedWidthFile() throws IOException {
        FileSchema schema = FileSchema.builder("parent_close_fixed")
                .addColumn("v", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED, FIXED_WIDTH)
                .build();
        byte[][] values = new byte[FIXED_ROWS][];
        Arrays.fill(values, new byte[FIXED_WIDTH]);
        WriterConfig config = WriterConfig.builder()
                .rowGroupTargetRows(FIXED_ROW_GROUP_ROWS)
                .build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, config)) {
            writer.columnWriter().writeBatch(batch -> batch.fixed("v", values));
        }
        return InMemoryFiles.toByteArray(out);
    }
}
