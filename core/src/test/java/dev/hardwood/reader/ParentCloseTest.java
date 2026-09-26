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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import dev.hardwood.InputFile;
import dev.hardwood.internal.reader.ColumnWorker;
import dev.hardwood.internal.reader.PrefetchTasks;
import dev.hardwood.internal.writer.ByteBufferOutputFile;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// [ParquetFileReader#close()] closes the child readers the caller left open, and waits
/// for the prefetches their reads started, before it closes the input files: no read
/// is in flight when a file closes, and none starts afterwards.
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
    /// A small fraction of a page, so a column reader's retriever stays in the first row group.
    private static final int INDEXED_BATCH_SIZE = 16;

    /// Where a close stops a child reader's column workers.
    private static final String WORKER_CLOSE = ColumnWorker.class.getName() + "#close";
    /// Where a close waits for the speculative tasks a read has started.
    private static final String AWAIT_PREFETCHES = PrefetchTasks.class.getName() + "#awaitAll";
    /// Where a close waits for the child builds already under way.
    private static final String AWAIT_BUILDS = ReentrantReadWriteLock.WriteLock.class.getName() + "#lock";

    /// Serves a file's bytes. Once armed, holds the first read made on a thread the
    /// arming predicate accepts until [#release()] or [#close()]. Records a read that is
    /// in flight when the file closes, and one that starts after it closed.
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

    @Test
    void closeStopsAnUnclosedRowReaderBeforeClosingTheFile() throws Exception {
        GatedInputFile file = new GatedInputFile(fixedWidthFile());
        ParquetFileReader reader = ParquetFileReader.open(file);
        RowReader rows = reader.rowReader();
        Thread testThread = Thread.currentThread();
        Thread consumer = Thread.ofPlatform().unstarted(() -> drain(rows));
        file.arm(t -> t != testThread && t != consumer);
        consumer.start();
        file.awaitHeldRead();

        closeWhileHeld(reader, file, WORKER_CLOSE, AWAIT_PREFETCHES);
        consumer.join();

        assertThat(file.violations).isEmpty();
        rows.close();
    }

    @Test
    void closeStopsUnclosedColumnReadersBeforeClosingTheFile() throws Exception {
        GatedInputFile file = new GatedInputFile(Files.readAllBytes(INDEXED_FILE));
        ParquetFileReader reader = ParquetFileReader.open(file);
        ColumnReaders columns = reader.buildColumnReaders(ColumnProjection.all())
                .batchSize(INDEXED_BATCH_SIZE)
                .build();
        Thread testThread = Thread.currentThread();
        Thread consumer = Thread.ofPlatform().unstarted(() -> drain(columns));
        file.arm(t -> t != testThread && t != consumer);
        consumer.start();
        file.awaitHeldRead();

        closeWhileHeld(reader, file, WORKER_CLOSE, AWAIT_PREFETCHES);
        consumer.join();

        assertThat(file.violations).isEmpty();
        columns.close();
    }

    /// Nothing consumes, so the column's retriever stays in the first row group; the read
    /// held is one a speculative task on the common pool started, planning or prefetching
    /// the second row group.
    @Test
    void closeWaitsForAStartedPrefetchBeforeClosingTheFile() throws Exception {
        GatedInputFile file = new GatedInputFile(Files.readAllBytes(INDEXED_FILE));
        ParquetFileReader reader = ParquetFileReader.open(file);
        Thread testThread = Thread.currentThread();
        file.arm(t -> !t.isVirtual() && t != testThread);
        ColumnReaders columns = reader.buildColumnReaders(ColumnProjection.all())
                .batchSize(INDEXED_BATCH_SIZE)
                .build();
        file.awaitHeldRead();

        closeWhileHeld(reader, file, AWAIT_PREFETCHES);

        assertThat(file.violations).isEmpty();
        columns.close();
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

    /// A tail read probes the row groups' offset indexes on the building thread; the test
    /// holds that read while the parent closes on another thread. The close waits for the
    /// build before closing the file; the build, finding the parent closed, closes the
    /// child it made and fails.
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
        file.arm(t -> t == builder);
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
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, config)) {
            writer.columnWriter().writeBatch(batch -> batch.fixed("v", values));
        }
        return out.toByteArray();
    }
}
