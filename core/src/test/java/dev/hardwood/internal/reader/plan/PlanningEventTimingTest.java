/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.plan;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import dev.hardwood.InMemoryFiles;
import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.internal.FetchReason;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.io.CountingInputFile;
import dev.hardwood.internal.reader.fetch.PrefetchTasks;
import dev.hardwood.internal.reader.step.ReadTasks;
import dev.hardwood.jfr.AbstractJfrRecorderTest;
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

/// The events planning decides are committed when the read reaches the file or row group they
/// describe, however far ahead planning ran, and a file's open is reported from the start of its
/// footer entry to the parse of its footer.
class PlanningEventTimingTest extends AbstractJfrRecorderTest {

    private static final int FILES = 6;
    private static final int ROWS = 1_000;
    private static final long HOLD_MILLIS = 300;

    /// A read held in its first file plans the files after it ahead, and closes there: only the
    /// first file and the next one, which the read resolves as its next unit, report their pruning.
    @Tag("pool")
    @Test
    void aFilePlannedAheadThatTheReadNeverReachesReportsNothing() throws Exception {
        enable("dev.hardwood.RowGroupFilter");
        CountDownLatch release = new CountDownLatch(1);
        List<CountingInputFile> files = new ArrayList<>();
        for (int i = 0; i < FILES; i++) {
            files.add(new CountingInputFile(new NamedFile("f" + i, write(i), i == 0 ? release : null)));
        }
        try (HardwoodContextImpl context = HardwoodContextImpl.create(2)) {
            ParquetFileReader reader = ParquetFileReader.openAll(List.copyOf(files), context);
            ColumnReaders columns = reader.buildColumnReaders(ColumnProjection.all())
                    .filter(FilterPredicate.gtEq("id", 0L)).build();
            Thread consumer = Thread.ofPlatform().start(() -> {
                try {
                    while (columns.nextBatch()) {
                        // reads until the close ends the read
                    }
                }
                catch (IOException | RuntimeException e) {
                    // the close ends the read
                }
            });
            for (int i = 0; i < 100 && dataReads(files.getLast()) == 0; i++) {
                Thread.sleep(50);
            }
            assertThat(dataReads(files.getLast())).as("the last file planned ahead").isPositive();

            Thread closer = Thread.ofPlatform().start(() -> {
                try {
                    columns.close();
                    reader.close();
                }
                catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            });
            // Released only once the close has stopped the columns and waits for the held read, so
            // no column moves on to a later file meanwhile
            awaitWaitingForTheRead(closer);
            release.countDown();
            closer.join(TimeUnit.SECONDS.toMillis(30));
            consumer.join(TimeUnit.SECONDS.toMillis(30));
        }
        awaitEvents();

        assertThat(events("dev.hardwood.RowGroupFilter").map(event -> event.getString("file")))
                .containsExactlyInAnyOrder("f0", "f1");
    }

    /// A file whose footer is read ahead reports its open from the start of its footer entry to the
    /// parse of its footer, however long after the read the parse runs.
    @Tag("pool")
    @Test
    void aFileOpenedAheadReportsItsOpenFromItsFooterEntryToItsParse() throws Exception {
        enable("dev.hardwood.FileOpened");
        List<InputFile> files = List.of(new NamedFile("f0", write(0), null), new NamedFile("f1", write(1), null));
        try (HardwoodContextImpl context = HardwoodContextImpl.create(2)) {
            FileMetadataCache cache = new FileMetadataCache(files, context);
            cache.prefetch(1, new PrefetchTasks(context.ioExecutor()), () -> {}).join();
            Thread.sleep(HOLD_MILLIS);
            cache.getFile(1);
            cache.close();
        }
        awaitEvents();

        assertThat(events("dev.hardwood.FileOpened"))
                .singleElement()
                .satisfies(event -> {
                    assertThat(event.getString("file")).isEqualTo("f1");
                    assertThat(event.getDuration().toMillis()).as("from the footer entry's start to the parse")
                            .isGreaterThanOrEqualTo(HOLD_MILLIS);
                });
    }

    /// A footer read ahead that no read parses reports no open.
    @Tag("pool")
    @Test
    void aFooterReadAheadThatIsNeverParsedReportsNoOpen() throws Exception {
        enable("dev.hardwood.FileOpened");
        List<InputFile> files = List.of(new NamedFile("f0", write(0), null), new NamedFile("f1", write(1), null));
        try (HardwoodContextImpl context = HardwoodContextImpl.create(2)) {
            FileMetadataCache cache = new FileMetadataCache(files, context);
            cache.prefetch(1, new PrefetchTasks(context.ioExecutor()), () -> {}).join();
            cache.close();
        }
        awaitEvents();

        assertThat(events("dev.hardwood.FileOpened")).isEmpty();
    }

    /// Waits until `closer` waits for the read's tasks, as a close does once it has stopped the read.
    private static void awaitWaitingForTheRead(Thread closer) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!(LockSupport.getBlocker(closer) instanceof ReadTasks)) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("The close did not wait for the held read: " + closer.getState());
            }
            Thread.sleep(1);
        }
    }

    private static long dataReads(CountingInputFile file) {
        return file.reads().stream().filter(read -> read.reason().contains(" col=")).count();
    }

    /// One row group of [#ROWS] rows of a required `id`, counting on from the files before.
    private static byte[] write(int fileIndex) throws IOException {
        long[] ids = new long[ROWS];
        for (int i = 0; i < ROWS; i++) {
            ids[i] = (long) fileIndex * ROWS + i;
        }
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT64, RepetitionType.REQUIRED)
                .build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, WriterConfig.builder().build())) {
            writer.columnWriter().writeBatch(batch -> batch.longs("id", ids));
        }
        return InMemoryFiles.toByteArray(out);
    }

    /// A named file of bytes the reader treats as remote; with a latch, its data reads wait for it.
    private static final class NamedFile implements InputFile {

        private final String name;
        private final InputFile delegate;
        private final CountDownLatch release;

        NamedFile(String name, byte[] bytes, CountDownLatch release) {
            this.name = name;
            this.delegate = InputFile.of(ByteBuffer.wrap(bytes));
            this.release = release;
        }

        @Override
        public void open() throws IOException {
            delegate.open();
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            if (release != null && FetchReason.current().contains(" col=")) {
                try {
                    release.await();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException(e);
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
            return name;
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
