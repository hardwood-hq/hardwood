/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import dev.hardwood.HardwoodContext;
import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.Validity;
import dev.hardwood.internal.compression.Compressor;
import dev.hardwood.internal.reader.HardwoodContextImpl;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/// The column chunks of a row group are appended to and encoded on a [HardwoodContext]'s pool and
/// the calling thread together, and the file does not depend on which thread did what.
@Tag("pool")
class WriterParallelEncodingTest {

    private static final Duration NO_HANG = Duration.ofSeconds(60);
    private static final int RECORDS = 20_000;
    private static final int BATCH = 1_000;

    /// `UNCOMPRESSED` hands the sink a page body the next page overwrites, which a chunk buffered
    /// ahead of its turn has to copy; `ZSTD` hands it a fresh array per page.
    @ParameterizedTest
    @EnumSource(value = CompressionCodec.class, names = { "ZSTD", "UNCOMPRESSED" })
    void writesTheSameBytesWhateverThePoolDoes(CompressionCodec codec) throws Exception {
        WriterConfig config = WriterConfig.builder()
                .codec(codec)
                .pageTargetRows(500)
                .rowGroupTargetRows(5_000)
                .bloomFilter("id")
                .build();
        byte[] serial = onBusyPool(context -> write(config, context));

        for (int threads : new int[] { 1, 2, 8 }) {
            try (HardwoodContext context = HardwoodContext.create(threads)) {
                for (int run = 0; run < 3; run++) {
                    assertThat(write(config, context)).as("%d threads, run %d", threads, run).isEqualTo(serial);
                }
            }
        }
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema(), config)) {
            fill(writer);
        }
        assertThat(bytes(out)).as("the writer's own context").isEqualTo(serial);
    }

    /// The row layer submits its staged records through the same append and flush.
    @Test
    void writesTheSameRowsWhateverThePoolDoes() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT64, RepetitionType.REQUIRED)
                .addColumn("name", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED, new LogicalType.StringType())
                .addColumn("score", PhysicalType.DOUBLE, RepetitionType.REQUIRED)
                .build();
        WriterConfig config = WriterConfig.builder().rowGroupTargetRows(5_000).build();
        byte[] serial = onBusyPool(context -> writeRows(schema, config, context));

        try (HardwoodContext context = HardwoodContext.create(8)) {
            assertThat(writeRows(schema, config, context)).isEqualTo(serial);
        }
    }

    /// A chunk's pages are compressed on the pool once a worker has no chunk of its own, so a
    /// single chunk spreads across threads, and the file is the one the caller writes alone. Each
    /// page takes a little while, so workers find bodies to take.
    @Test
    void compressesTheOnlyChunksPagesOnThePool() throws Exception {
        AtomicInteger handedOff = new AtomicInteger();
        Compressor recording = new TestCompressor() {
            @Override
            public byte[] compress(byte[] data, int offset, int length) {
                if (handedOff()) {
                    handedOff.incrementAndGet();
                }
                pause();
                return Arrays.copyOfRange(data, offset, offset + length);
            }
        };
        byte[] serial = onBusyPool(context -> writeIds(context, recording));
        assertThat(handedOff).hasValue(0);

        try (HardwoodContext context = HardwoodContext.create(4)) {
            for (int run = 0; run < 3; run++) {
                assertThat(writeIds(context, recording)).as("run %d", run).isEqualTo(serial);
            }
        }
        assertThat(handedOff).hasPositiveValue();
    }

    /// A page handed off fails on the worker that took it; its chunk raises the failure when the
    /// page is due.
    @Test
    void reportsAHandedOffPageFailureUnchanged() throws Exception {
        Compressor failingHandedOff = new TestCompressor() {
            @Override
            public byte[] compress(byte[] data, int offset, int length) {
                if (handedOff()) {
                    throw new ParquetWriteException("handed-off page failed");
                }
                pause();
                return Arrays.copyOfRange(data, offset, offset + length);
            }
        };
        InMemoryOutputFile out = OutputFile.inMemory();
        try (HardwoodContext context = HardwoodContext.create(4)) {
            ParquetFileWriter writer = ParquetFileWriter.create(out, idSchema(), pagePerBatch(), context,
                    failingHandedOff);
            fillIds(writer);

            assertTimeoutPreemptively(NO_HANG, () -> assertThatThrownBy(writer::endRowGroup)
                    .isInstanceOf(ParquetWriteException.class)
                    .hasMessage("handed-off page failed"));
            writer.close();
        }
        assertThatThrownBy(out::buffer)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("OutputFile was discarded");
    }

    /// One page fails while other threads compress the chunk's other pages, slowly; none may still
    /// be compressing once the failure leaves the writer. Which thread takes which page varies, so
    /// the failing page is chosen by its content: the page of a `PLAIN` `INT64` chunk starting
    /// with id 10,000.
    @Test
    void waitsForHandedOffPagesBeforeReportingAFailure() throws Exception {
        AtomicInteger compressing = new AtomicInteger();
        Compressor failingOnePage = new TestCompressor() {
            @Override
            public byte[] compress(byte[] data, int offset, int length) {
                if (ByteBuffer.wrap(data, offset, Long.BYTES).order(ByteOrder.LITTLE_ENDIAN).getLong() == 10 * BATCH) {
                    throw new ParquetWriteException("page failed");
                }
                compressing.incrementAndGet();
                try {
                    sleep();
                    return Arrays.copyOfRange(data, offset, offset + length);
                }
                finally {
                    compressing.decrementAndGet();
                }
            }
        };
        for (int run = 0; run < 3; run++) {
            try (HardwoodContext context = HardwoodContext.create(4)) {
                ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.inMemory(), idSchema(), pagePerBatch(),
                        context, failingOnePage);
                fillIds(writer);

                assertTimeoutPreemptively(NO_HANG, () -> assertThatThrownBy(writer::endRowGroup)
                        .isInstanceOf(ParquetWriteException.class)
                        .hasMessage("page failed"));
                assertThat(compressing).as("run %d", run).hasValue(0);
                writer.close();
            }
        }
    }

    @Test
    void reportsACodecFailureOnAPoolThreadUnchanged() throws Exception {
        // Set where the write runs: assertTimeoutPreemptively runs it on a thread of its own.
        AtomicReference<Thread> caller = new AtomicReference<>();
        CountDownLatch poolFailed = new CountDownLatch(1);
        // The caller holds back until a pool thread has failed, so the failure the write reports
        // is one raised off the caller's thread.
        Compressor failingOnPool = new TestCompressor() {
            @Override
            public byte[] compress(byte[] data, int offset, int length) {
                if (Thread.currentThread() != caller.get()) {
                    poolFailed.countDown();
                    throw new ParquetWriteException("codec failed on a pool thread");
                }
                await(poolFailed);
                return Arrays.copyOfRange(data, offset, offset + length);
            }
        };
        InMemoryOutputFile out = OutputFile.inMemory();
        try (HardwoodContext context = HardwoodContext.create(4)) {
            ParquetFileWriter writer = ParquetFileWriter.create(out, schema(), flushPerBatch(), context, failingOnPool);

            assertTimeoutPreemptively(NO_HANG, () -> assertThatThrownBy(() -> {
                caller.set(Thread.currentThread());
                fill(writer);
            })
                    .isInstanceOf(ParquetWriteException.class)
                    .hasMessage("codec failed on a pool thread"));
            writer.close();
        }
        assertThatThrownBy(out::buffer)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("OutputFile was discarded");
    }

    /// Of three chunks the last two fail, the third first. Whichever thread encodes which, the
    /// write reports the second chunk's failure. The case that tests the drain is the common
    /// one: the caller encodes the first chunk, a worker takes the second, and the caller then
    /// fails on the third while the worker still holds the second. Repeated, since which thread
    /// takes which chunk varies.
    @Test
    void reportsTheFirstFailureInSchemaOrder() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("ok", PhysicalType.INT32, RepetitionType.REQUIRED)
                .addColumn("first", PhysicalType.INT32, RepetitionType.REQUIRED)
                .addColumn("second", PhysicalType.INT32, RepetitionType.REQUIRED)
                .build();
        // PLAIN pages of a required column are its values alone, so a page body names its column.
        WriterConfig config = WriterConfig.builder()
                .codec(CompressionCodec.GZIP)
                .encoding(ColumnEncoding.PLAIN)
                .build();
        int[] ok = new int[BATCH];
        int[] first = new int[BATCH];
        int[] second = new int[BATCH];
        Arrays.fill(ok, 0x33333333);
        Arrays.fill(first, 0x11111111);
        Arrays.fill(second, 0x22222222);
        for (int r = 0; r < 20; r++) {
            int run = r;
            CountDownLatch secondFailed = new CountDownLatch(1);
            Compressor failingBoth = new TestCompressor() {
                @Override
                public byte[] compress(byte[] data, int offset, int length) {
                    if (data[offset] == 0x33) {
                        return Arrays.copyOfRange(data, offset, offset + length);
                    }
                    if (data[offset] == 0x11) {
                        await(secondFailed);
                        throw new ParquetWriteException("chunk 1 failed");
                    }
                    secondFailed.countDown();
                    throw new ParquetWriteException("chunk 2 failed");
                }
            };
            try (HardwoodContext context = HardwoodContext.create(1)) {
                ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.inMemory(), schema, config, context,
                        failingBoth);
                writer.columnWriter().writeBatch(batch -> batch.ints("ok", ok).ints("first", first).ints("second", second));

                assertTimeoutPreemptively(NO_HANG, () -> assertThatThrownBy(writer::endRowGroup)
                        .as("run %d", run)
                        .isInstanceOf(ParquetWriteException.class)
                        .hasMessage("chunk 1 failed"));
                writer.close();
            }
        }
    }

    @Test
    void waitsForThePoolBeforeReportingACallerFailure() throws Exception {
        // Set where the write runs: assertTimeoutPreemptively runs it on a thread of its own.
        AtomicReference<Thread> caller = new AtomicReference<>();
        CountDownLatch poolEntered = new CountDownLatch(1);
        CountDownLatch callerFailed = new CountDownLatch(1);
        AtomicInteger encodingOnPool = new AtomicInteger();
        // The caller fails while a pool thread is inside a chunk; that chunk must be finished
        // before the failure leaves the writer, or a pool thread would still be reading the row
        // group's buffers.
        Compressor failingOnCaller = new TestCompressor() {
            @Override
            public byte[] compress(byte[] data, int offset, int length) {
                if (Thread.currentThread() == caller.get()) {
                    await(poolEntered);
                    callerFailed.countDown();
                    throw new ParquetWriteException("codec failed on the caller's thread");
                }
                encodingOnPool.incrementAndGet();
                try {
                    poolEntered.countDown();
                    await(callerFailed);
                    sleep();
                    return Arrays.copyOfRange(data, offset, offset + length);
                }
                finally {
                    encodingOnPool.decrementAndGet();
                }
            }
        };
        try (HardwoodContext context = HardwoodContext.create(4)) {
            ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.inMemory(), schema(), flushPerBatch(),
                    context, failingOnCaller);

            assertTimeoutPreemptively(NO_HANG, () -> assertThatThrownBy(() -> {
                caller.set(Thread.currentThread());
                fill(writer);
            })
                    .isInstanceOf(ParquetWriteException.class)
                    .hasMessage("codec failed on the caller's thread"));
            assertThat(encodingOnPool).hasValue(0);
            writer.close();
        }
    }

    /// A schema of several columns is rejected at its first append, a one-column schema, which
    /// appends on the caller alone, at its first flush.
    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void rejectsAWriteOnAClosedContext(boolean oneColumn) throws Exception {
        FileSchema schema = oneColumn
                ? FileSchema.builder("schema").addColumn("id", PhysicalType.INT64, RepetitionType.REQUIRED).build()
                : schema();
        HardwoodContext context = HardwoodContext.create(2);
        InMemoryOutputFile out = OutputFile.inMemory();
        ParquetFileWriter writer = ParquetFileWriter.create(out, schema, flushPerBatch(), context);
        context.close();

        long[] ids = new long[BATCH];
        assertThatThrownBy(() -> {
            if (oneColumn) {
                writer.columnWriter().writeBatch(batch -> batch.longs("id", ids));
            }
            else {
                fill(writer);
            }
        })
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("The HardwoodContext this writer encodes on is closed; close the writer before its context");
        writer.close();
        assertThatThrownBy(out::buffer)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("OutputFile was discarded");
    }

    private static FileSchema idSchema() {
        return FileSchema.builder("schema").addColumn("id", PhysicalType.INT64, RepetitionType.REQUIRED).build();
    }

    /// One row group of many pages, so a single chunk has pages to hand off. `GZIP` names a codec
    /// other than `UNCOMPRESSED`, so the test's compressor runs.
    private static WriterConfig pagePerBatch() {
        return WriterConfig.builder()
                .codec(CompressionCodec.GZIP)
                .encoding(ColumnEncoding.PLAIN)
                .pageTargetRows(BATCH)
                .build();
    }

    private static byte[] writeIds(HardwoodContext context, Compressor compressor) throws IOException {
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, idSchema(), pagePerBatch(), context, compressor)) {
            fillIds(writer);
        }
        return bytes(out);
    }

    private static void fillIds(ParquetFileWriter writer) throws IOException {
        for (int from = 0; from < RECORDS; from += BATCH) {
            long[] ids = new long[BATCH];
            for (int r = 0; r < BATCH; r++) {
                ids[r] = from + r;
            }
            writer.columnWriter().writeBatch(batch -> batch.longs("id", ids));
        }
    }

    /// A row group per batch, so a flush happens inside the write. `GZIP` names a codec other than
    /// `UNCOMPRESSED`, which skips the compressor; the test's compressor is what runs.
    private static WriterConfig flushPerBatch() {
        return WriterConfig.builder()
                .codec(CompressionCodec.GZIP)
                .rowGroupTargetRows(BATCH)
                .build();
    }

    /// Flat and nested columns of several types and encodings, so chunks differ in cost.
    private static FileSchema schema() {
        return FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT64, RepetitionType.REQUIRED)
                .addColumn("name", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL, new LogicalType.StringType())
                .addColumn("score", PhysicalType.DOUBLE, RepetitionType.REQUIRED)
                .addColumn("flag", PhysicalType.BOOLEAN, RepetitionType.OPTIONAL)
                .list("tags", RepetitionType.OPTIONAL, el -> el.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL))
                .struct("point", RepetitionType.OPTIONAL, s -> s
                        .addColumn("x", PhysicalType.INT32, RepetitionType.REQUIRED)
                        .addColumn("y", PhysicalType.FLOAT, RepetitionType.OPTIONAL))
                .build();
    }

    /// Writes on a pool whose one thread is held by other work and so never runs a worker task:
    /// the caller appends and encodes every chunk itself, the single-threaded write. It also shows
    /// that a busy pool slows a write down to the caller's pace without stalling it.
    private static byte[] onBusyPool(Write write) throws Exception {
        try (HardwoodContextImpl busy = HardwoodContextImpl.create(1)) {
            CountDownLatch release = new CountDownLatch(1);
            busy.executor().submit(() -> {
                release.await();
                return null;
            });
            byte[] bytes = assertTimeoutPreemptively(NO_HANG, () -> write.to(busy));
            release.countDown();
            return bytes;
        }
    }

    @FunctionalInterface
    private interface Write {

        byte[] to(HardwoodContext context) throws IOException;
    }

    private static byte[] writeRows(FileSchema schema, WriterConfig config, HardwoodContext context)
            throws IOException {
        Random random = new Random(1523);
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, config, context)) {
            RowWriter rows = writer.rowWriter();
            for (int r = 0; r < RECORDS; r++) {
                long id = r;
                String name = "name-" + random.nextInt(50);
                double score = random.nextDouble();
                rows.writeRow(row -> row.setLong("id", id).setString("name", name).setDouble("score", score));
            }
        }
        return bytes(out);
    }

    private static byte[] write(WriterConfig config, HardwoodContext context) throws IOException {
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema(), config, context)) {
            fill(writer);
        }
        return bytes(out);
    }

    private static void fill(ParquetFileWriter writer) throws IOException {
        Random random = new Random(1455);
        byte[][] names = new byte[50][];
        for (int n = 0; n < names.length; n++) {
            names[n] = ("name-" + n).getBytes(StandardCharsets.UTF_8);
        }
        ColumnWriter columns = writer.columnWriter();
        for (int from = 0; from < RECORDS; from += BATCH) {
            long[] id = new long[BATCH];
            byte[][] name = new byte[BATCH][];
            boolean[] nameNulls = new boolean[BATCH];
            double[] score = new double[BATCH];
            boolean[] flag = new boolean[BATCH];
            boolean[] flagNulls = new boolean[BATCH];
            int[] tagOffsets = new int[BATCH + 1];
            boolean[] tagsNulls = new boolean[BATCH];
            boolean[] pointNulls = new boolean[BATCH];
            int[] x = new int[BATCH];
            float[] y = new float[BATCH];
            boolean[] yNulls = new boolean[BATCH];
            for (int r = 0; r < BATCH; r++) {
                id[r] = from + r;
                nameNulls[r] = random.nextInt(10) == 0;
                name[r] = nameNulls[r] ? new byte[0] : names[random.nextInt(names.length)];
                score[r] = random.nextDouble();
                flagNulls[r] = random.nextInt(5) == 0;
                flag[r] = random.nextBoolean();
                tagsNulls[r] = random.nextInt(8) == 0;
                tagOffsets[r + 1] = tagOffsets[r] + (tagsNulls[r] ? 0 : random.nextInt(4));
                pointNulls[r] = random.nextInt(6) == 0;
                x[r] = random.nextInt(1000);
                yNulls[r] = random.nextInt(4) == 0;
                y[r] = random.nextFloat();
            }
            int[] tags = new int[tagOffsets[BATCH]];
            boolean[] tagNulls = new boolean[tags.length];
            for (int t = 0; t < tags.length; t++) {
                tags[t] = random.nextInt(100);
                tagNulls[t] = random.nextInt(10) == 0;
            }
            columns.writeBatch(batch -> batch
                    .longs("id", id)
                    .bytes("name", name, nameNulls)
                    .doubles("score", score)
                    .booleans("flag", flag, flagNulls)
                    .list("tags", tagOffsets, Validity.ofNulls(tagsNulls))
                    .ints("tags.list.element", tags, tagNulls)
                    .struct("point", Validity.ofNulls(pointNulls))
                    .ints("point.x", x)
                    .floats("point.y", y, yNulls));
        }
    }

    private static byte[] bytes(InMemoryOutputFile out) {
        ByteBuffer buffer = out.buffer();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    private abstract static class TestCompressor implements Compressor {

        @Override
        public String getName() {
            return "TEST";
        }

        static void await(CountDownLatch latch) {
            try {
                if (!latch.await(30, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Timed out waiting for the other thread");
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }

        /// Whether the calling thread compresses a body another chunk's thread handed off.
        static boolean handedOff() {
            return StackWalker.getInstance().walk(frames -> frames.anyMatch(
                    f -> f.getClassName().endsWith(".PageCompression") && f.getMethodName().equals("help")));
        }

        static void pause() {
            try {
                Thread.sleep(2);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }

        static void sleep() {
            try {
                Thread.sleep(50);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
    }
}
