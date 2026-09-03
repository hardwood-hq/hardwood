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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.internal.compression.Compressor;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.reader.ParquetFileReader;

import static dev.hardwood.writer.WriterTestSupport.oneColumn;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// What a writer leaves at the destination when a write fails, when the caller aborts it, and
/// when the output cannot be discarded.
///
/// A write that throws fails the writer: it accepts no more data, and `close()` discards the
/// output. [ParquetFileWriter#abort()] discards it for a failure the writer does not see.
class WriterFailureTest {

    private static final String FAILED = "A previous write failed; the writer accepts no more data";

    @TempDir
    Path dir;

    @Test
    void rejectedBatchFailsTheWriter() throws Exception {
        Path file = dir.resolve("out.parquet");

        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), oneColumn())) {
            ColumnWriter columns = writer.columnWriter();
            columns.writeBatch(batch -> batch.ints(0, new int[] { 1, 2, 3 }));
            assertThatThrownBy(() -> columns.writeBatch(
                    batch -> batch.ints(0, new int[] { 4, 5 }).ints(0, new int[] { 6 })))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Column 0 (id) is already set in this batch");
            assertThatThrownBy(() -> columns.writeBatch(batch -> batch.ints(0, new int[] { 7 })))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage(FAILED);
        }

        assertNothingAt(file);
    }

    @Test
    void throwingBatchFillerFailsTheWriter() throws Exception {
        Path file = dir.resolve("out.parquet");

        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), oneColumn())) {
            ColumnWriter columns = writer.columnWriter();
            columns.writeBatch(batch -> batch.ints(0, new int[] { 1, 2, 3 }));
            assertThatThrownBy(() -> columns.writeBatch(batch -> {
                throw new IllegalStateException("source failed");
            }))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("source failed");
            assertThatThrownBy(() -> columns.writeBatch(batch -> batch.ints(0, new int[] { 7 })))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage(FAILED);
        }

        assertNothingAt(file);
    }

    @Test
    void errorInBatchFillerFailsTheWriter() throws Exception {
        Path file = dir.resolve("out.parquet");

        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), oneColumn())) {
            ColumnWriter columns = writer.columnWriter();
            columns.writeBatch(batch -> batch.ints(0, new int[] { 1, 2, 3 }));
            assertThatThrownBy(() -> columns.writeBatch(batch -> {
                throw new AssertionError("invariant broken");
            }))
                    .isInstanceOf(AssertionError.class)
                    .hasMessage("invariant broken");
        }

        assertNothingAt(file);
    }

    @Test
    void destinationFailureDuringBatchDiscardsOnClose() throws Exception {
        Path file = dir.resolve("out.parquet");
        ParquetFileWriter writer = failedWriter(new FailingOutputFile(OutputFile.of(file), 3, false));

        assertThatThrownBy(() -> writer.columnWriter().writeBatch(batch -> batch.ints(0, new int[] { 4 })))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(FAILED);

        writer.close();
        assertNothingAt(file);
    }

    @Test
    void codecFailureDuringBatchDiscardsOnClose() throws Exception {
        Path file = dir.resolve("out.parquet");
        ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), oneColumn(),
                flushPerRecord(), FAILING_CODEC);

        assertThatThrownBy(() -> writer.columnWriter().writeBatch(batch -> batch.ints(0, new int[] { 1, 2 })))
                .isInstanceOf(ParquetWriteException.class)
                .hasMessage("codec unavailable");

        writer.close();
        assertNothingAt(file);
    }

    @Test
    void failedRowBatchRejectsFurtherRowsAndDiscardsOnClose() throws Exception {
        Path file = dir.resolve("out.parquet");
        ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), oneColumn(),
                flushPerRecord(), FAILING_CODEC);
        RowWriter rows = writer.rowWriter();

        // Records are submitted a batch at a time, so the failing flush is triggered by one of
        // them rather than by the first.
        assertThatThrownBy(() -> {
            for (int i = 0; i < 4096; i++) {
                int value = i;
                rows.writeRow(row -> row.setInt(0, value));
            }
        }).isInstanceOf(ParquetWriteException.class)
          .hasMessage("codec unavailable");
        assertThatThrownBy(() -> rows.writeRow(row -> row.setInt(0, -1)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(FAILED);

        writer.close();
        assertNothingAt(file);
    }

    @Test
    void rejectedRecordFailsTheWriter() throws Exception {
        Path file = dir.resolve("out.parquet");

        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), oneColumn())) {
            RowWriter rows = writer.rowWriter();
            rows.writeRow(row -> row.setInt("id", 1));
            assertThatThrownBy(() -> rows.writeRow(row -> row.setInt("nope", 2)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("No field named 'nope' in the record; it has id");
            assertThatThrownBy(() -> rows.writeRow(row -> row.setInt("id", 3)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage(FAILED);
        }

        assertNothingAt(file);
    }

    @Test
    void throwingRecordFillerFailsTheWriter() throws Exception {
        Path file = dir.resolve("out.parquet");

        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), oneColumn())) {
            RowWriter rows = writer.rowWriter();
            rows.writeRow(row -> row.setInt("id", 1));
            assertThatThrownBy(() -> rows.writeRow(row -> {
                throw new IllegalStateException("source failed");
            }))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("source failed");
            assertThatThrownBy(() -> rows.writeRow(row -> row.setInt("id", 3)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage(FAILED);
        }

        assertNothingAt(file);
    }

    @Test
    void errorInRecordFillerFailsTheWriter() throws Exception {
        Path file = dir.resolve("out.parquet");

        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), oneColumn())) {
            RowWriter rows = writer.rowWriter();
            rows.writeRow(row -> row.setInt("id", 1));
            assertThatThrownBy(() -> rows.writeRow(row -> {
                throw new AssertionError("invariant broken");
            }))
                    .isInstanceOf(AssertionError.class)
                    .hasMessage("invariant broken");
        }

        assertNothingAt(file);
    }

    @Test
    void errorWhileClosingDiscards() throws Exception {
        Path file = dir.resolve("out.parquet");
        // No row-group target reached while writing, so the codec first runs inside close().
        ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), oneColumn(),
                WriterConfig.builder().codec(CompressionCodec.GZIP).build(), ERROR_CODEC);
        writer.columnWriter().writeBatch(batch -> batch.ints(0, new int[] { 1, 2, 3 }));

        assertThatThrownBy(writer::close)
                .isInstanceOf(AssertionError.class)
                .hasMessage("codec invariant broken");
        assertNothingAt(file);
    }

    @Test
    void metadataIsSettableAfterFailure() throws Exception {
        Path file = dir.resolve("out.parquet");
        ParquetFileWriter writer = failedWriter(new FailingOutputFile(OutputFile.of(file), 3, false));

        writer.keyValueMetadata("key", "value");
        writer.createdBy("test version 1.0 (build abc)");

        writer.close();
        assertNothingAt(file);
    }

    @Test
    void closeAfterFailureTwiceDoesNothingTheSecondTime() throws Exception {
        Path file = dir.resolve("out.parquet");
        FailingOutputFile out = new FailingOutputFile(OutputFile.of(file), 3, false);
        ParquetFileWriter writer = failedWriter(out);

        writer.close();
        writer.close();
        assertThat(out.discards).isEqualTo(1);
        assertNothingAt(file);
    }

    @Test
    void flushFailingInsideCloseDiscardsOnce() throws Exception {
        Path file = dir.resolve("out.parquet");
        FailingOutputFile out = new FailingOutputFile(OutputFile.of(file), 0, false);
        ParquetFileWriter writer = ParquetFileWriter.create(out, oneColumn(), flushPerRecord(), FAILING_CODEC);
        // Staged but not yet submitted, so the batch that fails is the one close() submits.
        writer.rowWriter().writeRow(row -> row.setInt(0, 1));

        assertThatThrownBy(writer::close)
                .isInstanceOf(ParquetWriteException.class)
                .hasMessage("codec unavailable");
        writer.close();

        assertThat(out.discards).isEqualTo(1);
        assertNothingAt(file);
    }

    @Test
    void closeReportsAnOutputThatCannotBeDiscarded() throws Exception {
        ParquetFileWriter writer = failedWriter(
                new FailingOutputFile(OutputFile.of(dir.resolve("out.parquet")), 3, true));

        assertThatThrownBy(writer::close)
                .isInstanceOf(IOException.class)
                .hasMessage("discard failed");
    }

    @Test
    void abortDiscardsAHealthyWriter() throws Exception {
        Path file = dir.resolve("out.parquet");

        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), oneColumn())) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, new int[] { 1, 2, 3 }));
            writer.abort();
            writer.abort();
            assertThatThrownBy(() -> writer.columnWriter())
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Writer is closed");
        }

        assertNothingAt(file);
    }

    @Test
    void abortOnAFailureOutsideTheWriterLeavesNothing() throws Exception {
        Path file = dir.resolve("out.parquet");

        assertThatThrownBy(() -> {
            try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), oneColumn())) {
                try {
                    writer.columnWriter().writeBatch(batch -> batch.ints(0, new int[] { 1, 2, 3 }));
                    throw new IllegalStateException("source failed");
                }
                catch (RuntimeException e) {
                    try {
                        writer.abort();
                    }
                    catch (IOException discardFailure) {
                        e.addSuppressed(discardFailure);
                    }
                    throw e;
                }
            }
        }).isInstanceOf(IllegalStateException.class)
          .hasMessage("source failed");

        assertNothingAt(file);
    }

    @Test
    void abortAfterCloseKeepsThePublishedFile() throws Exception {
        Path file = dir.resolve("out.parquet");
        ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), oneColumn());
        writer.columnWriter().writeBatch(batch -> batch.ints(0, new int[] { 1, 2, 3 }));
        writer.close();

        writer.abort();

        assertThat(readInts(file)).containsExactly(1, 2, 3);
    }

    @Test
    void abortReportsAnOutputThatCannotBeDiscarded() throws Exception {
        FailingOutputFile out = new FailingOutputFile(OutputFile.of(dir.resolve("out.parquet")), 0, true);
        ParquetFileWriter writer = ParquetFileWriter.create(out, oneColumn());

        assertThatThrownBy(writer::abort)
                .isInstanceOf(IOException.class)
                .hasMessage("discard failed");
    }

    @Test
    void healthyWriterPublishesOnClose() throws Exception {
        Path file = dir.resolve("out.parquet");

        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), oneColumn())) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, new int[] { 1, 2, 3 }));
        }

        assertThat(readInts(file)).containsExactly(1, 2, 3);
    }

    /// Nothing at the target, and no temporary sibling left behind.
    private static void assertNothingAt(Path file) throws IOException {
        try (Stream<Path> entries = Files.list(file.getParent())) {
            assertThat(entries).isEmpty();
        }
    }

    private static int[] readInts(Path file) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            return WriterTestSupport.readInts(reader, 0);
        }
    }

    /// A writer failed by its destination: with a row group per record, the third write to the
    /// destination (the first being the leading magic) falls inside the first batch.
    private static ParquetFileWriter failedWriter(FailingOutputFile out) throws IOException {
        ParquetFileWriter writer = ParquetFileWriter.create(out, oneColumn(),
                WriterConfig.builder().rowGroupTargetRows(1).build());
        assertThatThrownBy(() -> writer.columnWriter().writeBatch(batch -> batch.ints(0, new int[] { 1, 2, 3 })))
                .isInstanceOf(IOException.class)
                .hasMessage("write 3 failed");
        return writer;
    }

    /// A row group per record, so a flush happens inside the write call. `GZIP` names a codec
    /// that is not `UNCOMPRESSED`, which skips compression; [#FAILING_CODEC] is what runs.
    private static WriterConfig flushPerRecord() {
        return WriterConfig.builder()
                .codec(CompressionCodec.GZIP)
                .rowGroupTargetRows(1)
                .build();
    }

    private static final Compressor ERROR_CODEC = new Compressor() {
        @Override
        public byte[] compress(byte[] data, int offset, int length) {
            throw new AssertionError("codec invariant broken");
        }

        @Override
        public String getName() {
            return "ERROR";
        }
    };

    private static final Compressor FAILING_CODEC = new Compressor() {
        @Override
        public byte[] compress(byte[] data, int offset, int length) {
            throw new ParquetWriteException("codec unavailable");
        }

        @Override
        public String getName() {
            return "FAILING";
        }
    };

    /// An [OutputFile] whose `n`th `write` fails (none for `0`), whose `discard` optionally
    /// fails after releasing the delegate, and which counts the discards.
    private static final class FailingOutputFile implements OutputFile {

        private final OutputFile delegate;
        private final int failingWrite;
        private final boolean failDiscard;
        private int writes;
        private int discards;

        FailingOutputFile(OutputFile delegate, int failingWrite, boolean failDiscard) {
            this.delegate = delegate;
            this.failingWrite = failingWrite;
            this.failDiscard = failDiscard;
        }

        @Override
        public void create() throws IOException {
            delegate.create();
        }

        @Override
        public void write(ByteBuffer data) throws IOException {
            if (++writes == failingWrite) {
                throw new IOException("write " + failingWrite + " failed");
            }
            delegate.write(data);
        }

        @Override
        public long position() {
            return delegate.position();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        @Override
        public void discard() throws IOException {
            discards++;
            delegate.discard();
            if (failDiscard) {
                throw new IOException("discard failed");
            }
        }
    }
}
