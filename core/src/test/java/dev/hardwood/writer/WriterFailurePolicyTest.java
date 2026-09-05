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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.internal.writer.ByteBufferOutputFile;
import dev.hardwood.reader.ParquetFileReader;

import static dev.hardwood.writer.WriterTestSupport.oneColumn;
import static dev.hardwood.writer.WriterTestSupport.readInts;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Tests the failure-poisoning and discard-on-close behaviour introduced to uphold the
/// documented contract: "a failure leaves nothing behind."
///
/// A writer that has thrown from [ColumnWriter#writeBatch] is marked as failed. Under the
/// default [WriteFailurePolicy#DISCARD], `close()` discards rather than publishing the truncated
/// prefix; under [WriteFailurePolicy#COMMIT_PREFIX], the prefix is published as a valid file.
class WriterFailurePolicyTest {

    @Test
    void failedBatchDiscardsOnClose(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("out.parquet");

        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), oneColumn())) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, new int[] { 1, 2, 3 }));
            try {
                // Second batch is invalid: the same column named twice.
                writer.columnWriter().writeBatch(
                        batch -> batch.ints(0, new int[] { 4, 5 }).ints(0, new int[] { 6 }));
            }
            catch (IllegalArgumentException expected) {
                // Caller notices the failure and stops writing.
            }
        }
        // Default policy is DISCARD: no file at the target, no temp file.
        assertThat(Files.exists(file)).isFalse();
        assertThat(Files.exists(file.resolveSibling(file.getFileName() + ".hardwood-tmp"))).isFalse();
    }

    @Test
    void failedBatchWithCommitPrefixPublishesGoodRows(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("out.parquet");
        WriterConfig config = WriterConfig.builder()
                .writeFailurePolicy(WriteFailurePolicy.COMMIT_PREFIX)
                .build();

        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), oneColumn(), config)) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, new int[] { 1, 2, 3 }));
            try {
                writer.columnWriter().writeBatch(
                        batch -> batch.ints(0, new int[] { 4, 5 }).ints(0, new int[] { 6 }));
            }
            catch (IllegalArgumentException expected) {
                // Caller notices and stops.
            }
        }
        // COMMIT_PREFIX: the file is published with the first batch's rows.
        assertThat(Files.exists(file)).isTrue();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(3);
            assertThat(readInts(reader, 0)).containsExactly(1, 2, 3);
        }
    }

    @Test
    void writerRejectsWriteAfterFailure() throws Exception {
        ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), oneColumn());
        ColumnWriter columns = writer.columnWriter();

        // Good batch.
        columns.writeBatch(batch -> batch.ints(0, new int[] { 1, 2, 3 }));

        // Bad batch: duplicate column.
        assertThatThrownBy(() -> columns.writeBatch(
                batch -> batch.ints(0, new int[] { 4, 5 }).ints(0, new int[] { 6 })))
                .isInstanceOf(IllegalArgumentException.class);

        // The writer is now poisoned: the next writeBatch must be rejected.
        assertThatThrownBy(() -> columns.writeBatch(batch -> batch.ints(0, new int[] { 7 })))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("failed");

        writer.close();
    }

    @Test
    void metadataStillSettableAfterFailure() throws Exception {
        ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), oneColumn());
        ColumnWriter columns = writer.columnWriter();
        columns.writeBatch(batch -> batch.ints(0, new int[] { 1 }));

        // Poison the writer.
        assertThatThrownBy(() -> columns.writeBatch(
                batch -> batch.ints(0, new int[] { 2 }).ints(0, new int[] { 3 })))
                .isInstanceOf(IllegalArgumentException.class);

        // Metadata setters remain usable: no exception.
        writer.keyValueMetadata("key", "value");
        writer.createdBy("test version 1.0 (build abc)");

        writer.close();
    }

    @Test
    void rowWriterRecoveryDoesNotPoison(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("out.parquet");

        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), oneColumn())) {
            RowWriter rows = writer.rowWriter();

            // Good record.
            rows.writeRow(row -> row.setInt("id", 1));

            // Bad record: unknown field name. This fails in plan.writeRecord(),
            // before writeStagedBatch(), so it should NOT poison the writer.
            assertThatThrownBy(() -> rows.writeRow(row -> row.setInt("nope", 2)))
                    .isInstanceOf(IllegalArgumentException.class);

            // Another good record: should succeed because the writer is NOT poisoned.
            rows.writeRow(row -> row.setInt("id", 3));
        }
        // The file should be published with the two good records.
        assertThat(Files.exists(file)).isTrue();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(2);
            assertThat(readInts(reader, 0)).containsExactly(1, 3);
        }
    }

    @Test
    void ioFailureDuringFlushPoisons(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("out.parquet");

        // An OutputFile that fails on the third write call. Call 1 is the leading magic
        // (4 bytes). Calls 2+ are row-group page data. Failing on call 3 simulates an
        // I/O failure mid-flush.
        FailOnNthWrite failingOut = new FailOnNthWrite(OutputFile.of(file), 3);

        // A very small row-group target so that writeBatch triggers a flushRowGroup()
        // inside writeStagedBatch(), which will hit the failing write.
        WriterConfig config = WriterConfig.builder()
                .rowGroupTargetRows(1)
                .build();

        ParquetFileWriter writer = ParquetFileWriter.create(failingOut, oneColumn(), config);
        ColumnWriter columns = writer.columnWriter();

        // This writeBatch writes multiple rows, and with rowGroupTargetRows=1 each row
        // triggers a flushRowGroup() inside the loop, hitting the FailOnNthWrite.
        assertThatThrownBy(() -> columns.writeBatch(batch -> batch.ints(0, new int[] { 1, 2, 3 })))
                .isInstanceOf(IOException.class);

        // Writer is now poisoned.
        assertThatThrownBy(() -> columns.writeBatch(batch -> batch.ints(0, new int[] { 4 })))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("failed");

        writer.close();
        // DISCARD policy: no file at the target.
        assertThat(Files.exists(file)).isFalse();
    }

    @Test
    void doubleCloseAfterFailureIsSilent() throws Exception {
        ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), oneColumn());

        // Poison the writer.
        assertThatThrownBy(() -> writer.columnWriter().writeBatch(
                batch -> batch.ints(0, new int[] { 1, 2 }).ints(0, new int[] { 3 })))
                .isInstanceOf(IllegalArgumentException.class);

        // First close: discards silently.
        writer.close();
        // Second close: idempotent, no exception.
        writer.close();
    }

    @Test
    void unpoisonedWriterPublishesNormally(@TempDir Path dir) throws Exception {
        // Sanity check: a writer that never fails still publishes under the default policy.
        Path file = dir.resolve("out.parquet");

        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), oneColumn())) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, new int[] { 1, 2, 3 }));
        }
        assertThat(Files.exists(file)).isTrue();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(3);
            assertThat(readInts(reader, 0)).containsExactly(1, 2, 3);
        }
    }

    /// An [OutputFile] that throws on the Nth `write()` call, simulating an I/O failure
    /// during a row group flush.
    private static final class FailOnNthWrite implements OutputFile {

        private final OutputFile delegate;
        private final int failOnCall;
        private int writeCount;

        FailOnNthWrite(OutputFile delegate, int failOnCall) {
            this.delegate = delegate;
            this.failOnCall = failOnCall;
        }

        @Override
        public void create() throws IOException {
            delegate.create();
        }

        @Override
        public void write(ByteBuffer data) throws IOException {
            if (++writeCount == failOnCall) {
                throw new IOException("injected write failure on call " + failOnCall);
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
            delegate.discard();
        }
    }
}
