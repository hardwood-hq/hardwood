/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A later file whose footer cannot be read fails the read when the read reaches it: every row
/// of the files before it is returned first, and the failure names the file it belongs to
/// rather than the row group the read was in when that file was planned.
class LaterFileFailureTest {

    private static final Path FIRST = Paths.get("src/test/resources/multi_file_part0.parquet");

    @Test
    void everyRowOfTheFileBeforeIsReturnedFirst() throws Exception {
        long expectedRows;
        try (ParquetFileReader first = ParquetFileReader.open(InputFile.of(FIRST))) {
            expectedRows = first.getFileMetaData().numRows();
        }

        AtomicLong rows = new AtomicLong();
        try (ParquetFileReader parquet = ParquetFileReader.openAll(
                List.of(InputFile.of(FIRST), new UnreadableInputFile()))) {
            assertThatThrownBy(() -> {
                try (RowReader reader = parquet.rowReader()) {
                    while (reader.hasNext()) {
                        reader.next();
                        rows.incrementAndGet();
                    }
                }
            }).isInstanceOf(IOException.class)
                    .hasMessage("[unreadable.parquet] Failed to read metadata");
        }
        assertThat(rows.get()).isEqualTo(expectedRows);
    }

    /// The same through column readers, over a first file of several row groups and batches,
    /// every one of which is returned before the failure.
    @Test
    void everyBatchOfTheFileBeforeIsReturnedFirstByColumnReaders() throws Exception {
        int rowCount = 50_000;
        long[] ids = new long[rowCount];
        for (int i = 0; i < rowCount; i++) {
            ids[i] = i;
        }
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT64, RepetitionType.REQUIRED)
                .build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema,
                WriterConfig.builder().pageTargetRows(1_000).rowGroupTargetRows(10_000).build())) {
            writer.columnWriter().writeBatch(batch -> batch.longs("id", ids));
        }
        InputFile first = InputFile.of(ByteBuffer.wrap(InMemoryFiles.toByteArray(out)));

        AtomicLong rows = new AtomicLong();
        AtomicLong sum = new AtomicLong();
        try (ParquetFileReader parquet = ParquetFileReader.openAll(List.of(first, new UnreadableInputFile()))) {
            assertThatThrownBy(() -> {
                try (ColumnReaders columns = parquet.buildColumnReaders(ColumnProjection.all()).batchSize(4_096)
                        .build()) {
                    while (columns.nextBatch()) {
                        int count = columns.getRecordCount();
                        long[] values = columns.getColumnReader(0).getLongs();
                        for (int i = 0; i < count; i++) {
                            sum.addAndGet(values[i]);
                        }
                        rows.addAndGet(count);
                    }
                }
            }).isInstanceOf(IOException.class)
                    .hasMessage("[unreadable.parquet] Failed to read metadata");
        }
        assertThat(rows.get()).isEqualTo(rowCount);
        assertThat(sum.get()).isEqualTo((long) rowCount * (rowCount - 1) / 2);
    }

    /// Opens, but fails every read, so its footer cannot be loaded.
    private static final class UnreadableInputFile implements InputFile {

        @Override
        public void open() {
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            throw new IOException("Device not ready");
        }

        @Override
        public long length() {
            return 1024;
        }

        @Override
        public String name() {
            return "unreadable.parquet";
        }

        @Override
        public void close() {
        }
    }
}
