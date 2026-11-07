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

/// A later file whose footer reads but whose data does not fails the read once the read reaches
/// its data: every row of the files before it is returned first (#1711).
class LaterFileDataFailureTest {

    private static final Path FIRST = Paths.get("src/test/resources/multi_file_part0.parquet");
    private static final Path SECOND = Paths.get("src/test/resources/multi_file_part1.parquet");

    @Test
    void everyRowOfTheFileBeforeIsReturnedFirst() throws Exception {
        long expectedRows;
        try (ParquetFileReader first = ParquetFileReader.open(InputFile.of(FIRST))) {
            expectedRows = first.getFileMetaData().numRows();
        }

        AtomicLong rows = new AtomicLong();
        try (ParquetFileReader parquet = ParquetFileReader.openAll(
                List.of(InputFile.of(FIRST), new DataUnreadableInputFile(InputFile.of(SECOND))))) {
            assertThatThrownBy(() -> {
                try (RowReader reader = parquet.rowReader()) {
                    while (reader.hasNext()) {
                        reader.next();
                        rows.incrementAndGet();
                    }
                }
            }).isInstanceOf(IOException.class)
                    .hasMessage("[data-unreadable.parquet] Failed to fetch data at offset 4 (length 470)");
        }
        assertThat(rows.get()).isEqualTo(expectedRows);
    }

    /// The same through column readers, over a first file of several row groups and batches, every
    /// one of which is returned before the failure.
    @Test
    void everyBatchOfTheFileBeforeIsReturnedFirstByColumnReaders() throws Exception {
        int rowCount = 50_000;
        long[] ids = new long[rowCount];
        for (int i = 0; i < rowCount; i++) {
            ids[i] = i;
        }
        InputFile first = write(ids);
        InputFile second = new DataUnreadableInputFile(write(ids));

        AtomicLong rows = new AtomicLong();
        AtomicLong sum = new AtomicLong();
        try (ParquetFileReader parquet = ParquetFileReader.openAll(List.of(first, second))) {
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
                    .hasMessage("[data-unreadable.parquet] Failed to fetch data at offset 4 (length 11382)");
        }
        assertThat(rows.get()).isEqualTo(rowCount);
        assertThat(sum.get()).isEqualTo((long) rowCount * (rowCount - 1) / 2);
    }

    private static InputFile write(long[] ids) throws IOException {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT64, RepetitionType.REQUIRED)
                .build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema,
                WriterConfig.builder().pageTargetRows(1_000).rowGroupTargetRows(10_000).build())) {
            writer.columnWriter().writeBatch(batch -> batch.longs("id", ids));
        }
        return InputFile.of(ByteBuffer.wrap(InMemoryFiles.toByteArray(out)));
    }

    /// Fails every read that starts at the file's first page, at offset 4: its metadata, footer and
    /// page index read, its data does not.
    private static final class DataUnreadableInputFile implements InputFile {

        private static final long FIRST_PAGE = 4;

        private final InputFile delegate;

        DataUnreadableInputFile(InputFile delegate) {
            this.delegate = delegate;
        }

        @Override
        public void open() throws IOException {
            delegate.open();
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            if (offset == FIRST_PAGE) {
                throw new IOException("Device not ready");
            }
            return delegate.readRange(offset, length);
        }

        @Override
        public long length() throws IOException {
            return delegate.length();
        }

        @Override
        public String name() {
            return "data-unreadable.parquet";
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
