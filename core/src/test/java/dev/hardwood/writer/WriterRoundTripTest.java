/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.thrift.PageHeaderReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.internal.writer.ByteBufferOutputFile;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Round-trip tests: write a flat `REQUIRED INT32` file with [ParquetFileWriter],
/// then read it back with [ParquetFileReader] and assert the values survive.
class WriterRoundTripTest {

    @Test
    void writesAndReadsBackTwoIntColumns() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("a", PhysicalType.INT32, RepetitionType.REQUIRED)
                .addColumn("b", PhysicalType.INT32, RepetitionType.REQUIRED)
                .build();

        int[] a = { 1, 2, 3, 4, 5 };
        int[] b = { 10, 20, 30, 40, 50 };

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.writeInts(0, a);
            writer.writeInts(1, b);
        }

        ByteBuffer bytes = ByteBuffer.wrap(out.toByteArray());
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(bytes))) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(5);
            assertThat(reader.getFileMetaData().createdBy()).isEqualTo("hardwood");
            assertThat(reader.getFileSchema().getColumnCount()).isEqualTo(2);
            assertThat(reader.getFileSchema().isFlatSchema()).isTrue();

            assertThat(readInts(reader, 0)).containsExactly(a);
            assertThat(readInts(reader, 1)).containsExactly(b);
        }
    }

    @Test
    void writesToLocalFileWithAtomicRename(@TempDir Path dir) throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT32, RepetitionType.REQUIRED)
                .build();
        int[] ids = { 7, 8, 9 };

        Path file = dir.resolve("out.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema)) {
            writer.writeInts(0, ids);
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(3);
            assertThat(readInts(reader, 0)).containsExactly(ids);
        }
    }

    @Test
    void emptyColumnRoundTrips() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT32, RepetitionType.REQUIRED)
                .build();

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.writeInts(0, new int[0]);
        }

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())))) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(0);
        }
    }

    @Test
    void rejectsWritingTheSameColumnTwice() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT32, RepetitionType.REQUIRED)
                .build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), schema)) {
            writer.writeInts(0, new int[] { 1, 2, 3 });
            assertThatThrownBy(() -> writer.writeInts(0, new int[] { 4, 5, 6 }))
                    .isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void rejectsRowCountMismatchAcrossColumns() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("a", PhysicalType.INT32, RepetitionType.REQUIRED)
                .addColumn("b", PhysicalType.INT32, RepetitionType.REQUIRED)
                .build();
        ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), schema);
        writer.writeInts(0, new int[] { 1, 2, 3 });
        assertThatThrownBy(() -> writer.writeInts(1, new int[] { 1, 2 }))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNonInt32Column() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("v", PhysicalType.INT64, RepetitionType.REQUIRED)
                .build();
        ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), schema);
        assertThatThrownBy(() -> writer.writeInts(0, new int[] { 1 }))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void rejectsNonRequiredColumn() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("v", PhysicalType.INT32, RepetitionType.OPTIONAL)
                .build();
        ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), schema);
        assertThatThrownBy(() -> writer.writeInts(0, new int[] { 1 }))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void rejectsUseAfterClose() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT32, RepetitionType.REQUIRED)
                .build();
        ParquetFileWriter writer = ParquetFileWriter.create(new ByteBufferOutputFile(), schema);
        writer.writeInts(0, new int[] { 1, 2, 3 });
        writer.close();
        assertThatThrownBy(() -> writer.writeInts(0, new int[] { 4 }))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void failedCloseLeavesNoFileAtTarget(@TempDir Path dir) throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("a", PhysicalType.INT32, RepetitionType.REQUIRED)
                .addColumn("b", PhysicalType.INT32, RepetitionType.REQUIRED)
                .build();
        Path file = dir.resolve("partial.parquet");

        ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema);
        writer.writeInts(0, new int[] { 1, 2, 3 }); // column "b" never written

        assertThatThrownBy(writer::close).isInstanceOf(IllegalStateException.class);
        // The footer never completed, so nothing must be published at the target.
        assertThat(Files.exists(file)).isFalse();
    }

    @Test
    void largeColumnIsSplitAcrossMultiplePages() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT32, RepetitionType.REQUIRED)
                .build();
        // Comfortably more than one target page (262,144 INT32 values per 1 MiB page).
        int n = 600_000;
        int[] values = new int[n];
        for (int i = 0; i < n; i++) {
            values[i] = i;
        }

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.writeInts(0, values);
        }
        byte[] bytes = out.toByteArray();

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(bytes)))) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(n);
            ColumnMetaData meta = reader.getFileMetaData().rowGroups().get(0).columns().get(0).metaData();
            assertThat(countDataPages(bytes, meta.dataPageOffset(), meta.numValues())).isGreaterThan(1);
            // Arrays.equals over containsExactly: the latter is O(n) with per-element
            // boxing/description and is needlessly slow at 600k elements.
            assertThat(Arrays.equals(readInts(reader, 0), values)).isTrue();
        }
    }

    /// Walks the column chunk's contiguous data pages from `startOffset`, returning
    /// how many pages it took to cover `totalValues`.
    private static int countDataPages(byte[] file, long startOffset, long totalValues) throws Exception {
        ByteBuffer buf = ByteBuffer.wrap(file);
        int offset = Math.toIntExact(startOffset);
        long seen = 0;
        int pages = 0;
        while (seen < totalValues) {
            ThriftCompactReader reader = new ThriftCompactReader(buf, offset);
            PageHeader header = PageHeaderReader.read(reader);
            pages++;
            seen += header.dataPageHeader().numValues();
            offset += reader.getBytesRead() + header.compressedPageSize();
        }
        return pages;
    }

    private static int[] readInts(ParquetFileReader reader, int columnIndex) {
        try (ColumnReader column = reader.columnReader(columnIndex)) {
            int[] result = new int[reader.getFileMetaData().numRows() < 0 ? 0
                    : Math.toIntExact(reader.getFileMetaData().numRows())];
            int pos = 0;
            while (column.nextBatch()) {
                int count = column.getValueCount();
                int[] batch = column.getInts();
                System.arraycopy(batch, 0, result, pos, count);
                pos += count;
            }
            return result;
        }
    }
}
