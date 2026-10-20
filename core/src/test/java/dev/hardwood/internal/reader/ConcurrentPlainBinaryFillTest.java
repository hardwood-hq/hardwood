/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import dev.hardwood.HardwoodContext;
import dev.hardwood.InMemoryFiles;
import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.BinaryValues;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ColumnEncoding;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;

/// The pages of a compressed `PLAIN` binary column, small against the batches, are decoded on
/// several threads, so a cell spans many pages whose segments copy their values from the pages'
/// decompressed bytes into chunks of their own at the same time ([BinaryFill]). Every value reads
/// as written, through the column readers and through the row reader, on every pass of a reused
/// context.
class ConcurrentPlainBinaryFillTest {

    private static final int ROWS = 50_000;
    private static final int PAGE_ROWS = 10;
    private static final int BATCH_SIZE = 200;
    private static final int THREADS = 4;
    private static final int PASSES = 5;
    /// Rows for the row reader, whose batches the reader sizes: enough for several of them.
    private static final int ROW_READER_ROWS = 600_000;
    private static final int ROW_READER_PAGE_ROWS = 1_000;

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void everyValueReadsAsWritten() throws Exception {
        byte[] file = write(ROWS, PAGE_ROWS);
        try (HardwoodContext context = HardwoodContext.create(THREADS)) {
            for (int pass = 0; pass < PASSES; pass++) {
                int row = 0;
                try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)), context);
                     ColumnReaders columns = reader.buildColumnReaders(ColumnProjection.all())
                             .batchSize(BATCH_SIZE)
                             .build()) {
                    ColumnReader column = columns.getColumnReader("v");
                    while (columns.nextBatch()) {
                        BinaryValues binary = column.getBinaryValues();
                        for (int i = 0; i < column.getRecordCount(); i++) {
                            String value = window(binary, i);
                            assertThat(value).as("pass %d, row %d", pass, row).isEqualTo(value(row));
                            row++;
                        }
                    }
                }
                assertThat(row).as("rows of pass %d", pass).isEqualTo(ROWS);
            }
        }
    }

    /// The same through the row reader, whose recycled holders keep their chunks and fill them
    /// again from batch to batch.
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void everyValueReadsAsWrittenThroughTheRowReader() throws Exception {
        byte[] file = write(ROW_READER_ROWS, ROW_READER_PAGE_ROWS);
        try (HardwoodContext context = HardwoodContext.create(THREADS)) {
            for (int pass = 0; pass < PASSES; pass++) {
                int row = 0;
                try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)), context);
                     RowReader rows = reader.rowReader()) {
                    while (rows.hasNext()) {
                        rows.next();
                        assertThat(rows.getString("v")).as("pass %d, row %d", pass, row).isEqualTo(value(row));
                        row++;
                    }
                }
                assertThat(row).as("rows of pass %d", pass).isEqualTo(ROW_READER_ROWS);
            }
        }
    }

    /// A value of its own length per row, so that bytes written from another page or another
    /// offset do not read as the row's value.
    private static String value(int row) {
        return "row-" + row + "-" + "x".repeat(row % 13);
    }

    /// `rows` rows of one required `BYTE_ARRAY` column `v`, `PLAIN`-encoded, GZIP-compressed, in
    /// pages of `pageRows` rows.
    private static byte[] write(int rows, int pageRows) throws Exception {
        FileSchema schema = FileSchema.builder("binary")
                .addColumn("v", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED)
                .build();
        byte[][] values = new byte[rows][];
        for (int i = 0; i < rows; i++) {
            values[i] = value(i).getBytes(StandardCharsets.UTF_8);
        }
        WriterConfig config = WriterConfig.builder()
                .encoding(ColumnEncoding.PLAIN)
                .codec(CompressionCodec.GZIP)
                .pageTargetRows(pageRows)
                .build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, config)) {
            writer.columnWriter().writeBatch(batch -> batch.bytes("v", values));
        }
        return InMemoryFiles.toByteArray(out);
    }

    private static String window(BinaryValues values, int i) {
        return new String(values.buffer(i), values.start(i), values.end(i) - values.start(i), StandardCharsets.UTF_8);
    }
}
