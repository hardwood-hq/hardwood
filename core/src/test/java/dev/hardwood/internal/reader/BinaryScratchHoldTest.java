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
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ColumnEncoding;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;

/// A flat binary page keeps its pooled scratch ([ScheduledFlatColumn]'s `binaryScratch`) until
/// its task and every segment of it that parked are done ([PageTask#holds]): a parked segment's
/// values are written from the page's decompressed bytes once its cell is sized, after the page's
/// task may have ended.
///
/// The column is `PLAIN`-encoded and compressed, so its values are read from the decompressed
/// bytes in the scratch. Its pages are small against the batches, so a cell spans many pages,
/// decoded on several threads, and a segment that reaches its cell before the cell's other pages
/// are measured parks. A scratch returned to the pool while a segment still parks on it is taken
/// by a later page, whose bytes the segment then writes in place of its own.
class BinaryScratchHoldTest {

    private static final int ROWS = 50_000;
    private static final int PAGE_ROWS = 10;
    private static final int BATCH_SIZE = 200;
    private static final int THREADS = 4;
    private static final int PASSES = 5;

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void everyValueReadsAsWritten() throws Exception {
        byte[] file = write();
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

    /// A value of its own length per row, so that bytes written from another page or another
    /// offset do not read as the row's value.
    private static String value(int row) {
        return "row-" + row + "-" + "x".repeat(row % 13);
    }

    /// One required `BYTE_ARRAY` column `v`, `PLAIN`-encoded, GZIP-compressed, in pages of
    /// `PAGE_ROWS` rows.
    private static byte[] write() throws Exception {
        FileSchema schema = FileSchema.builder("binary")
                .addColumn("v", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED)
                .build();
        byte[][] values = new byte[ROWS][];
        for (int i = 0; i < ROWS; i++) {
            values[i] = value(i).getBytes(StandardCharsets.UTF_8);
        }
        WriterConfig config = WriterConfig.builder()
                .encoding(ColumnEncoding.PLAIN)
                .codec(CompressionCodec.GZIP)
                .pageTargetRows(PAGE_ROWS)
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
