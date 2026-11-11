/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.BinaryValues;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ColumnEncoding;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;

/// A batch whose rows reach further than the read's decode look-ahead still completes.
///
/// Batch sizing assumes 16 bytes per string, so one batch here takes every row; the look-ahead
/// counts the strings at their measured width, eight batch budgets of 16 KB values, so it ends
/// well inside that one batch. The consumer needs the whole batch, so its pages decode past the
/// look-ahead.
class LongBatchLookAheadTest {

    private static final int ROWS = 4000;
    private static final int WIDTH = 16 * 1024;

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void aBatchLongerThanTheLookAheadCompletes() throws Exception {
        byte[] file = writeWideStrings();

        long rows = 0;
        long bytes = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)));
             ColumnReader col = reader.columnReader("s")) {
            while (col.nextBatch()) {
                BinaryValues values = col.getBinaryValues();
                for (int i = 0; i < col.getValueCount(); i++) {
                    bytes += values.length(i);
                }
                rows += col.getValueCount();
            }
        }

        assertThat(rows).isEqualTo(ROWS);
        assertThat(bytes).isEqualTo((long) ROWS * WIDTH);
    }

    /// One row group of `ROWS` PLAIN strings of `WIDTH` bytes each, compressible so the file
    /// stays small.
    private static byte[] writeWideStrings() throws Exception {
        FileSchema schema = FileSchema.builder("wide")
                .addColumn("s", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED, new LogicalType.StringType())
                .build();
        byte[][] values = new byte[ROWS][];
        for (int i = 0; i < ROWS; i++) {
            values[i] = new byte[WIDTH];
            Arrays.fill(values[i], (byte) ('a' + i % 26));
        }
        WriterConfig config = WriterConfig.builder().encoding(ColumnEncoding.PLAIN).build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, config)) {
            writer.columnWriter().writeBatch(batch -> batch.bytes("s", values));
        }
        return InMemoryFiles.toByteArray(out);
    }
}
