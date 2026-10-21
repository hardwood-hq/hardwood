/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.ByteBuffer;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Timeout;

import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A `HardwoodContext` closed while a reader using it is reading fails that read, and the reader
/// still closes: the tasks its pool had queued run instead of being dropped, so no column waits
/// for a task that never runs.
class ContextClosedMidReadTest {

    /// More rows than a read decodes ahead of its consumer, so that the read needs the pool after
    /// its first batch. A dictionary-encoded column would not do: it costs the read-ahead budget
    /// next to nothing per row, and on one CPU its whole file is decoded before the first row.
    private static final int ROWS = 1_000_000;
    private static final int BATCH_SIZE = 1_024;

    @RepeatedTest(5)
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aRowReadOnAClosedContextFailsAndItsReaderCloses() throws Exception {
        HardwoodContext context = HardwoodContext.create(1);
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(longsFile())), context);
             RowReader rows = reader.rowReader()) {
            rows.next();
            context.close();

            assertThatThrownBy(() -> {
                while (rows.hasNext()) {
                    rows.next();
                }
            })
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("The HardwoodContext was closed while a reader using it was reading")
                    .cause()
                    .isInstanceOf(RejectedExecutionException.class);
        }
    }

    /// The same of a column read, whose fixed-width columns a read's scheduler submits.
    @RepeatedTest(5)
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aColumnReadOnAClosedContextFailsAndItsReadersClose() throws Exception {
        HardwoodContext context = HardwoodContext.create(1);
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(longsFile())), context);
             ColumnReaders columns = reader.buildColumnReaders(ColumnProjection.columns("a", "b"))
                     .batchSize(BATCH_SIZE)
                     .build()) {
            columns.nextBatch();
            context.close();

            assertThatThrownBy(() -> {
                while (columns.nextBatch()) {
                    // reads on
                }
            })
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("The HardwoodContext was closed while a reader using it was reading")
                    .cause()
                    .isInstanceOf(RejectedExecutionException.class);
        }
    }

    /// Two required `INT64` columns of `ROWS` rows.
    private static byte[] longsFile() throws Exception {
        FileSchema schema = FileSchema.builder("longs")
                .addColumn("a", PhysicalType.INT64, RepetitionType.REQUIRED)
                .addColumn("b", PhysicalType.INT64, RepetitionType.REQUIRED)
                .build();
        long[] values = new long[ROWS];
        for (int i = 0; i < ROWS; i++) {
            values[i] = i;
        }
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(batch -> {
                batch.longs("a", values);
                batch.longs("b", values);
            });
        }
        return InMemoryFiles.toByteArray(out);
    }
}
