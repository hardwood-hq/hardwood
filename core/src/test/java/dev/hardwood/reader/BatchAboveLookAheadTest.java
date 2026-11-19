/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.hardwood.HardwoodContext;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ColumnEncoding;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;

/// A batch larger than the read's look-ahead, of a column with repetition whose pages start where
/// the batches end, its values plain-encoded so that the look-ahead counts their bytes. Such a column closes a batch only as it places the record after it, so the
/// decode of the page starting at the batch's end is part of the work that completes the batch;
/// the look-ahead, which ends there, refuses it, and the read must still advance it as the
/// consumer's need.
class BatchAboveLookAheadTest {

    private static final int ROWS = 1800;
    private static final int BATCH_SIZE = 600;
    /// bytes per value: the look-ahead, eight byte budgets of the decoded rows, holds about 500
    /// rows, fewer than a batch
    private static final int VALUE_BYTES = 100_000;

    @TempDir
    static Path dir;

    private static Path file;

    @BeforeAll
    static void writeFile() throws Exception {
        file = dir.resolve("batch-above-look-ahead.parquet");
        FileSchema schema = FileSchema.builder("schema")
                .primitive("id", PhysicalType.INT64, RepetitionType.REQUIRED)
                .list("l", RepetitionType.REQUIRED, el -> el.primitive(PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED))
                .build();
        long[] ids = new long[ROWS];
        int[] offsets = new int[ROWS + 1];
        byte[][] values = new byte[ROWS][];
        byte[] value = new byte[VALUE_BYTES];
        Arrays.fill(value, (byte) 'a');
        for (int i = 0; i < ROWS; i++) {
            ids[i] = i;
            offsets[i + 1] = i + 1;
            values[i] = value;
        }
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(file), schema,
                WriterConfig.builder()
                        .pageTargetRows(100)
                        .pageTargetBytes(64 << 20)
                        .encoding("l.list.element", ColumnEncoding.PLAIN)
                        .build())) {
            writer.columnWriter().writeBatch(batch -> batch
                    .longs("id", ids)
                    .list("l", offsets)
                    .bytes("l.list.element", values));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "l", "id,l" })
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void readsEveryRow(String columns) throws Exception {
        long rows = 0;
        try (HardwoodContext context = HardwoodContext.create();
                ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file), context);
                ColumnReaders readers = reader.buildColumnReaders(ColumnProjection.columns(columns.split(",")))
                        .batchSize(BATCH_SIZE)
                        .build()) {
            while (readers.nextBatch()) {
                rows += readers.getRecordCount();
            }
        }
        assertThat(rows).isEqualTo(ROWS);
    }
}
