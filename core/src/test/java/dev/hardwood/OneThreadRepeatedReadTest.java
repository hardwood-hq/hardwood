/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.BinaryValues;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;

import static org.assertj.core.api.Assertions.assertThat;

/// A read of columns with repetition completes on a one-thread context when its batches end
/// exactly where a row group ends. A cell of such a column is sealed only by the page holding the
/// next record start, which then starts exactly one batch capacity past the column's open cell.
class OneThreadRepeatedReadTest {

    private static final int ROWS_PER_ROW_GROUP = 1_000;
    private static final int ROW_GROUPS = 2;
    private static final int K = 3;

    @ParameterizedTest
    @ValueSource(ints = { 50, 100, 250, 1_000 })
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aBatchSizeDividingTheRowGroupsReadsEveryValue(int batchSize) throws Exception {
        byte[] file = write();
        long rows = 0;
        long intSum = 0;
        long stringBytes = 0;
        try (HardwoodContext context = HardwoodContext.create(1);
             ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)), context);
             ColumnReaders columns = reader.buildColumnReaders(ColumnProjection.all()).batchSize(batchSize).build()) {
            ColumnReader ints = columns.getColumnReader("i.list.element");
            ColumnReader strings = columns.getColumnReader("s.list.element");
            while (columns.nextBatch()) {
                rows += columns.getRecordCount();
                int[] values = ints.getInts();
                for (int v = 0; v < ints.getValueCount(); v++) {
                    intSum += values[v];
                }
                BinaryValues binary = strings.getBinaryValues();
                for (int v = 0; v < binary.size(); v++) {
                    stringBytes += binary.length(v);
                }
            }
        }

        long values = (long) ROW_GROUPS * ROWS_PER_ROW_GROUP * K;
        assertThat(rows).isEqualTo((long) ROW_GROUPS * ROWS_PER_ROW_GROUP);
        assertThat(intSum).isEqualTo(values * (values - 1) / 2);
        assertThat(stringBytes).isEqualTo(values);
    }

    /// Two row groups of `REQUIRED list<REQUIRED int32>` and `REQUIRED list<REQUIRED binary>`,
    /// each row holding `K` elements.
    private static byte[] write() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .list("i", RepetitionType.REQUIRED,
                        el -> el.primitive(PhysicalType.INT32, RepetitionType.REQUIRED))
                .list("s", RepetitionType.REQUIRED,
                        el -> el.primitive(PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED))
                .build();
        int[] offsets = new int[ROWS_PER_ROW_GROUP + 1];
        for (int i = 0; i <= ROWS_PER_ROW_GROUP; i++) {
            offsets[i] = i * K;
        }
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            for (int rowGroup = 0; rowGroup < ROW_GROUPS; rowGroup++) {
                int[] elements = new int[ROWS_PER_ROW_GROUP * K];
                byte[][] strings = new byte[elements.length][];
                for (int i = 0; i < elements.length; i++) {
                    elements[i] = rowGroup * elements.length + i;
                    strings[i] = "x".getBytes(StandardCharsets.UTF_8);
                }
                writer.columnWriter().writeBatch(batch -> batch
                        .list("i", offsets)
                        .ints("i.list.element", elements)
                        .list("s", offsets)
                        .bytes("s.list.element", strings));
                writer.endRowGroup();
            }
        }
        return InMemoryFiles.toByteArray(out);
    }
}
