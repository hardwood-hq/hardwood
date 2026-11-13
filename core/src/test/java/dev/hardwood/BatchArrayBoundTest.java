/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.reader.plan.BatchSizing;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;

import static org.assertj.core.api.Assertions.assertThat;

/// Every array a batch hands out stays within `BatchSizing.MAX_ARRAY_BYTES`, half of G1's smallest
/// region less the array header, so that no batch allocates a humongous object on a heap below
/// 4 GB (#1497). The reads use the default batch size, on columns whose byte budget alone would
/// size the arrays above the bound.
class BatchArrayBoundTest {

    private static final int ROWS = 300_000;
    private static final int LIST_ROWS = 4_000;
    private static final int FAN_OUT = 64;

    @Test
    void aNarrowReadOfEightByteValuesKeepsItsArraysWithinTheBound() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("l", PhysicalType.INT64, RepetitionType.REQUIRED)
                .addColumn("d", PhysicalType.DOUBLE, RepetitionType.OPTIONAL)
                .build();
        long[] longs = new long[ROWS];
        double[] doubles = new double[ROWS];
        for (int i = 0; i < ROWS; i++) {
            longs[i] = i;
            doubles[i] = i;
        }
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(batch -> batch.longs("l", longs).doubles("d", doubles));
        }

        try (ParquetFileReader reader = open(out); ColumnReader column = reader.columnReader("l")) {
            long rows = 0;
            while (column.nextBatch()) {
                rows += column.getRecordCount();
                assertWithinBound(column.getLongs().length, Long.BYTES);
            }
            assertThat(rows).isEqualTo(ROWS);
        }
        try (ParquetFileReader reader = open(out); ColumnReader column = reader.columnReader("d")) {
            long rows = 0;
            while (column.nextBatch()) {
                rows += column.getRecordCount();
                assertWithinBound(column.getDoubles().length, Double.BYTES);
            }
            assertThat(rows).isEqualTo(ROWS);
        }
    }

    @Test
    void aListReadKeepsItsValuesAndOffsetsWithinTheBound() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .list("v", RepetitionType.REQUIRED,
                        el -> el.primitive(PhysicalType.FLOAT, RepetitionType.REQUIRED))
                .build();
        int[] offsets = new int[LIST_ROWS + 1];
        for (int i = 0; i <= LIST_ROWS; i++) {
            offsets[i] = i * FAN_OUT;
        }
        float[] elements = new float[LIST_ROWS * FAN_OUT];
        for (int i = 0; i < elements.length; i++) {
            elements[i] = i;
        }
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(batch -> batch.list("v", offsets).floats("v.list.element", elements));
        }

        try (ParquetFileReader reader = open(out); ColumnReader column = reader.columnReader("v.list.element")) {
            long values = 0;
            while (column.nextBatch()) {
                values += column.getValueCount();
                assertWithinBound(column.getFloats().length, Float.BYTES);
                assertWithinBound(column.getLayerOffsets(0).length, Integer.BYTES);
            }
            assertThat(values).isEqualTo((long) LIST_ROWS * FAN_OUT);
        }
    }

    private static ParquetFileReader open(InMemoryOutputFile out) throws Exception {
        return ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(InMemoryFiles.toByteArray(out))));
    }

    private static void assertWithinBound(int length, int elementBytes) {
        assertThat((long) length * elementBytes).isLessThanOrEqualTo(BatchSizing.MAX_ARRAY_BYTES);
    }
}
