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

import dev.hardwood.internal.writer.ByteBufferOutputFile;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;

import static org.assertj.core.api.Assertions.assertThat;

/// Regression test for #748. A wide repeated column expands each row into many
/// leaf values, so a value array sized by row count alone is the list's fan-out
/// times larger than the byte budget. When the row count fits under
/// `BatchSizing.MAX_BATCH`, the fan-out-blind sizing drained the whole column
/// into a single batch — value and level arrays scaled by the fan-out — which is
/// memory-bound to assemble and scan and, at higher fan-out, exhausted the heap.
///
/// Fan-out-aware sizing must bound each batch by value memory, so a wide list is
/// split across several batches rather than materialised in one.
class WideListBatchBoundTest {

    private static final int K = 512;      // leaf values per row (list fan-out)
    private static final int ROWS = 4000;  // < MAX_BATCH rows, but ROWS * K = 2.05M values >> the byte budget
    private static final String LEAF = "v.list.element";

    @Test
    void wideListIsNotDrainedIntoOneOversizedBatch() throws Exception {
        byte[] file = writeFixedWidthListOfInts();

        long totalValues = 0;
        long maxBatchValues = 0;
        int batches = 0;
        try (ParquetFileReader reader =
                        ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)));
                // Default (auto) batch size — the path #748 fixed.
                ColumnReader col = reader.columnReader(LEAF)) {
            while (col.nextBatch()) {
                batches++;
                int values = col.getValueCount();
                totalValues += values;
                maxBatchValues = Math.max(maxBatchValues, values);
            }
        }

        // Every value is read back...
        assertThat(totalValues).isEqualTo((long) ROWS * K);
        // ...but the column is split by value budget, not handed over in one batch
        // whose value array is the fan-out times the intended footprint.
        assertThat(batches).isGreaterThan(1);
        assertThat(maxBatchValues).isLessThan((long) ROWS * K);
    }

    /// A `REQUIRED list<REQUIRED int32>` of exactly `K` present elements per row,
    /// written as one row group.
    private static byte[] writeFixedWidthListOfInts() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .list("v", RepetitionType.REQUIRED,
                        el -> el.primitive(PhysicalType.INT32, RepetitionType.REQUIRED))
                .build();

        int[] offsets = new int[ROWS + 1];
        for (int i = 0; i <= ROWS; i++) {
            offsets[i] = i * K;
        }
        int[] elements = new int[ROWS * K];
        for (int i = 0; i < elements.length; i++) {
            elements[i] = i;
        }

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.writeBatch(batch -> batch
                    .list("v", offsets)
                    .ints(LEAF, elements));
        }
        return out.toByteArray();
    }
}
