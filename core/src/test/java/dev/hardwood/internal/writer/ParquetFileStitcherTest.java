/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ParquetFileStitcherTest {
    @Test
    void stitchesBodiesAndRowGroupsWithoutReencoding() throws Exception {
        byte[] first = file(schema("id"), 1, 2, 3);
        byte[] second = file(schema("id"), 4, 5);
        ByteBufferOutputFile output = new ByteBufferOutputFile();

        ParquetFileStitcher.stitch(List.of(
                InputFile.of(ByteBuffer.wrap(first)), InputFile.of(ByteBuffer.wrap(second))), output);

        byte[] merged = output.toByteArray();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(merged)))) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(5);
            assertThat(reader.getFileMetaData().rowGroups()).hasSize(2);
            assertThat(readInts(reader)).containsExactly(1, 2, 3, 4, 5);
        }
        int firstFooter = footerStart(first);
        int secondFooter = footerStart(second);
        assertThat(Arrays.copyOfRange(merged, 4, firstFooter)).isEqualTo(Arrays.copyOfRange(first, 4, firstFooter));
        assertThat(Arrays.copyOfRange(merged, firstFooter, firstFooter + secondFooter - 4))
                .isEqualTo(Arrays.copyOfRange(second, 4, secondFooter));
    }

    @Test
    void rejectsMismatchedSchemasBeforeCreatingOutput() throws Exception {
        ByteBufferOutputFile output = new ByteBufferOutputFile();
        assertThatThrownBy(() -> ParquetFileStitcher.stitch(List.of(
                InputFile.of(ByteBuffer.wrap(file(schema("a"), 1))),
                InputFile.of(ByteBuffer.wrap(file(schema("b"), 2)))), output))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Schema mismatch");
    }

    private static FileSchema schema(String name) {
        return FileSchema.builder("schema")
                .addColumn(name, PhysicalType.INT32, RepetitionType.REQUIRED)
                .build();
    }

    private static byte[] file(FileSchema schema, int... values) throws Exception {
        ByteBufferOutputFile output = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(output, schema)) {
            writer.writeBatch(batch -> batch.ints(0, values));
        }
        return output.toByteArray();
    }

    private static List<Integer> readInts(ParquetFileReader reader) {
        List<Integer> values = new ArrayList<>();
        try (ColumnReader column = reader.columnReader(0)) {
            while (column.nextBatch()) {
                for (int i = 0; i < column.getRecordCount(); i++) {
                    values.add(column.getInts()[i]);
                }
            }
        }
        return values;
    }

    private static int footerStart(byte[] file) {
        return file.length - 8 - ByteBuffer.wrap(file, file.length - 8, 4)
                .order(java.nio.ByteOrder.LITTLE_ENDIAN).getInt();
    }
}
