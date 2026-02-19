/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for delta encoding support (DELTA_BINARY_PACKED and DELTA_LENGTH_BYTE_ARRAY).
 */
class DeltaBinaryPackedTest {

    @Test
    void testDeltaBinaryPackedInt32AndInt64() throws Exception {
        Path file = Paths.get("src/test/resources/delta_binary_packed_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(file)) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(200);

            // Verify all columns use DELTA_BINARY_PACKED encoding
            RowGroup rowGroup = reader.getFileMetaData().rowGroups().get(0);
            for (int i = 0; i < reader.getFileSchema().getColumnCount(); i++) {
                var columnChunk = rowGroup.columns().get(i);
                var columnName = reader.getFileSchema().getColumn(i).name();
                assertThat(columnChunk.metaData().encodings())
                        .as("Column '%s' should use DELTA_BINARY_PACKED encoding", columnName)
                        .contains(Encoding.DELTA_BINARY_PACKED);
            }

            int rowIndex = 0;
            try (RowReader rowReader = reader.createRowReader()) {
                while (rowReader.hasNext()) {
                    rowReader.next();
                    rowIndex++;

                    // id column (INT64): 1, 2, 3, ...
                    long id = rowReader.getLong("id");
                    assertThat(id).isEqualTo(rowIndex);

                    // value_i32 column (INT32): 10, 20, 30, ... (constant delta = 10)
                    int valueI32 = rowReader.getInt("value_i32");
                    assertThat(valueI32).isEqualTo(rowIndex * 10);

                    // value_i64 column (INT64): 1, 4, 9, 16, ... (squares)
                    long valueI64 = rowReader.getLong("value_i64");
                    assertThat(valueI64).isEqualTo((long) rowIndex * rowIndex);
                }
            }
            assertThat(rowIndex).isEqualTo(200);
        }
    }

    @Test
    void testDeltaBinaryPackedOptionalColumn() throws Exception {
        Path file = Paths.get("src/test/resources/delta_binary_packed_optional_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(file)) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(100);

            // Verify all columns use DELTA_BINARY_PACKED encoding
            RowGroup rowGroup = reader.getFileMetaData().rowGroups().get(0);
            for (int i = 0; i < reader.getFileSchema().getColumnCount(); i++) {
                var columnChunk = rowGroup.columns().get(i);
                var columnName = reader.getFileSchema().getColumn(i).name();
                assertThat(columnChunk.metaData().encodings())
                        .as("Column '%s' should use DELTA_BINARY_PACKED encoding", columnName)
                        .contains(Encoding.DELTA_BINARY_PACKED);
            }

            int rowIndex = 0;
            int nullCount = 0;
            try (RowReader rowReader = reader.createRowReader()) {
                while (rowReader.hasNext()) {
                    rowReader.next();
                    rowIndex++;

                    // id column (INT32): 1, 2, 3, ...
                    int id = rowReader.getInt("id");
                    assertThat(id).isEqualTo(rowIndex);

                    // optional_value column: i*5 if i%3 != 0, else null
                    if (rowIndex % 3 == 0) {
                        assertThat(rowReader.isNull("optional_value")).isTrue();
                        nullCount++;
                    }
                    else {
                        assertThat(rowReader.getInt("optional_value")).isEqualTo(rowIndex * 5);
                    }
                }
            }
            assertThat(rowIndex).isEqualTo(100);
            assertThat(nullCount).isEqualTo(33); // 3, 6, 9, ... 99 = 33 nulls
        }
    }

    @Test
    void testDeltaLengthByteArray() throws Exception {
        Path file = Paths.get("src/test/resources/delta_length_byte_array_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(file)) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(5);

            // Verify string columns use DELTA_LENGTH_BYTE_ARRAY encoding
            RowGroup rowGroup = reader.getFileMetaData().rowGroups().get(0);
            var nameColumn = rowGroup.columns().get(1); // name is column 1
            var descColumn = rowGroup.columns().get(2); // description is column 2
            assertThat(nameColumn.metaData().encodings())
                    .as("Column 'name' should use DELTA_LENGTH_BYTE_ARRAY encoding")
                    .contains(Encoding.DELTA_LENGTH_BYTE_ARRAY);
            assertThat(descColumn.metaData().encodings())
                    .as("Column 'description' should use DELTA_LENGTH_BYTE_ARRAY encoding")
                    .contains(Encoding.DELTA_LENGTH_BYTE_ARRAY);

            String[] expectedNames = { "Hello", "World", "Foobar", "Test", "Delta" };
            String[] expectedDescriptions = { "Short", "A bit longer text", "Medium length", "Tiny", "Another string value" };

            int rowIndex = 0;
            try (RowReader rowReader = reader.createRowReader()) {
                while (rowReader.hasNext()) {
                    rowReader.next();

                    // id column (INT64)
                    long id = rowReader.getLong("id");
                    assertThat(id).isEqualTo(rowIndex + 1);

                    // name column (DELTA_LENGTH_BYTE_ARRAY)
                    String name = rowReader.getString("name");
                    assertThat(name).isEqualTo(expectedNames[rowIndex]);

                    // description column (DELTA_LENGTH_BYTE_ARRAY)
                    String description = rowReader.getString("description");
                    assertThat(description).isEqualTo(expectedDescriptions[rowIndex]);

                    rowIndex++;
                }
            }
            assertThat(rowIndex).isEqualTo(5);
        }
    }
}
