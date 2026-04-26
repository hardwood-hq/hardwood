/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// End-to-end coverage for the BSON logical type: confirms the column schema reports
/// [LogicalType.BsonType] and that `getBinary` returns the raw BSON bytes unchanged —
/// including payloads containing non-UTF-8 bytes that would be corrupted by a string decode.
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BsonLogicalTypeTest {

    private static final Path FILE = Paths.get("src/test/resources/logical_types_test.parquet");
    private static final String COLUMN = "bson_payload";

    private final List<byte[]> values = new ArrayList<>();

    @BeforeAll
    void readAllRows() throws IOException {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(FILE));
             RowReader rowReader = fileReader.createRowReader()) {
            while (rowReader.hasNext()) {
                rowReader.next();
                values.add(rowReader.getBinary(COLUMN));
            }
        }
    }

    @Test
    void schemaReportsBsonLogicalTypeOnByteArray() throws IOException {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(FILE))) {
            ColumnSchema column = fileReader.getFileSchema().getColumn(COLUMN);
            assertThat(column.type()).isEqualTo(PhysicalType.BYTE_ARRAY);
            assertThat(column.logicalType()).isInstanceOf(LogicalType.BsonType.class);
        }
    }

    static Stream<Arguments> bsonRows() {
        // Hex mirrors the three BSON documents produced by tools/simple-datagen.py. Rows 1 and 2
        // contain bytes above 0x7F, which would be mangled if BSON fell through a String decode.
        return Stream.of(
                Arguments.of(0, "0500000000"),
                Arguments.of(1, "12000000026b0006000000686921800000"),
                Arguments.of(2, "0f00000005780003000000ff00fe0000"));
    }

    @ParameterizedTest(name = "row {0} -> {1}")
    @MethodSource("bsonRows")
    void getBinaryReturnsRawBsonBytes(int rowIndex, String expectedHex) {
        byte[] expected = HexFormat.of().parseHex(expectedHex);
        assertThat(values.get(rowIndex)).isEqualTo(expected);
    }
}
