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

/// End-to-end coverage for the JSON logical type: confirms the column schema reports
/// [LogicalType.JsonType] and that `getString` returns the raw UTF-8 JSON payload.
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class JsonLogicalTypeTest {

    private static final Path FILE = Paths.get("src/test/resources/logical_types_test.parquet");
    private static final String COLUMN = "profile_json";

    private final List<String> values = new ArrayList<>();

    @BeforeAll
    void readAllRows() throws IOException {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(FILE));
             RowReader rowReader = fileReader.createRowReader()) {
            while (rowReader.hasNext()) {
                rowReader.next();
                values.add(rowReader.getString(COLUMN));
            }
        }
    }

    @Test
    void schemaReportsJsonLogicalTypeOnByteArray() throws IOException {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(FILE))) {
            ColumnSchema column = fileReader.getFileSchema().getColumn(COLUMN);
            assertThat(column.type()).isEqualTo(PhysicalType.BYTE_ARRAY);
            assertThat(column.logicalType()).isInstanceOf(LogicalType.JsonType.class);
        }
    }

    static Stream<Arguments> jsonRows() {
        return Stream.of(
                Arguments.of(0, "{\"role\":\"admin\",\"tags\":[\"x\",\"y\"]}"),
                Arguments.of(1, "{\"role\":\"user\",\"active\":true}"),
                Arguments.of(2, "{\"nested\":{\"k\":1,\"v\":[1,2,3]}}"));
    }

    @ParameterizedTest(name = "row {0} -> {1}")
    @MethodSource("jsonRows")
    void getStringReturnsJsonPayload(int rowIndex, String expected) {
        assertThat(values.get(rowIndex)).isEqualTo(expected);
    }
}
