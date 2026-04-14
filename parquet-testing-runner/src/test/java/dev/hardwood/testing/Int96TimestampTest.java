/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.testing;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.InputFile;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;

/// End-to-end reader coverage for INT96: reads a Spark-produced file via `RowReader` and
/// asserts that `getTimestamp` detects the INT96 physical type (no TIMESTAMP logical
/// annotation) and returns the decoded [Instant]. Byte-level decoding is covered by
/// `LogicalTypeConverterTest` in the core module.
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class Int96TimestampTest {

    private final List<Instant> values = new ArrayList<>();

    @BeforeAll
    void readAllRows() throws IOException {
        Path file = ParquetTestingRepoCloner.getTestFile("data/int96_from_spark.parquet");
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
             RowReader rowReader = reader.createRowReader()) {

            assertThat(reader.getFileSchema().getColumn(0).type()).isEqualTo(PhysicalType.INT96);
            assertThat(reader.getFileSchema().getColumn(0).logicalType()).isNull();

            while (rowReader.hasNext()) {
                rowReader.next();
                values.add(rowReader.getTimestamp("a"));
            }
        }
        assertThat(values).hasSize(6);
    }

    static Stream<Arguments> representableRows() {
        return Stream.of(
                Arguments.of(0, Instant.parse("2024-01-01T20:34:56.123456Z")),
                Arguments.of(1, Instant.parse("2024-01-01T01:00:00Z")),
                Arguments.of(2, Instant.parse("9999-12-31T03:00:00Z")),
                Arguments.of(3, Instant.parse("2024-12-30T23:00:00Z")));
    }

    @ParameterizedTest(name = "row {0} -> {1}")
    @MethodSource("representableRows")
    void decodesRepresentableTimestamp(int rowIndex, Instant expected) {
        assertThat(values.get(rowIndex)).isEqualTo(expected);
    }

    @Test
    void nullRowReturnsNull() {
        assertThat(values.get(4)).isNull();
    }

    @Test
    void overflowRowStillReturnsInstant() {
        // Row 5 is a year-290000 timestamp that Spark cannot faithfully encode in INT96
        // (nanos-since-epoch overflows int64); we assert only that a value is returned.
        assertThat(values.get(5)).isNotNull();
    }
}
