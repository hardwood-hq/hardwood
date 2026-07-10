/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.testing;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import dev.hardwood.InputFile;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// End-to-end coverage for an unrecognized logical type, reading the canonical
/// `unknown-logical-type.parquet` fixture from apache/parquet-testing. A column annotated with a
/// logical type Hardwood does not recognize must still open, expose its BYTE_ARRAY physical type
/// with a null logical annotation, and read its raw values back, while a recognized annotation on
/// a sibling column is unaffected. Thrift-level decoding of the unknown union member is covered by
/// `LogicalTypeReaderTest` / `SchemaElementReaderTest` in the core module.
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class UnknownLogicalTypeReadTest {

    private static final String KNOWN_COLUMN = "column with known type";
    private static final String UNKNOWN_COLUMN = "column with unknown type";

    private final List<String> knownValues = new ArrayList<>();
    private final List<byte[]> unknownValues = new ArrayList<>();

    @BeforeAll
    void readAllRows() throws IOException {
        Path file = ParquetTestingRepoCloner.getTestFile("data/unknown-logical-type.parquet");
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
             RowReader rowReader = reader.rowReader()) {

            FileSchema schema = reader.getFileSchema();
            // The unrecognized annotation is dropped: the physical type surfaces, logical type is null.
            assertThat(schema.getColumn(UNKNOWN_COLUMN).type()).isEqualTo(PhysicalType.BYTE_ARRAY);
            assertThat(schema.getColumn(UNKNOWN_COLUMN).logicalType()).isNull();
            // A recognized annotation on a sibling column is unaffected.
            assertThat(schema.getColumn(KNOWN_COLUMN).type()).isEqualTo(PhysicalType.BYTE_ARRAY);
            assertThat(schema.getColumn(KNOWN_COLUMN).logicalType())
                    .isInstanceOf(LogicalType.StringType.class);

            while (rowReader.hasNext()) {
                rowReader.next();
                knownValues.add(rowReader.getString(KNOWN_COLUMN));
                unknownValues.add(rowReader.getBinary(UNKNOWN_COLUMN));
            }
        }
    }

    @Test
    void knownColumnDecodesAsString() {
        assertThat(knownValues)
                .containsExactly("known string 1", "known string 2", "known string 3");
    }

    @Test
    void unknownColumnExposesRawPhysicalBytes() {
        List<String> asText = unknownValues.stream()
                .map(bytes -> new String(bytes, StandardCharsets.UTF_8))
                .toList();
        assertThat(asText)
                .containsExactly("unknown string 1", "unknown string 2", "unknown string 3");
    }
}
