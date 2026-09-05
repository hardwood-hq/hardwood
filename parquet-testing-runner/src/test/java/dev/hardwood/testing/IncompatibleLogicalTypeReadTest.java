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

/// End-to-end coverage for a logical type its physical type cannot carry, reading the
/// `int32_with_uuid_logical_type.parquet` fixture from apache/parquet-testing: a `UUID`, which
/// the format defines over `FIXED_LEN_BYTE_ARRAY(16)`, annotating an `INT32`.
///
/// The annotation is not a value: the column's data is ten `INT32`s and reads back as such. So
/// the file opens, the footer is reported as it stands — annotation included, because dropping it
/// would misreport what the file says — and the physical values are readable.
///
/// [ParquetComparisonTest] skips the fixture: parquet-java rejects the pairing while parsing the
/// footer ("UUID can only annotate FIXED_LEN_BYTE_ARRAY(16)") and so never opens the file, which
/// leaves no reference reader to compare against. This test is the coverage that replaces it.
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class IncompatibleLogicalTypeReadTest {

    private static final String COLUMN = "int32_uuid";

    private final List<Integer> values = new ArrayList<>();
    private FileSchema schema;

    @BeforeAll
    void readAllRows() throws IOException {
        Path file = ParquetTestingRepoCloner.getTestFile("data/int32_with_uuid_logical_type.parquet");
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
                RowReader rowReader = reader.rowReader()) {

            schema = reader.getFileSchema();
            while (rowReader.hasNext()) {
                rowReader.next();
                values.add(rowReader.getInt(COLUMN));
            }
        }
    }

    @Test
    void theSchemaIsReportedAsTheFooterStatesIt() {
        assertThat(schema.getColumn(COLUMN).type()).isEqualTo(PhysicalType.INT32);
        assertThat(schema.getColumn(COLUMN).logicalType()).isInstanceOf(LogicalType.UuidType.class);
    }

    @Test
    void thePhysicalValuesReadBack() {
        assertThat(values).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }
}
