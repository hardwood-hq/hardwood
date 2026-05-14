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

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnSchema;

import static org.assertj.core.api.Assertions.assertThat;

class NullLogicalTypeTest {

    private static final Path FILE = Paths.get("src/test/resources/null_logical_type_test.parquet");

    @Test
    void testSchemaReportsNullLogicalType() throws IOException {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(FILE))) {
            ColumnSchema column = fileReader.getFileSchema().getColumn("nothing");
            assertThat(column.logicalType()).isInstanceOf(LogicalType.NullType.class);
        }
    }

    @Test
    void testEveryRowReadsAsNull() throws IOException {
        try (ParquetFileReader fileReader = ParquetFileReader.open(InputFile.of(FILE));
             RowReader rowReader = fileReader.rowReader()) {
            int rows = 0;
            while (rowReader.hasNext()) {
                rowReader.next();
                assertThat(rowReader.isNull("nothing")).isTrue();
                rows++;
            }
            assertThat(rows).isEqualTo(3);
        }
    }
}
