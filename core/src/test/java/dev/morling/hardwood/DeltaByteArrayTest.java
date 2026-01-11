/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

import dev.morling.hardwood.metadata.Encoding;
import dev.morling.hardwood.metadata.RowGroup;
import dev.morling.hardwood.reader.ParquetFileReader;
import dev.morling.hardwood.row.PqRow;
import dev.morling.hardwood.row.PqType;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for DELTA_BYTE_ARRAY encoding support.
 */
class DeltaByteArrayTest {

    @Test
    void testDeltaByteArray() throws Exception {
        Path file = Paths.get("src/test/resources/delta_byte_array_test.parquet");

        try (ParquetFileReader reader = ParquetFileReader.open(file)) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(8);

            // Verify string columns use DELTA_BYTE_ARRAY encoding
            RowGroup rowGroup = reader.getFileMetaData().rowGroups().get(0);
            var prefixColumn = rowGroup.columns().get(1); // prefix_strings is column 1
            var varyingColumn = rowGroup.columns().get(2); // varying_strings is column 2
            assertThat(prefixColumn.metaData().encodings())
                    .as("Column 'prefix_strings' should use DELTA_BYTE_ARRAY encoding")
                    .contains(Encoding.DELTA_BYTE_ARRAY);
            assertThat(varyingColumn.metaData().encodings())
                    .as("Column 'varying_strings' should use DELTA_BYTE_ARRAY encoding")
                    .contains(Encoding.DELTA_BYTE_ARRAY);

            String[] expectedPrefixStrings = {
                    "apple",
                    "application",
                    "apply",
                    "banana",
                    "bandana",
                    "band",
                    "bandwidth",
                    "ban"
            };

            String[] expectedVaryingStrings = {
                    "hello",
                    "world",
                    "wonderful",
                    "wonder",
                    "wander",
                    "wandering",
                    "test",
                    "testing"
            };

            int rowIndex = 0;
            for (PqRow row : reader.createRowReader()) {
                // id column (INT32)
                Integer id = row.getValue(PqType.INT32, "id");
                assertThat(id).isEqualTo(rowIndex + 1);

                // prefix_strings column (DELTA_BYTE_ARRAY)
                String prefixString = row.getValue(PqType.STRING, "prefix_strings");
                assertThat(prefixString).isEqualTo(expectedPrefixStrings[rowIndex]);

                // varying_strings column (DELTA_BYTE_ARRAY)
                String varyingString = row.getValue(PqType.STRING, "varying_strings");
                assertThat(varyingString).isEqualTo(expectedVaryingStrings[rowIndex]);

                rowIndex++;
            }
            assertThat(rowIndex).isEqualTo(8);
        }
    }
}
