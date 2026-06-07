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

import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;

/// Tests for DELTA_BYTE_ARRAY encoding on FIXED_LEN_BYTE_ARRAY columns
class DeltaByteArrayFlbaTest {

    private static final Path FILE =
            Paths.get("src/test/resources/delta_byte_array_flba_test.parquet");

    // Source values from simple-datagen.py (seed 597)
    private static final byte[][] EXPECTED_REQUIRED = {
        new byte[]{(byte)0xAA, (byte)0xBB, 0x01, 0x00},
        new byte[]{(byte)0xAA, (byte)0xBB, 0x01, 0x01},
        new byte[]{(byte)0xAA, (byte)0xBB, 0x02, 0x00},
        new byte[]{(byte)0xAA, (byte)0xCC, 0x00, 0x00},
        new byte[]{(byte)0xAA, (byte)0xCC, 0x00, (byte)0xFF},
        new byte[]{(byte)0xAA, (byte)0xCC, 0x01, 0x00},
        new byte[]{(byte)0xFF, 0x00, 0x00, 0x00},
        new byte[]{(byte)0xFF, 0x00, 0x00, 0x01},
        new byte[]{(byte)0xFF, 0x00, 0x01, 0x00},
        new byte[]{(byte)0xFF, (byte)0xFF, 0x00, 0x00},
    };

    @Test
    void tagReqAndTagOptColumnsUsesDeltaByteArrayEncoding() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE))) {
            assertThat(reader.getFileMetaData().numRows()).isEqualTo(10);

            RowGroup rg = reader.getFileMetaData().rowGroups().get(0);
            ColumnChunk reqCol = rg.columns().get(1); // tag_req
            ColumnChunk optCol = rg.columns().get(2); // tag_opt

            assertThat(reqCol.metaData().encodings())
                    .as("tag_req must use DELTA_BYTE_ARRAY")
                    .contains(Encoding.DELTA_BYTE_ARRAY);
            assertThat(optCol.metaData().encodings())
                    .as("tag_opt must use DELTA_BYTE_ARRAY")
                    .contains(Encoding.DELTA_BYTE_ARRAY);
        }
    }

    @Test
    void requiredFlbaColumnDecodesAllValues() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE));
             RowReader rows = reader.rowReader()) {

            int i = 0;
            while (rows.hasNext()) {
                rows.next();
                assertThat(rows.getInt("id")).isEqualTo(i + 1);
                assertThat(rows.getBinary("tag_req"))
                        .as("tag_req at row %d", i)
                        .isEqualTo(EXPECTED_REQUIRED[i]);
                i++;
            }
            assertThat(i).isEqualTo(10);
        }
    }

    @Test
    void optionalFlbaColumnDecodesNullAndNonNullValues() throws Exception {
        // Nulls on rows where (rowIndex % 3 == 0), i.e. rows 0, 3, 6, 9
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FILE));
             RowReader rows = reader.rowReader()) {

            int i = 0;
            while (rows.hasNext()) {
                rows.next();
                if (i % 3 == 0) {
                    assertThat(rows.isNull("tag_opt"))
                            .as("tag_opt should be null at row %d", i)
                            .isTrue();
                } else {
                    // Non-null tag_opt rows carry the same bytes as tag_req (the
                    // generator reuses the required values), so reuse the expectations.
                    assertThat(rows.getBinary("tag_opt"))
                            .as("tag_opt at row %d", i)
                            .isEqualTo(EXPECTED_REQUIRED[i]);
                }
                i++;
            }
            assertThat(i).isEqualTo(10);
        }
    }
}
