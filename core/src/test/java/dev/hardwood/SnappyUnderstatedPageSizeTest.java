/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Reads a mapped Snappy file whose page header declares half of what its Snappy stream holds.
/// The page is decompressed natively into a direct buffer sized from the header, which bounds
/// nothing, so the stream's own length is checked first and the read fails with an exception
/// rather than a write past the buffer.
class SnappyUnderstatedPageSizeTest {

    @Test
    void aPageDeclaringLessThanItsSnappyStreamHoldsFails() {
        InputFile file = InputFile.of(Paths.get("src/test/resources/snappy_understated_page_size.parquet"));

        assertThatThrownBy(() -> {
            try (ParquetFileReader reader = ParquetFileReader.open(file);
                 RowReader rows = reader.rowReader()) {
                while (rows.hasNext()) {
                    rows.next();
                }
            }
        })
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("[snappy_understated_page_size.parquet: row group 0, column 'id', page 0] "
                        + "Snappy decompression size mismatch: expected 80000, got 160000");
    }
}
