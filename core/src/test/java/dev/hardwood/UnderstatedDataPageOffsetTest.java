/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.file.Paths;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqList;

import static org.assertj.core.api.Assertions.assertThat;

/// Reads files whose dictionary-encoded nested `tags` column has no OffsetIndex and a footer
/// that misstates where the first data page starts: `data_page_offset` leaving out the
/// dictionary page's header, as DuckDB wrote it before duckdb/duckdb#10829, or naming the
/// dictionary page in the absence of `dictionary_page_offset`. `tail(n)` probes the first data
/// page's format to decide whether it can skip pages, and must return the same rows a full scan
/// does.
class UnderstatedDataPageOffsetTest {

    private static final int TOTAL_ROWS = 500;

    @ParameterizedTest
    @ValueSource(strings = {
            "nested_dict_understated_offset_v1.parquet",
            "nested_dict_understated_offset_v2.parquet",
            "nested_dict_no_dict_offset_v2.parquet"
    })
    void testTailReturnsAlignedRows(String fixture) throws Exception {
        int tailRows = 200;
        InputFile file = InputFile.of(Paths.get("src/test/resources", fixture));

        try (ParquetFileReader reader = ParquetFileReader.open(file);
             RowReader rows = reader.buildRowReader().tail(tailRows).build()) {
            int expected = TOTAL_ROWS - tailRows;
            while (rows.hasNext()) {
                rows.next();
                assertThat(rows.getInt("narrow")).as("narrow").isEqualTo(expected);
                PqList tags = rows.getList("tags");
                assertThat(tags.strings()).as("tags of row %d", expected)
                        .containsExactly(String.format("row=%05d", expected), "tag-" + expected % 7);
                expected++;
            }
            assertThat(expected).as("rows read").isEqualTo(TOTAL_ROWS);
        }
    }
}
