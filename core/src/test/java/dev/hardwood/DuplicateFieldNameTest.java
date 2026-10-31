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
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqStruct;

import static org.assertj.core.api.Assertions.assertThat;

/// Sibling fields of one name, which Parquet does not forbid: a row reader reaches each of them by
/// index, and the last of them by name. The fixtures give the siblings different values, so a
/// lookup that reached the first would read another value.
class DuplicateFieldNameTest {

    @Test
    void flatRowReaderResolvesARepeatedNameToTheLastColumn() throws Exception {
        try (ParquetFileReader reader = open("duplicate_column_names.parquet");
             RowReader rows = reader.rowReader()) {
            assertThat(rows.hasNext()).isTrue();
            rows.next();

            assertThat(rows.getLong(0)).isEqualTo(1L);
            assertThat(rows.getLong(1)).isEqualTo(10L);
            assertThat(rows.getLong("id")).isEqualTo(10L);
            assertThat(rows.isNull("id")).isFalse();
        }
    }

    @Test
    void nestedRowReaderResolvesARepeatedNameToTheLastField() throws Exception {
        try (ParquetFileReader reader = open("duplicate_field_names_nested.parquet");
             RowReader rows = reader.rowReader()) {
            assertThat(rows.hasNext()).isTrue();
            rows.next();

            assertThat(rows.getLong(0)).isEqualTo(1L);
            assertThat(rows.getLong(1)).isEqualTo(100L);
            assertThat(rows.getLong("id")).isEqualTo(100L);
        }
    }

    @Test
    void structResolvesARepeatedChildNameToTheLastChild() throws Exception {
        try (ParquetFileReader reader = open("duplicate_field_names_nested.parquet");
             RowReader rows = reader.rowReader()) {
            assertThat(rows.hasNext()).isTrue();
            rows.next();
            PqStruct s = rows.getStruct("s");

            assertThat(s.getInt(0)).isEqualTo(3);
            assertThat(s.getInt(1)).isEqualTo(30);
            assertThat(s.getInt("v")).isEqualTo(30);
        }
    }

    private static ParquetFileReader open(String fileName) throws Exception {
        return ParquetFileReader.open(InputFile.of(Paths.get("src/test/resources/" + fileName)));
    }
}
