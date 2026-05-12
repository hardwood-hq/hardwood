/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalTime;

import org.junit.jupiter.api.Test;

import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;

import static org.assertj.core.api.Assertions.assertThat;

/// Coverage for hardwood#473 by-index field access on `PqStruct`. Reads each
/// field both by name and by its position in the projected schema and asserts
/// they return the same value.
class PqStructByIndexTest {

    private static final Path NESTED_STRUCT = Paths.get("src/test/resources/nested_struct_test.parquet");
    private static final Path WIDE_STRUCT = Paths.get("src/test/resources/wide_struct_test.parquet");

    @Test
    void pqStructByIndexMatchesByName() throws Exception {
        try (ParquetFileReader f = ParquetFileReader.open(InputFile.of(NESTED_STRUCT));
             RowReader r = f.rowReader()) {
            r.next();
            PqStruct addr = r.getStruct("address");
            assertThat(addr.getFieldCount()).isEqualTo(3);

            // Field 0 = street, 1 = city, 2 = zip (PyArrow preserves schema order)
            assertThat(addr.getFieldName(0)).isEqualTo("street");
            assertThat(addr.getFieldName(1)).isEqualTo("city");
            assertThat(addr.getFieldName(2)).isEqualTo("zip");

            assertThat(addr.getString(0)).isEqualTo(addr.getString("street"));
            assertThat(addr.getString(1)).isEqualTo(addr.getString("city"));
            assertThat(addr.getInt(2)).isEqualTo(addr.getInt("zip"));
        }
    }

    @Test
    void pqStructGetValueByIndexMatchesByName() throws Exception {
        try (ParquetFileReader f = ParquetFileReader.open(InputFile.of(NESTED_STRUCT));
             RowReader r = f.rowReader()) {
            r.next();
            PqStruct addr = r.getStruct("address");
            assertThat(addr.getValue(0)).isEqualTo(addr.getValue("street"));
            assertThat(addr.getValue(2)).isEqualTo(addr.getValue("zip"));
            assertThat(addr.getRawValue(2)).isEqualTo(addr.getRawValue("zip"));
        }
    }

    @Test
    void pqStructIsNullByIndexMatchesByName() throws Exception {
        try (ParquetFileReader f = ParquetFileReader.open(InputFile.of(NESTED_STRUCT));
             RowReader r = f.rowReader()) {
            r.next();
            PqStruct addr = r.getStruct("address");
            for (int i = 0; i < addr.getFieldCount(); i++) {
                assertThat(addr.isNull(i))
                        .as("isNull(%d) vs isNull(\"%s\")", i, addr.getFieldName(i))
                        .isEqualTo(addr.isNull(addr.getFieldName(i)));
            }
        }
    }

    @Test
    void pqStructByIndexResolvesAcrossAllRows() throws Exception {
        try (ParquetFileReader f = ParquetFileReader.open(InputFile.of(NESTED_STRUCT));
             RowReader r = f.rowReader()) {
            // Row 0
            r.next();
            PqStruct row0 = r.getStruct("address");
            assertThat(row0.getString(0)).isEqualTo("123 Main St");
            assertThat(row0.getString(1)).isEqualTo("New York");
            assertThat(row0.getInt(2)).isEqualTo(10001);

            // Row 1
            r.next();
            PqStruct row1 = r.getStruct("address");
            assertThat(row1.getString(0)).isEqualTo("456 Oak Ave");
            assertThat(row1.getString(1)).isEqualTo("Los Angeles");
            assertThat(row1.getInt(2)).isEqualTo(90001);

            // Row 2: null struct
            r.next();
            assertThat(r.getStruct("address")).isNull();
        }
    }

    @Test
    void byIndexAccessorsParallelByNameAcrossEveryFieldType() throws Exception {
        // Wide-struct fixture has one field per typed accessor on PqStruct
        // (UUID / INTERVAL / VARIANT are exercised by their own dedicated
        // test classes — see TypedAccessorsIssue445Test /
        // VariantInRepeatedAccessorsTest). Each pair below calls the typed
        // accessor by both name and index against the same field and asserts
        // the two paths agree, so the routing shared between them is covered
        // for every primitive logical type plus nested list and map.
        try (ParquetFileReader f = ParquetFileReader.open(InputFile.of(WIDE_STRUCT));
             RowReader r = f.rowReader()) {
            r.next();
            PqStruct s = r.getStruct("fields");
            assertThat(s.getFieldCount()).isEqualTo(13);

            assertThat(s.getInt(0)).isEqualTo(s.getInt("a_int")).isEqualTo(42);
            assertThat(s.getLong(1)).isEqualTo(s.getLong("b_long")).isEqualTo(12345678901234L);
            assertThat(s.getFloat(2)).isEqualTo(s.getFloat("c_float")).isEqualTo(1.5f);
            assertThat(s.getDouble(3)).isEqualTo(s.getDouble("d_double")).isEqualTo(2.5);
            assertThat(s.getBoolean(4)).isEqualTo(s.getBoolean("e_bool")).isTrue();
            assertThat(s.getString(5)).isEqualTo(s.getString("f_string")).isEqualTo("hello");
            assertThat(s.getBinary(6)).isEqualTo(s.getBinary("g_binary"));
            assertThat(s.getDate(7)).isEqualTo(s.getDate("h_date")).isEqualTo(LocalDate.of(2026, 1, 15));
            assertThat(s.getTime(8)).isEqualTo(s.getTime("i_time")).isEqualTo(LocalTime.of(12, 30, 45));
            assertThat(s.getTimestamp(9)).isEqualTo(s.getTimestamp("j_timestamp"));
            assertThat(s.getDecimal(10)).isEqualTo(s.getDecimal("k_decimal")).isEqualTo(new BigDecimal("123.45"));

            PqList listByIndex = s.getList(11);
            PqList listByName = s.getList("m_list");
            assertThat(listByIndex.size()).isEqualTo(listByName.size()).isEqualTo(3);
            assertThat(listByIndex.get(0)).isEqualTo(listByName.get(0));

            PqMap mapByIndex = s.getMap(12);
            PqMap mapByName = s.getMap("n_map");
            assertThat(mapByIndex.size()).isEqualTo(mapByName.size()).isEqualTo(2);
            assertThat(mapByIndex.getValue("alpha")).isEqualTo(mapByName.getValue("alpha"));

            // Generic decoded/raw + isNull also route by index.
            for (int i = 0; i < s.getFieldCount(); i++) {
                String name = s.getFieldName(i);
                assertThat(s.isNull(i)).as("isNull(%d) vs isNull(\"%s\")", i, name)
                        .isEqualTo(s.isNull(name));
            }
        }
    }

    @Test
    void wrongNestedTypeByIndexThrowsIae() throws Exception {
        try (ParquetFileReader f = ParquetFileReader.open(InputFile.of(NESTED_STRUCT));
             RowReader r = f.rowReader()) {
            r.next();
            PqStruct addr = r.getStruct("address");
            // Field 2 is the INT32 `zip`; asking for it as a nested struct
            // should fail-fast with the field-named IAE matching the by-name path.
            assertThat(addr.getFieldName(2)).isEqualTo("zip");
            try {
                addr.getStruct(2);
                throw new AssertionError("expected IllegalArgumentException");
            } catch (IllegalArgumentException expected) {
                assertThat(expected).hasMessageContaining("zip");
            }
        }
    }
}
