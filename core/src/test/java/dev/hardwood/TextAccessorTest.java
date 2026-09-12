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
import java.util.HexFormat;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// `getString` and `getStrings` read the columns that hold text and refuse the rest
/// (hardwood#1196).
///
/// Every column held as bytes decodes to *some* text, so before the check these returned a
/// `DECIMAL`'s or an `INT96`'s stored bytes as characters rather than failing, and a column
/// held as anything else failed on a cast with no message. The refusal runs at the top
/// level, in the `PqStruct` / `PqList` / `PqMap` flyweights and on the `ColumnReader`.
class TextAccessorTest {

    private static final Path FLAT = Paths.get("src/test/resources/predicate/predicate_single.parquet");
    private static final Path OPAQUE =
            Paths.get("src/test/resources/predicate/predicate_opaque_single.parquet");
    private static final Path INT96 = Paths.get("src/test/resources/int96_timestamp_test.parquet");
    private static final Path NESTED = Paths.get("src/test/resources/nested_logical_accessors.parquet");
    private static final Path STRUCT = Paths.get("src/test/resources/typed_accessors_issue_445.parquet");

    // ==================== Top level ====================

    @Test
    void theTextColumnsAreStringEnumJsonAndAnUnannotatedByteArray() throws Exception {
        try (ParquetFileReader file = ParquetFileReader.open(InputFile.of(FLAT));
             RowReader rows = file.rowReader()) {
            rows.next();
            assertThat(rows.getString("str")).isEqualTo("k0000");
            assertThat(rows.getString("json")).isEqualTo("{\"k\":\"k0000\"}");
            assertThat(rows.getString("enum")).isEqualTo("E0");
            // The unannotated BYTE_ARRAY column holds two zero bytes, which decode as text.
            assertThat(rows.getString("ba")).isEqualTo("\u0000\u0000");
        }
    }

    @Test
    void aBinaryColumnWhoseBytesAreNotTextIsRefused() throws Exception {
        try (ParquetFileReader file = ParquetFileReader.open(InputFile.of(FLAT));
             RowReader rows = file.rowReader()) {
            rows.next();
            assertThatThrownBy(() -> rows.getString("dec_flba"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("[predicate_single.parquet] Column 'dec_flba' is FIXED_LEN_BYTE_ARRAY"
                            + " annotated DECIMAL(20, 2), which cannot be read as a string");
            assertThatThrownBy(() -> rows.getString("f16"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("[predicate_single.parquet] Column 'f16' is FIXED_LEN_BYTE_ARRAY"
                            + " annotated FLOAT16, which cannot be read as a string");
            assertThatThrownBy(() -> rows.getString("uuid"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("[predicate_single.parquet] Column 'uuid' is FIXED_LEN_BYTE_ARRAY"
                            + " annotated UUID, which cannot be read as a string");
            // An unannotated FIXED_LEN_BYTE_ARRAY is a fixed-width payload, not the
            // pre-annotation text an unannotated BYTE_ARRAY may be.
            assertThatThrownBy(() -> rows.getString("flba5"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("[predicate_single.parquet] Column 'flba5' is FIXED_LEN_BYTE_ARRAY,"
                            + " which cannot be read as a string");
        }
    }

    @Test
    void anOpaquePayloadColumnIsRefused() throws Exception {
        try (ParquetFileReader file = ParquetFileReader.open(InputFile.of(OPAQUE));
             RowReader rows = file.rowReader()) {
            rows.next();
            assertThatThrownBy(() -> rows.getString("bson"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("[predicate_opaque_single.parquet] Column 'bson' is BYTE_ARRAY"
                            + " annotated BSON, which cannot be read as a string");
            assertThatThrownBy(() -> rows.getString("iv"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("[predicate_opaque_single.parquet] Column 'iv' is FIXED_LEN_BYTE_ARRAY"
                            + " annotated INTERVAL, which cannot be read as a string");
        }
    }

    @Test
    void anInt96ColumnIsRefused() throws Exception {
        try (ParquetFileReader file = ParquetFileReader.open(InputFile.of(INT96));
             RowReader rows = file.rowReader()) {
            rows.next();
            assertThatThrownBy(() -> rows.getString("ts"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("[int96_timestamp_test.parquet] Column 'ts' is INT96,"
                            + " which cannot be read as a string");
        }
    }

    @Test
    void aColumnHeldAsSomethingOtherThanBytesIsRefused() throws Exception {
        try (ParquetFileReader file = ParquetFileReader.open(InputFile.of(FLAT));
             RowReader rows = file.rowReader()) {
            rows.next();
            assertThatThrownBy(() -> rows.getString("i32"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("[predicate_single.parquet] Column 'i32' is INT32,"
                            + " which cannot be read as a string");
            assertThatThrownBy(() -> rows.getString("dec_i64"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("[predicate_single.parquet] Column 'dec_i64' is INT64"
                            + " annotated DECIMAL(18, 2), which cannot be read as a string");
        }
    }

    /// The refusal is the column's, so it comes where the other logical accessors put
    /// theirs: after the null test, which reads a null value as `null` from any column.
    @Test
    void aNullValueReadsAsNullWhateverTheColumnHolds() throws Exception {
        try (ParquetFileReader file = ParquetFileReader.open(InputFile.of(FLAT));
             RowReader rows = file.rowReader()) {
            int nullRows = 0;
            while (nullRows == 0 && rows.hasNext()) {
                rows.next();
                if (rows.isNull("dec_flba")) {
                    assertThat(rows.getString("dec_flba")).isNull();
                    nullRows++;
                }
            }
            assertThat(nullRows).isEqualTo(1);
        }
    }

    /// `getInt`, `getLong` and `getBinary` read the stored value of any column their
    /// physical type holds, whatever it is annotated with, as `reference/accessors.md`
    /// records. `getString` is the one byte-backed accessor that is not of that kind.
    @Test
    void thePhysicalAccessorsReadAnAnnotatedColumn() throws Exception {
        try (ParquetFileReader file = ParquetFileReader.open(InputFile.of(FLAT));
             RowReader rows = file.rowReader()) {
            rows.next();
            assertThat(rows.getInt("date")).isEqualTo(18_600);
            assertThat(rows.getInt("time_ms")).isEqualTo(3_600_000);
            assertThat(rows.getInt("dec_i32")).isEqualTo(-25_000);
            assertThat(rows.getLong("ts_us_utc")).isEqualTo(1_699_280_000_000_000L);
            assertThat(rows.getLong("dec_i64")).isEqualTo(-246_913_400L);
            assertThat(HexFormat.of().formatHex(rows.getBinary("dec_flba")))
                    .isEqualTo("ffffffffffffff9e58");
            assertThat(HexFormat.of().formatHex(rows.getBinary("f16"))).isEqualTo("40d2");
            assertThat(rows.getBinary("uuid")).hasSize(16);
        }
        try (ParquetFileReader file = ParquetFileReader.open(InputFile.of(INT96));
             RowReader rows = file.rowReader()) {
            rows.next();
            assertThat(rows.getBinary("ts")).hasSize(12);
        }
    }

    // ==================== ColumnReader ====================

    @Test
    void getStringsReadsATextColumnAndRefusesTheRest() throws Exception {
        try (ParquetFileReader file = ParquetFileReader.open(InputFile.of(FLAT));
             ColumnReader text = file.columnReader("str")) {
            assertThat(text.nextBatch()).isTrue();
            assertThat(text.getStrings()[0]).isEqualTo("k0000");
        }
        try (ParquetFileReader file = ParquetFileReader.open(InputFile.of(FLAT));
             ColumnReader decimal = file.columnReader("dec_flba")) {
            assertThat(decimal.nextBatch()).isTrue();
            assertThatThrownBy(decimal::getStrings)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("[predicate_single.parquet] Column 'dec_flba' is FIXED_LEN_BYTE_ARRAY"
                            + " annotated DECIMAL(20, 2), which cannot be read as a string");
        }
        try (ParquetFileReader file = ParquetFileReader.open(InputFile.of(INT96));
             ColumnReader int96 = file.columnReader("ts")) {
            assertThat(int96.nextBatch()).isTrue();
            assertThatThrownBy(int96::getStrings)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("[int96_timestamp_test.parquet] Column 'ts' is INT96,"
                            + " which cannot be read as a string");
        }
    }

    // ==================== Flyweights ====================

    @Test
    void aListElementThatIsNotTextIsRefused() throws Exception {
        try (ParquetFileReader file = ParquetFileReader.open(InputFile.of(NESTED));
             RowReader rows = file.rowReader()) {
            rows.next();
            // An unannotated BYTE_ARRAY element is text, as at the top level.
            assertThat(rows.getList("bins").strings()).hasSize(2);
            assertThatThrownBy(rows.getList("uuids")::strings)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("[nested_logical_accessors.parquet] Column 'element' is"
                            + " FIXED_LEN_BYTE_ARRAY annotated UUID, which cannot be read as a string");
            assertThatThrownBy(rows.getList("plain_ints")::strings)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("[nested_logical_accessors.parquet] Column 'element' is INT32,"
                            + " which cannot be read as a string");
        }
    }

    @Test
    void aMapKeyOrValueThatIsNotTextIsRefused() throws Exception {
        try (ParquetFileReader file = ParquetFileReader.open(InputFile.of(NESTED));
             RowReader rows = file.rowReader()) {
            rows.next();
            PqMap.Entry uuidEntry = onlyEntry(rows.getMap("uuid_map"));
            assertThat(uuidEntry.getStringKey()).isEqualTo("a");
            assertThatThrownBy(uuidEntry::getStringValue)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("[nested_logical_accessors.parquet] Column 'value' is"
                            + " FIXED_LEN_BYTE_ARRAY annotated UUID, which cannot be read as a string");
        }
        try (ParquetFileReader file = ParquetFileReader.open(InputFile.of(STRUCT));
             RowReader rows = file.rowReader()) {
            rows.next();
            assertThatThrownBy(onlyEntry(rows.getMap("decimal_keyed"))::getStringKey)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("[typed_accessors_issue_445.parquet] Column 'key' is"
                            + " FIXED_LEN_BYTE_ARRAY annotated DECIMAL(18, 2),"
                            + " which cannot be read as a string");
        }
    }

    @Test
    void aStructFieldThatIsNotTextIsRefused() throws Exception {
        try (ParquetFileReader file = ParquetFileReader.open(InputFile.of(STRUCT));
             RowReader rows = file.rowReader()) {
            rows.next();
            PqStruct struct = rows.getStruct("nested_struct");
            assertThat(struct.getString("payload")).isEqualTo("{\"answer\":42}");
            assertThatThrownBy(() -> struct.getString("i8"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("[typed_accessors_issue_445.parquet] Column 'i8' is INT32"
                            + " annotated INT_8, which cannot be read as a string");
        }
    }

    private static PqMap.Entry onlyEntry(PqMap map) {
        List<PqMap.Entry> entries = map.getEntries();
        assertThat(entries).hasSize(1);
        return entries.get(0);
    }
}
