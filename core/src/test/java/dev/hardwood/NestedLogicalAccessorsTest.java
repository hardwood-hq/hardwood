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
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// The typed accessors that decode an annotation, read from a list element and a map
/// value (hardwood#1164).
///
/// Each accessor is exercised over every physical type its decode can start from —
/// `TIME` over `INT32` and `INT64`, `DECIMAL` over all three — because the read
/// selects the storage array by that physical type and a wrong arm surfaces as a
/// wrong value rather than as a failure.
class NestedLogicalAccessorsTest {

    private static final Path FIXTURE =
            Paths.get("src/test/resources/nested_logical_accessors.parquet");
    private static final Path WIDE_STRUCT = Paths.get("src/test/resources/wide_struct_test.parquet");

    private static final UUID UUID_A = UUID.fromString("00112233-4455-6677-8899-aabbccddeeff");
    private static final UUID UUID_B = UUID.fromString("ffeeddcc-bbaa-9988-7766-554433221100");

    // ==================== List elements ====================

    @Test
    void timesDecodeFromBothPhysicalTypes() throws Exception {
        try (ParquetFileReader file = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rows = file.rowReader()) {
            rows.next();
            // TIME(MILLIS) is stored as INT32, TIME(MICROS) as INT64.
            assertThat(rows.getList("times_ms").times())
                    .containsExactly(LocalTime.of(12, 30, 45, 500_000_000), LocalTime.of(0, 0, 0, 1_000_000));
            assertThat(rows.getList("times_us").times())
                    .containsExactly(LocalTime.of(1, 2, 3, 456_789_000));
        }
    }

    @Test
    void datesDecodeFromInt32() throws Exception {
        try (ParquetFileReader file = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rows = file.rowReader()) {
            rows.next();
            assertThat(rows.getList("dates").dates())
                    .containsExactly(LocalDate.of(2026, 1, 15), LocalDate.of(1970, 1, 1));
        }
    }

    @Test
    void decimalsDecodeFromEveryPhysicalType() throws Exception {
        try (ParquetFileReader file = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rows = file.rowReader()) {
            rows.next();
            assertThat(rows.getList("dec_i32").decimals())
                    .containsExactly(new BigDecimal("123.45"), new BigDecimal("-6.78"));
            assertThat(rows.getList("dec_i64").decimals())
                    .containsExactly(new BigDecimal("12345678.9012"), new BigDecimal("-0.0001"));
            assertThat(rows.getList("dec_flba").decimals())
                    .containsExactly(new BigDecimal("123.4567"), new BigDecimal("-0.0001"));
        }
    }

    @Test
    void uuidsBooleansAndBinariesDecodeFromTheirColumnArrays() throws Exception {
        try (ParquetFileReader file = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rows = file.rowReader()) {
            rows.next();
            assertThat(rows.getList("uuids").uuids()).containsExactly(UUID_A, UUID_B);
            assertThat(rows.getList("bools").booleans()).containsExactly(true, false);
            assertThat(rows.getList("bins").binaries())
                    .containsExactly(new byte[] {0x00, 0x01}, new byte[] {});
        }
    }

    @Test
    void aNullElementSurfacesAsNullThroughEveryTypedAccessor() throws Exception {
        try (ParquetFileReader file = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rows = file.rowReader()) {
            // Row 2 holds a single null element in each of these lists.
            rows.next();
            rows.next();
            rows.next();
            assertThat(rows.getList("times_ms").times()).containsExactly((LocalTime) null);
            assertThat(rows.getList("dates").dates()).containsExactly((LocalDate) null);
            assertThat(rows.getList("dec_flba").decimals()).containsExactly((BigDecimal) null);
            assertThat(rows.getList("uuids").uuids()).containsExactly((UUID) null);
            assertThat(rows.getList("bools").booleans()).containsExactly((Boolean) null);
            assertThat(rows.getList("bins").binaries()).containsExactly((byte[]) null);
        }
    }

    // ==================== Map values ====================

    @Test
    void mapValuesDecodeThroughTheirTypedAccessor() throws Exception {
        try (ParquetFileReader file = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rows = file.rowReader()) {
            rows.next();
            assertThat(entry(rows.getMap("date_map")).getDateValue()).isEqualTo(LocalDate.of(2026, 1, 15));
            assertThat(entry(rows.getMap("time_map")).getTimeValue())
                    .isEqualTo(LocalTime.of(12, 30, 45, 500_000_000));
            assertThat(entry(rows.getMap("dec_map")).getDecimalValue()).isEqualTo(new BigDecimal("123.4567"));
            assertThat(entry(rows.getMap("uuid_map")).getUuidValue()).isEqualTo(UUID_A);
        }
    }

    @Test
    void aNullMapValueSurfacesAsNull() throws Exception {
        try (ParquetFileReader file = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rows = file.rowReader()) {
            // Row 1's date_map has entries "b" (present) and "c" (null value).
            rows.next();
            rows.next();
            List<PqMap.Entry> entries = rows.getMap("date_map").getEntries();
            assertThat(entries).hasSize(2);
            assertThat(entries.get(0).getDateValue()).isEqualTo(LocalDate.of(1970, 1, 2));
            assertThat(entries.get(1).getDateValue()).isNull();
        }
    }

    // ==================== Annotation mismatches ====================

    /// `getDate`, `getUuid` and `getInterval` are the three accessors no cast can
    /// catch out: a `DATE`, a bare `INT32` and a `TIME` are one `int[]`, and every
    /// `FIXED_LEN_BYTE_ARRAY` of the right width is one `BinaryBatchValues`. Each of
    /// these returned a decode of the wrong bytes before the annotation was checked.
    @Test
    void theThreeAccessorsNoCastCatchesRejectTheColumnByName() throws Exception {
        try (ParquetFileReader file = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rows = file.rowReader()) {
            rows.next();
            // The leaf's own name in the 3-level list encoding is `element`, which is
            // what the rejection can name.
            assertThatThrownBy(rows.getList("plain_ints")::dates)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("[nested_logical_accessors.parquet] Column 'element' is INT32,"
                            + " which cannot be read as a date");
            assertThatThrownBy(rows.getList("dec_flba")::uuids)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("[nested_logical_accessors.parquet] Column 'element' is"
                            + " FIXED_LEN_BYTE_ARRAY annotated DECIMAL(20, 4),"
                            + " which cannot be read as a UUID");
            assertThatThrownBy(rows.getList("uuids")::intervals)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("[nested_logical_accessors.parquet] Column 'element' is"
                            + " FIXED_LEN_BYTE_ARRAY annotated UUID,"
                            + " which cannot be read as an interval");
            assertThatThrownBy(entry(rows.getMap("uuid_map"))::getDateValue)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("[nested_logical_accessors.parquet] Column 'value' is"
                            + " FIXED_LEN_BYTE_ARRAY annotated UUID, which cannot be read as a date");
        }
    }

    /// Every other typed accessor already fails on a cast the decode makes, so none
    /// checks the annotation ahead of it — `_designs/EXCEPTION_MODEL.md` puts that
    /// validation above its cost bar, and #971 is the sweep that gives these failures
    /// a message. The type is pinned rather than the text, which the JDK writes.
    @Test
    void theOtherTypedAccessorsFailOnTheCastTheyAlreadyMake() throws Exception {
        try (ParquetFileReader file = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rows = file.rowReader()) {
            rows.next();
            // No annotation to cast: the dereference below the cast is what throws.
            assertThatThrownBy(rows.getList("plain_ints")::times)
                    .isInstanceOf(NullPointerException.class);
            assertThatThrownBy(rows.getList("plain_ints")::decimals)
                    .isInstanceOf(NullPointerException.class);
            // The wrong annotation: the cast to the expected one throws.
            assertThatThrownBy(rows.getList("dates")::times)
                    .isInstanceOf(ClassCastException.class);
            assertThatThrownBy(entry(rows.getMap("date_map"))::getDecimalValue)
                    .isInstanceOf(ClassCastException.class);
        }
    }

    @Test
    void aStructAccessorRejectsAFieldItsAnnotationDoesNotFit() throws Exception {
        try (ParquetFileReader file = ParquetFileReader.open(InputFile.of(WIDE_STRUCT));
             RowReader rows = file.rowReader()) {
            rows.next();
            PqStruct s = rows.getStruct("fields");
            // a_int is a bare INT32 and i_time a TIME(MILLIS): both share the DATE
            // column's int[], and before the guard both decoded to a wrong LocalDate.
            assertThatThrownBy(() -> s.getDate("a_int"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("[wide_struct_test.parquet] Column 'a_int' is INT32,"
                            + " which cannot be read as a date");
            assertThatThrownBy(() -> s.getDate("i_time"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("[wide_struct_test.parquet] Column 'i_time' is"
                            + " INT32 annotated TIME(MILLIS, local), which cannot be read as a date");
            // getDecimal is #971's population — it fails on its own annotation cast.
            assertThatThrownBy(() -> s.getDecimal("a_int"))
                    .isInstanceOf(NullPointerException.class);
        }
    }

    private static PqMap.Entry entry(PqMap map) {
        List<PqMap.Entry> entries = map.getEntries();
        assertThat(entries).hasSize(1);
        return entries.get(0);
    }
}
