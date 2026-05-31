/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;

/// End-to-end coverage for legacy `converted_type` annotations
/// (hardwood-hq/hardwood#529). The fixture is written by older-style writers
/// (parquet-mr / Hive / Impala / Spark): every primitive carries only a legacy
/// `converted_type`, with no modern `logicalType`. The schema builder must
/// promote each one to its `LogicalType` so the read path decodes the column
/// correctly instead of handing back a bare physical value.
///
/// Fixture: `tools/simple-datagen.py` → `legacy_converted_types_test.parquet`.
/// Two rows; row 1 carries edge values (signed minima, unsigned maxima stored
/// as the raw two's-complement bit pattern).
class LegacyConvertedTypesReadTest {

    private static final Path FILE = Paths.get("src/test/resources/legacy_converted_types_test.parquet");

    private static ParquetFileReader open() throws IOException {
        return ParquetFileReader.open(InputFile.of(FILE));
    }

    private static LogicalType logicalTypeOf(String column) throws IOException {
        try (ParquetFileReader reader = open()) {
            return reader.getFileSchema().getColumn(column).logicalType();
        }
    }

    @Test
    void everyLegacyConvertedTypeResolvesToItsLogicalType() throws IOException {
        assertThat(logicalTypeOf("utf8_col")).isEqualTo(new LogicalType.StringType());
        assertThat(logicalTypeOf("enum_col")).isEqualTo(new LogicalType.EnumType());
        assertThat(logicalTypeOf("json_col")).isEqualTo(new LogicalType.JsonType());
        assertThat(logicalTypeOf("bson_col")).isEqualTo(new LogicalType.BsonType());
        assertThat(logicalTypeOf("date_col")).isEqualTo(new LogicalType.DateType());
        assertThat(logicalTypeOf("decimal_col")).isEqualTo(new LogicalType.DecimalType(2, 18));
        assertThat(logicalTypeOf("time_millis_col"))
                .isEqualTo(new LogicalType.TimeType(true, LogicalType.TimeUnit.MILLIS));
        assertThat(logicalTypeOf("time_micros_col"))
                .isEqualTo(new LogicalType.TimeType(true, LogicalType.TimeUnit.MICROS));
        assertThat(logicalTypeOf("timestamp_millis_col"))
                .isEqualTo(new LogicalType.TimestampType(true, LogicalType.TimeUnit.MILLIS));
        assertThat(logicalTypeOf("timestamp_micros_col"))
                .isEqualTo(new LogicalType.TimestampType(true, LogicalType.TimeUnit.MICROS));
        assertThat(logicalTypeOf("int8_col")).isEqualTo(new LogicalType.IntType(8, true));
        assertThat(logicalTypeOf("int16_col")).isEqualTo(new LogicalType.IntType(16, true));
        assertThat(logicalTypeOf("int32_col")).isEqualTo(new LogicalType.IntType(32, true));
        assertThat(logicalTypeOf("int64_col")).isEqualTo(new LogicalType.IntType(64, true));
        assertThat(logicalTypeOf("uint8_col")).isEqualTo(new LogicalType.IntType(8, false));
        assertThat(logicalTypeOf("uint16_col")).isEqualTo(new LogicalType.IntType(16, false));
        assertThat(logicalTypeOf("uint32_col")).isEqualTo(new LogicalType.IntType(32, false));
        assertThat(logicalTypeOf("uint64_col")).isEqualTo(new LogicalType.IntType(64, false));
    }

    @Test
    void stringEnumJsonBsonDecodeFromByteArray() throws IOException {
        try (ParquetFileReader reader = open();
             RowReader rows = reader.rowReader()) {
            rows.next();
            assertThat(rows.getString("utf8_col")).isEqualTo("alpha");
            assertThat(rows.getString("json_col")).isEqualTo("{\"k\":1}");
            assertThat(new String(rows.getBinary("enum_col"), StandardCharsets.UTF_8)).isEqualTo("RED");
            // Minimal empty BSON document: 4-byte length prefix (5) + terminating null.
            assertThat(rows.getBinary("bson_col")).containsExactly(0x05, 0x00, 0x00, 0x00, 0x00);
        }
    }

    @Test
    void dateDecimalTimeTimestampDecodeToTemporalAndDecimalValues() throws IOException {
        try (ParquetFileReader reader = open();
             RowReader rows = reader.rowReader()) {
            rows.next();
            assertThat(rows.getDate("date_col")).isEqualTo(LocalDate.of(1970, 1, 1));
            assertThat(rows.getDecimal("decimal_col")).isEqualByComparingTo(new BigDecimal("123.45"));
            assertThat(rows.getTime("time_millis_col")).isEqualTo(LocalTime.MIDNIGHT);
            assertThat(rows.getTime("time_micros_col")).isEqualTo(LocalTime.MIDNIGHT);
            assertThat(rows.getTimestamp("timestamp_millis_col")).isEqualTo(Instant.EPOCH);
            assertThat(rows.getTimestamp("timestamp_micros_col")).isEqualTo(Instant.EPOCH);

            rows.next();
            assertThat(rows.getDate("date_col")).isEqualTo(LocalDate.of(2024, 1, 1));
            assertThat(rows.getDecimal("decimal_col")).isEqualByComparingTo(new BigDecimal("-1.00"));
            assertThat(rows.getTime("time_millis_col")).isEqualTo(LocalTime.of(1, 1, 1));
            assertThat(rows.getTime("time_micros_col")).isEqualTo(LocalTime.of(1, 1, 1));
            assertThat(rows.getTimestamp("timestamp_millis_col"))
                    .isEqualTo(Instant.parse("2024-01-01T00:00:00Z"));
            assertThat(rows.getTimestamp("timestamp_micros_col"))
                    .isEqualTo(Instant.parse("2024-01-01T00:00:00Z"));
        }
    }

    @Test
    void signedIntsWidenAndNarrowPerBitWidth() throws IOException {
        try (ParquetFileReader reader = open();
             RowReader rows = reader.rowReader()) {
            rows.next();
            // INT_8 / INT_16 narrow to Byte / Short; INT_32 / INT_64 pass through.
            assertThat(rows.getValue("int8_col")).isEqualTo((byte) -128);
            assertThat(rows.getValue("int16_col")).isEqualTo((short) -32768);
            assertThat(rows.getValue("int32_col")).isEqualTo(Integer.MIN_VALUE);
            assertThat(rows.getValue("int64_col")).isEqualTo(-100L);

            rows.next();
            assertThat(rows.getValue("int8_col")).isEqualTo((byte) 127);
            assertThat(rows.getValue("int16_col")).isEqualTo((short) 32767);
            assertThat(rows.getValue("int32_col")).isEqualTo(Integer.MAX_VALUE);
            assertThat(rows.getValue("int64_col")).isEqualTo(100L);
        }
    }

    @Test
    void unsignedIntsPreserveTheUnsignedBitPattern() throws IOException {
        try (ParquetFileReader reader = open();
             RowReader rows = reader.rowReader()) {
            rows.next();
            assertThat(rows.getValue("uint8_col")).isEqualTo(0);
            assertThat(rows.getValue("uint16_col")).isEqualTo(0);
            assertThat(rows.getValue("uint32_col")).isEqualTo(0);
            assertThat(rows.getValue("uint64_col")).isEqualTo(0L);

            rows.next();
            assertThat(rows.getValue("uint8_col")).isEqualTo(255);
            assertThat(rows.getValue("uint16_col")).isEqualTo(65535);
            // uint32 max is stored as int -1; reinterpret as unsigned.
            assertThat(Integer.toUnsignedLong((Integer) rows.getValue("uint32_col")))
                    .isEqualTo(4294967295L);
            // uint64 max is stored as long -1; reinterpret as unsigned.
            assertThat(Long.toUnsignedString((Long) rows.getValue("uint64_col")))
                    .isEqualTo("18446744073709551615");
        }
    }
}
