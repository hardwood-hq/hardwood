/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import dev.morling.hardwood.metadata.LogicalType;
import dev.morling.hardwood.reader.ParquetFileReader;
import dev.morling.hardwood.reader.RowReader;
import dev.morling.hardwood.row.Row;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for reading Parquet files with logical data types.
 */
public class LogicalTypesTest {

    @Test
    void testReadAllLogicalTypes() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/logical_types_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            // Verify schema has logical types
            assertThat(fileReader.getFileSchema().getColumnCount()).isEqualTo(19);
            assertThat(fileReader.getFileSchema().getColumn("name").logicalType()).isNotNull();
            assertThat(fileReader.getFileSchema().getColumn("birth_date").logicalType()).isNotNull();
            assertThat(fileReader.getFileSchema().getColumn("created_at_millis").logicalType()).isNotNull();

            try (RowReader rowReader = fileReader.createRowReader()) {
                List<Row> rows = new ArrayList<>();
                for (Row row : rowReader) {
                    rows.add(row);
                }

                // Verify we read exactly 3 rows
                assertThat(rows).hasSize(3);

                // Row 0: Alice
                Row row0 = rows.get(0);
                assertThat(row0.getInt("id")).isEqualTo(1);
                assertThat(row0.getString("name")).isEqualTo("Alice");
                assertThat(row0.getDate("birth_date")).isEqualTo(LocalDate.of(1990, 1, 15));
                // TIMESTAMP with all units
                assertThat(row0.getTimestamp("created_at_millis")).isEqualTo(Instant.parse("2025-01-01T10:30:00Z"));
                assertThat(row0.getTimestamp("created_at_micros")).isEqualTo(Instant.parse("2025-01-01T10:30:00.123456Z"));
                assertThat(row0.getTimestamp("created_at_nanos")).isEqualTo(Instant.parse("2025-01-01T10:30:00.123456789Z"));
                // TIME with all units
                assertThat(row0.getTime("wake_time_millis")).isEqualTo(LocalTime.of(7, 30, 0));
                assertThat(row0.getTime("wake_time_micros")).isEqualTo(LocalTime.of(7, 30, 0, 123456000));
                assertThat(row0.getTime("wake_time_nanos")).isEqualTo(LocalTime.of(7, 30, 0, 123456789));
                assertThat(row0.getDecimal("balance")).isEqualTo(new BigDecimal("1234.56"));
                assertThat(row0.getInt("tiny_int")).isEqualTo(10);
                assertThat(row0.getInt("small_int")).isEqualTo(1000);
                assertThat(row0.getInt("medium_int")).isEqualTo(100000);
                assertThat(row0.getLong("big_int")).isEqualTo(10000000000L);
                assertThat(row0.getInt("tiny_uint")).isEqualTo(255);
                assertThat(row0.getInt("small_uint")).isEqualTo(65535);
                assertThat(row0.getInt("medium_uint")).isEqualTo(2147483647);
                assertThat(row0.getLong("big_uint")).isEqualTo(9223372036854775807L);
                assertThat(row0.getUuid("account_id")).isEqualTo(UUID.fromString("12345678-1234-5678-1234-567812345678"));

                // Row 1: Bob
                Row row1 = rows.get(1);
                assertThat(row1.getInt("id")).isEqualTo(2);
                assertThat(row1.getString("name")).isEqualTo("Bob");
                assertThat(row1.getDate("birth_date")).isEqualTo(LocalDate.of(1985, 6, 30));
                // TIMESTAMP with all units
                assertThat(row1.getTimestamp("created_at_millis")).isEqualTo(Instant.parse("2025-01-02T14:45:30Z"));
                assertThat(row1.getTimestamp("created_at_micros")).isEqualTo(Instant.parse("2025-01-02T14:45:30.654321Z"));
                assertThat(row1.getTimestamp("created_at_nanos")).isEqualTo(Instant.parse("2025-01-02T14:45:30.654321987Z"));
                // TIME with all units
                assertThat(row1.getTime("wake_time_millis")).isEqualTo(LocalTime.of(8, 0, 0));
                assertThat(row1.getTime("wake_time_micros")).isEqualTo(LocalTime.of(8, 0, 0, 654321000));
                assertThat(row1.getTime("wake_time_nanos")).isEqualTo(LocalTime.of(8, 0, 0, 654321987));
                assertThat(row1.getDecimal("balance")).isEqualTo(new BigDecimal("9876.54"));
                assertThat(row1.getInt("tiny_int")).isEqualTo(20);
                assertThat(row1.getInt("small_int")).isEqualTo(2000);
                assertThat(row1.getInt("medium_int")).isEqualTo(200000);
                assertThat(row1.getLong("big_int")).isEqualTo(20000000000L);
                assertThat(row1.getInt("tiny_uint")).isEqualTo(128);
                assertThat(row1.getInt("small_uint")).isEqualTo(32768);
                assertThat(row1.getInt("medium_uint")).isEqualTo(1000000);
                assertThat(row1.getLong("big_uint")).isEqualTo(5000000000000000000L);
                assertThat(row1.getUuid("account_id")).isEqualTo(UUID.fromString("87654321-4321-8765-4321-876543218765"));

                // Row 2: Charlie
                Row row2 = rows.get(2);
                assertThat(row2.getInt("id")).isEqualTo(3);
                assertThat(row2.getString("name")).isEqualTo("Charlie");
                assertThat(row2.getDate("birth_date")).isEqualTo(LocalDate.of(2000, 12, 25));
                // TIMESTAMP with all units
                assertThat(row2.getTimestamp("created_at_millis")).isEqualTo(Instant.parse("2025-01-03T09:15:45Z"));
                assertThat(row2.getTimestamp("created_at_micros")).isEqualTo(Instant.parse("2025-01-03T09:15:45.111222Z"));
                assertThat(row2.getTimestamp("created_at_nanos")).isEqualTo(Instant.parse("2025-01-03T09:15:45.111222333Z"));
                // TIME with all units
                assertThat(row2.getTime("wake_time_millis")).isEqualTo(LocalTime.of(6, 45, 0));
                assertThat(row2.getTime("wake_time_micros")).isEqualTo(LocalTime.of(6, 45, 0, 111222000));
                assertThat(row2.getTime("wake_time_nanos")).isEqualTo(LocalTime.of(6, 45, 0, 111222333));
                assertThat(row2.getDecimal("balance")).isEqualTo(new BigDecimal("5555.55"));
                assertThat(row2.getInt("tiny_int")).isEqualTo(30);
                assertThat(row2.getInt("small_int")).isEqualTo(3000);
                assertThat(row2.getInt("medium_int")).isEqualTo(300000);
                assertThat(row2.getLong("big_int")).isEqualTo(30000000000L);
                assertThat(row2.getInt("tiny_uint")).isEqualTo(64);
                assertThat(row2.getInt("small_uint")).isEqualTo(16384);
                assertThat(row2.getInt("medium_uint")).isEqualTo(500000);
                assertThat(row2.getLong("big_uint")).isEqualTo(4611686018427387904L);
                assertThat(row2.getUuid("account_id")).isEqualTo(UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"));
            }
        }
    }

    @Test
    void testGetObjectWithLogicalTypes() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/logical_types_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            try (RowReader rowReader = fileReader.createRowReader()) {
                Row row = rowReader.iterator().next();

                // Test getObject() returns converted logical types
                assertThat(row.getObject("name")).isInstanceOf(String.class);
                assertThat(row.getObject("birth_date")).isInstanceOf(LocalDate.class);
                // All timestamp units return Instant
                assertThat(row.getObject("created_at_millis")).isInstanceOf(Instant.class);
                assertThat(row.getObject("created_at_micros")).isInstanceOf(Instant.class);
                assertThat(row.getObject("created_at_nanos")).isInstanceOf(Instant.class);
                // All time units return LocalTime
                assertThat(row.getObject("wake_time_millis")).isInstanceOf(LocalTime.class);
                assertThat(row.getObject("wake_time_micros")).isInstanceOf(LocalTime.class);
                assertThat(row.getObject("wake_time_nanos")).isInstanceOf(LocalTime.class);
                assertThat(row.getObject("balance")).isInstanceOf(BigDecimal.class);
                assertThat(row.getObject("account_id")).isInstanceOf(UUID.class);

                // Verify values match
                assertThat(row.getObject("name")).isEqualTo("Alice");
                assertThat(row.getObject("birth_date")).isEqualTo(LocalDate.of(1990, 1, 15));
            }
        }
    }

    @Test
    void testLogicalTypeMetadata() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/logical_types_test.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            var schema = fileReader.getFileSchema();

            // Verify logical types are parsed correctly
            assertThat(schema.getColumn("name").logicalType()).isInstanceOf(
                    LogicalType.StringType.class);
            assertThat(schema.getColumn("birth_date").logicalType()).isInstanceOf(
                    LogicalType.DateType.class);
            // All timestamp columns should have TimestampType
            assertThat(schema.getColumn("created_at_millis").logicalType()).isInstanceOf(
                    LogicalType.TimestampType.class);
            assertThat(schema.getColumn("created_at_micros").logicalType()).isInstanceOf(
                    LogicalType.TimestampType.class);
            assertThat(schema.getColumn("created_at_nanos").logicalType()).isInstanceOf(
                    LogicalType.TimestampType.class);
            // All time columns should have TimeType
            assertThat(schema.getColumn("wake_time_millis").logicalType()).isInstanceOf(
                    LogicalType.TimeType.class);
            assertThat(schema.getColumn("wake_time_micros").logicalType()).isInstanceOf(
                    LogicalType.TimeType.class);
            assertThat(schema.getColumn("wake_time_nanos").logicalType()).isInstanceOf(
                    LogicalType.TimeType.class);
            assertThat(schema.getColumn("balance").logicalType()).isInstanceOf(
                    LogicalType.DecimalType.class);
            assertThat(schema.getColumn("tiny_int").logicalType()).isInstanceOf(
                    LogicalType.IntType.class);
            assertThat(schema.getColumn("small_int").logicalType()).isInstanceOf(
                    LogicalType.IntType.class);
            assertThat(schema.getColumn("tiny_uint").logicalType()).isInstanceOf(
                    LogicalType.IntType.class);
            assertThat(schema.getColumn("small_uint").logicalType()).isInstanceOf(
                    LogicalType.IntType.class);
            assertThat(schema.getColumn("medium_uint").logicalType()).isInstanceOf(
                    LogicalType.IntType.class);
            assertThat(schema.getColumn("big_uint").logicalType()).isInstanceOf(
                    LogicalType.IntType.class);
            // medium_int and big_int don't have logical type annotations (PyArrow doesn't write INT_32/INT_64)
            assertThat(schema.getColumn("medium_int").logicalType()).isNull();
            assertThat(schema.getColumn("big_int").logicalType()).isNull();
            // account_id has UUID logical type (PyArrow 21+ writes UUID annotation)
            assertThat(schema.getColumn("account_id").logicalType()).isInstanceOf(
                    LogicalType.UuidType.class);

            // Verify TIMESTAMP units are correctly parsed
            var timestampMillis = (LogicalType.TimestampType) schema
                    .getColumn("created_at_millis").logicalType();
            assertThat(timestampMillis.unit()).isEqualTo(
                    LogicalType.TimestampType.TimeUnit.MILLIS);
            assertThat(timestampMillis.isAdjustedToUTC()).isTrue();

            var timestampMicros = (LogicalType.TimestampType) schema
                    .getColumn("created_at_micros").logicalType();
            assertThat(timestampMicros.unit()).isEqualTo(
                    LogicalType.TimestampType.TimeUnit.MICROS);
            assertThat(timestampMicros.isAdjustedToUTC()).isTrue();

            var timestampNanos = (LogicalType.TimestampType) schema
                    .getColumn("created_at_nanos").logicalType();
            assertThat(timestampNanos.unit()).isEqualTo(
                    LogicalType.TimestampType.TimeUnit.NANOS);
            assertThat(timestampNanos.isAdjustedToUTC()).isTrue();

            // Verify TIME units are correctly parsed
            var timeMillis = (LogicalType.TimeType) schema
                    .getColumn("wake_time_millis").logicalType();
            assertThat(timeMillis.unit()).isEqualTo(
                    LogicalType.TimeType.TimeUnit.MILLIS);

            var timeMicros = (LogicalType.TimeType) schema
                    .getColumn("wake_time_micros").logicalType();
            assertThat(timeMicros.unit()).isEqualTo(
                    LogicalType.TimeType.TimeUnit.MICROS);

            var timeNanos = (LogicalType.TimeType) schema
                    .getColumn("wake_time_nanos").logicalType();
            assertThat(timeNanos.unit()).isEqualTo(
                    LogicalType.TimeType.TimeUnit.NANOS);

            var decimalType = (LogicalType.DecimalType) schema.getColumn("balance")
                    .logicalType();
            assertThat(decimalType.scale()).isEqualTo(2);
            assertThat(decimalType.precision()).isEqualTo(10);

            var intType = (LogicalType.IntType) schema.getColumn("tiny_int").logicalType();
            assertThat(intType.bitWidth()).isEqualTo(8);
            assertThat(intType.isSigned()).isTrue();

            // Verify unsigned integer types
            var tinyUintType = (LogicalType.IntType) schema.getColumn("tiny_uint")
                    .logicalType();
            assertThat(tinyUintType.bitWidth()).isEqualTo(8);
            assertThat(tinyUintType.isSigned()).isFalse();

            var smallUintType = (LogicalType.IntType) schema.getColumn("small_uint")
                    .logicalType();
            assertThat(smallUintType.bitWidth()).isEqualTo(16);
            assertThat(smallUintType.isSigned()).isFalse();

            var mediumUintType = (LogicalType.IntType) schema.getColumn("medium_uint")
                    .logicalType();
            assertThat(mediumUintType.bitWidth()).isEqualTo(32);
            assertThat(mediumUintType.isSigned()).isFalse();

            var bigUintType = (LogicalType.IntType) schema.getColumn("big_uint")
                    .logicalType();
            assertThat(bigUintType.bitWidth()).isEqualTo(64);
            assertThat(bigUintType.isSigned()).isFalse();
        }
    }
}
