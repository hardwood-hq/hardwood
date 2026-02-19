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

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.reader.ParquetFileReader;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for logical type metadata in Parquet file schemas.
 */
public class LogicalTypesMetadataTest {

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
