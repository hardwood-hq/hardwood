/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.conversion;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.reader.ParquetReadException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HexFormat;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LogicalTypeConverterTest {

    static Stream<Arguments> int96Cases() {
        return Stream.of(
                // Unix epoch: nanos-of-day=0, julian-day=2440588 (JD of 1970-01-01)
                Arguments.of("epoch", int96(0L, 2440588), Instant.EPOCH),
                // One nanosecond after epoch
                Arguments.of("epoch + 1 ns", int96(1L, 2440588), Instant.ofEpochSecond(0, 1)),
                // Noon on the epoch: 12 * 3600 * 1e9 ns
                Arguments.of("epoch noon", int96(12L * 3600 * 1_000_000_000L, 2440588),
                        Instant.parse("1970-01-01T12:00:00Z")),
                // Bytes copied from parquet-testing/data/int96_from_spark.parquet — row 0
                Arguments.of("spark row 0", hex("002a1ed963430000978a2500"),
                        Instant.parse("2024-01-01T20:34:56.123456Z")),
                // row 1
                Arguments.of("spark row 1", hex("00a0b83046030000978a2500"),
                        Instant.parse("2024-01-01T01:00:00Z")),
                // row 2 — late date
                Arguments.of("spark row 2", hex("00e02992d20900002cfe5100"),
                        Instant.parse("9999-12-31T03:00:00Z")),
                // row 3
                Arguments.of("spark row 3", hex("006096604e4b0000038c2500"),
                        Instant.parse("2024-12-30T23:00:00Z")));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("int96Cases")
    void int96ToInstantDecodesKnownValues(String name, byte[] bytes, Instant expected) {
        assertThat(LogicalTypeConverter.int96ToInstant(bytes)).isEqualTo(expected);
    }

    /// A `BYTE_ARRAY` `DECIMAL` value stored as no bytes is zero, as a filter predicate on the
    /// column compares it.
    @Test
    void anEmptyDecimalPayloadIsZero() {
        assertThat(LogicalTypeConverter.bytesToDecimal(new byte[0], 3)).isEqualTo(new BigDecimal("0.000"));
        assertThat(LogicalTypeConverter.bytesToDecimal(new byte[] { 0x7F, 0x01 }, 1, 0, 2))
                .isEqualTo(new BigDecimal("0.00"));
    }

    @Test
    void aDecimalPayloadInsideABufferIsReadAtItsOffset() {
        assertThat(LogicalTypeConverter.bytesToDecimal(new byte[] { 0x7F, (byte) 0xFF, 0x06 }, 1, 2, 1))
                .isEqualTo(new BigDecimal("-25.0"));
    }


    @Test
    void uuidWrongWidthIsAReadFailureForBothOverloads() {
        assertThatThrownBy(() -> LogicalTypeConverter.bytesToUuid(new byte[15]))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("UUID requires exactly 16 bytes, got 15");
        assertThatThrownBy(() -> LogicalTypeConverter.bytesToUuid(new byte[20], 2, 15))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("UUID requires exactly 16 bytes, got 15");
    }

    @Test
    void intervalWrongWidthIsAReadFailureForBothOverloads() {
        assertThatThrownBy(() -> LogicalTypeConverter.bytesToInterval(new byte[11]))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("INTERVAL requires exactly 12 bytes, got 11");
        assertThatThrownBy(() -> LogicalTypeConverter.bytesToInterval(new byte[13], 1, 11))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("INTERVAL requires exactly 12 bytes, got 11");
    }

    @Test
    void nullTypeRejectsNonNullValuesButPassesNullThrough() {
        assertThatThrownBy(() -> LogicalTypeConverter.convert(7, PhysicalType.INT32, LogicalType.nullType()))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Non-null physical value on NULL-typed column: 7");
        assertThat(LogicalTypeConverter.convert(null, PhysicalType.INT32, LogicalType.nullType())).isNull();
    }

    @Test
    void structuralLogicalTypeStillCannotReachPrimitiveConversion() {
        assertThatThrownBy(() -> LogicalTypeConverter.convert(
                new byte[0], PhysicalType.BYTE_ARRAY, LogicalType.list()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Structural logical type ListType reached primitive-value conversion");
    }


    @Test
    void int96ToInstantRejectsWrongLength() {
        assertThatThrownBy(() -> LogicalTypeConverter.int96ToInstant(new byte[11]))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("INT96 requires exactly 12 bytes, got 11");
    }

    static Stream<Arguments> bsonPayloads() {
        return Stream.of(
                // Minimal empty BSON document: 4-byte little-endian length + terminator.
                Arguments.of("empty document", new byte[] {0x05, 0x00, 0x00, 0x00, 0x00}),
                // Not valid UTF-8; a string decoder would replace these with U+FFFD.
                Arguments.of("non-utf8 bytes", new byte[] {(byte) 0xC3, (byte) 0x28, (byte) 0xA0, (byte) 0xA1}));
    }

    @ParameterizedTest(name = "BSON {0} passes through as raw bytes")
    @MethodSource("bsonPayloads")
    void bsonReturnsRawBytes(String name, byte[] payload) {
        Object result = LogicalTypeConverter.convert(payload, PhysicalType.BYTE_ARRAY, LogicalType.bson());

        assertThat(result).isInstanceOf(byte[].class).isEqualTo(payload);
    }

    @Test
    void jsonDispatchesToString() {
        byte[] jsonBytes = "{\"k\":1}".getBytes(StandardCharsets.UTF_8);

        Object result = LogicalTypeConverter.convert(jsonBytes, PhysicalType.BYTE_ARRAY, LogicalType.json());

        assertThat(result).isInstanceOf(String.class).isEqualTo("{\"k\":1}");
    }

    /// Build a 12-byte INT96 payload from nanos-of-day and Julian day, little-endian.
    private static byte[] int96(long nanosOfDay, int julianDay) {
        byte[] bytes = new byte[12];
        ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
                .putLong(nanosOfDay)
                .putInt(julianDay);
        return bytes;
    }

    private static byte[] hex(String hex) {
        return HexFormat.of().parseHex(hex);
    }
}
