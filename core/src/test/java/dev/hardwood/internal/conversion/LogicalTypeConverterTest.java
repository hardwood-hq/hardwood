/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.conversion;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.util.HexFormat;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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

    @Test
    void int96ToInstantRejectsWrongLength() {
        assertThatThrownBy(() -> LogicalTypeConverter.int96ToInstant(new byte[11]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("12 bytes");
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
