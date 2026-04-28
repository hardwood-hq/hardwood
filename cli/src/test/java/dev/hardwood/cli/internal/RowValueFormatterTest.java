/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.internal;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.row.PqInterval;
import dev.hardwood.schema.ColumnSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// Canonical rendering of raw dictionary values through [RowValueFormatter]. The
/// RowReader path is exercised by [dev.hardwood.cli.dive.DiveStateTest]; here we
/// cover the dictionary entry point directly since it's used on the hot path of
/// a screen that can have 500 000+ entries.
class RowValueFormatterTest {

    @Test
    void timestampMicrosUtc() {
        ColumnSchema col = column(PhysicalType.INT64,
                new LogicalType.TimestampType(true, LogicalType.TimeUnit.MICROS));

        // 2025-01-01T00:00:00.000000Z
        long micros = 1735689600_000_000L;

        assertThat(RowValueFormatter.formatDictionaryValue(micros, col))
                .isEqualTo("2025-01-01T00:00:00Z");
    }

    @Test
    void timestampMicrosNotUtcDropsZ() {
        ColumnSchema col = column(PhysicalType.INT64,
                new LogicalType.TimestampType(false, LogicalType.TimeUnit.MICROS));
        long micros = 1735689600_000_000L;

        assertThat(RowValueFormatter.formatDictionaryValue(micros, col))
                .isEqualTo("2025-01-01T00:00:00");
    }

    @Test
    void dateRendersAsLocalDate() {
        ColumnSchema col = column(PhysicalType.INT32, new LogicalType.DateType());
        // 2025-04-24 = epoch day 20202
        assertThat(RowValueFormatter.formatDictionaryValue(20202, col))
                .isEqualTo("2025-04-24");
    }

    @Test
    void timeMicrosRendersAsLocalTime() {
        ColumnSchema col = column(PhysicalType.INT64,
                new LogicalType.TimeType(false, LogicalType.TimeUnit.MICROS));
        long micros = (12L * 3600 + 34 * 60 + 56) * 1_000_000L;
        assertThat(RowValueFormatter.formatDictionaryValue(micros, col))
                .isEqualTo("12:34:56");
    }

    @Test
    void stringBytesDecodedAsUtf8() {
        ColumnSchema col = column(PhysicalType.BYTE_ARRAY, new LogicalType.StringType());
        byte[] bytes = "héllo".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        assertThat(RowValueFormatter.formatDictionaryValue(bytes, col)).isEqualTo("héllo");
    }

    @Test
    void rawLongFallbackWithoutLogicalType() {
        ColumnSchema col = column(PhysicalType.INT64, null);
        assertThat(RowValueFormatter.formatDictionaryValue(42L, col)).isEqualTo("42");
    }

    @Test
    void unsignedInt32() {
        ColumnSchema col = column(PhysicalType.INT32, new LogicalType.IntType(32, false));
        assertThat(RowValueFormatter.formatDictionaryValue(-1, col))
                .isEqualTo("4294967295");
    }

    @Test
    void rawBinaryWithoutLogicalTypeRendersAsHex() {
        ColumnSchema col = column(PhysicalType.BYTE_ARRAY, null);
        byte[] bytes = new byte[]{(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF};
        assertThat(RowValueFormatter.formatDictionaryValue(bytes, col))
                .isEqualTo("0xdeadbeef");
    }

    @Test
    void printableBinaryWithoutLogicalTypeRendersAsString() {
        ColumnSchema col = column(PhysicalType.BYTE_ARRAY, null);
        byte[] bytes = "hello".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        assertThat(RowValueFormatter.formatDictionaryValue(bytes, col))
                .isEqualTo("hello");
    }

    @Test
    void intervalRendersAsReadableComponents() {
        // Row 0 from interval_logical_type_test.parquet: 1 month, 15 days, 1 hour (3_600_000 ms)
        assertThat(RowValueFormatter.formatInterval(new PqInterval(1, 15, 3_600_000)))
                .isEqualTo("1mo 15d 3600000ms");
    }

    @Test
    void intervalWithZeroComponentsOmitsThem() {
        assertThat(RowValueFormatter.formatInterval(new PqInterval(0, 30, 0)))
                .isEqualTo("30d");
    }

    @Test
    void intervalAllZeroRendersAsZeroMs() {
        assertThat(RowValueFormatter.formatInterval(new PqInterval(0, 0, 0)))
                .isEqualTo("0ms");
    }

    @Test
    void intervalAtMaxUnsigned32BitRenders() {
        // 0xFFFFFFFF = 4_294_967_295 — the upper bound of the on-disk
        // unsigned-32-bit encoding, which `PqInterval` exposes verbatim as a long.
        long maxUint32 = 0xFFFFFFFFL;
        assertThat(RowValueFormatter.formatInterval(new PqInterval(maxUint32, maxUint32, maxUint32)))
                .isEqualTo("4294967295mo 4294967295d 4294967295ms");
    }

    @Test
    void intervalDictionaryBytesRenderAsComponents() {
        ColumnSchema col = column(PhysicalType.FIXED_LEN_BYTE_ARRAY, new LogicalType.IntervalType());
        // 1 month, 15 days, 3_600_000 ms — little-endian unsigned 32-bit
        byte[] bytes = new byte[12];
        ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        bb.putInt(1);
        bb.putInt(15);
        bb.putInt(3_600_000);
        assertThat(RowValueFormatter.formatDictionaryValue(bytes, col))
                .isEqualTo("1mo 15d 3600000ms");
    }

    private static ColumnSchema column(PhysicalType type, LogicalType logical) {
        return new ColumnSchema(
                FieldPath.of("value"),
                type,
                RepetitionType.REQUIRED,
                null,
                0,
                0,
                0,
                logical);
    }
}
