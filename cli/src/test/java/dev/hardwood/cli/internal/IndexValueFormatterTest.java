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
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.ColumnSchema;

import static org.assertj.core.api.Assertions.assertThat;

class IndexValueFormatterTest {

    @Test
    void rendersPrintableString() {
        assertThat(IndexValueFormatter.format("hello".getBytes(StandardCharsets.UTF_8), stringColumn()))
                .isEqualTo("hello");
    }

    @Test
    void rendersNonAsciiPrintableString() {
        assertThat(IndexValueFormatter.format("Última".getBytes(StandardCharsets.UTF_8), stringColumn()))
                .isEqualTo("Última");
    }

    @Test
    void truncatesLongString() {
        String longValue = "abcdefghijklmnopqrstuvwxyz";
        assertThat(IndexValueFormatter.format(longValue.getBytes(StandardCharsets.UTF_8), stringColumn()))
                .hasSize(20)
                .endsWith("...");
    }

    @Test
    void replacesControlCharsWithPlaceholder() {
        byte[] mixed = {'A', 0x01, 'B', 0x00, 'C'};
        assertThat(IndexValueFormatter.format(mixed, stringColumn()))
                .isEqualTo("A\u00B7B\u00B7C");
    }

    @Test
    void rendersAllControlBytesAsHex() {
        byte[] allNull = new byte[19];
        String result = IndexValueFormatter.format(allNull, stringColumn());
        assertThat(result).startsWith("0x").hasSize(20);
    }

    @Test
    void decodesInt32() {
        byte[] bytes = {0x2A, 0x00, 0x00, 0x00};
        assertThat(IndexValueFormatter.format(bytes, intColumn())).isEqualTo("42");
    }

    @Test
    void rendersEmptyStringExplicitly() {
        assertThat(IndexValueFormatter.format(new byte[0], stringColumn())).isEqualTo("\"\"");
    }

    @Test
    void rendersTimestampMicrosLogically() {
        ColumnSchema col = timestampColumn(true, LogicalType.TimeUnit.MICROS);
        // 2025-01-01T00:00:00Z = 1735689600 seconds = 1735689600_000_000 micros, little-endian INT64
        long micros = 1735689600_000_000L;
        byte[] bytes = new byte[8];
        for (int i = 0; i < 8; i++) {
            bytes[i] = (byte) (micros >> (i * 8));
        }
        assertThat(IndexValueFormatter.format(bytes, col)).isEqualTo("2025-01-01T00:00:00Z");
    }

    @Test
    void physicalModeRendersTimestampAsRawLong() {
        ColumnSchema col = timestampColumn(true, LogicalType.TimeUnit.MICROS);
        long micros = 1735689600_000_000L;
        byte[] bytes = new byte[8];
        for (int i = 0; i < 8; i++) {
            bytes[i] = (byte) (micros >> (i * 8));
        }
        assertThat(IndexValueFormatter.format(bytes, col, false))
                .isEqualTo(Long.toString(micros));
    }

    @Test
    void rendersDateLogically() {
        ColumnSchema col = new ColumnSchema(FieldPath.of("d"), PhysicalType.INT32,
                RepetitionType.OPTIONAL, null, 0, 1, 0, new LogicalType.DateType());
        // epoch day 20202 = 2025-04-24, little-endian INT32
        int day = 20202;
        byte[] bytes = new byte[]{(byte) day, (byte) (day >> 8), (byte) (day >> 16), (byte) (day >> 24)};
        assertThat(IndexValueFormatter.format(bytes, col)).isEqualTo("2025-04-24");
    }

    @Test
    void rendersIntervalLogically() {
        ColumnSchema col = new ColumnSchema(FieldPath.of("iv"), PhysicalType.FIXED_LEN_BYTE_ARRAY,
                RepetitionType.OPTIONAL, null, 0, 1, 0, new LogicalType.IntervalType());
        // 1 month, 15 days, 3_600_000 ms — little-endian unsigned 32-bit
        byte[] bytes = new byte[12];
        ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        bb.putInt(1);
        bb.putInt(15);
        bb.putInt(3_600_000);
        assertThat(IndexValueFormatter.format(bytes, col)).isEqualTo("1mo 15d 3600000ms");
    }

    @Test
    void intervalPhysicalModeRendersAsHex() {
        ColumnSchema col = new ColumnSchema(FieldPath.of("iv"), PhysicalType.FIXED_LEN_BYTE_ARRAY,
                RepetitionType.OPTIONAL, null, 0, 1, 0, new LogicalType.IntervalType());
        byte[] bytes = new byte[12];
        ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        bb.putInt(1);
        bb.putInt(15);
        bb.putInt(3_600_000);
        assertThat(IndexValueFormatter.format(bytes, col, false, false))
                .isEqualTo(java.util.HexFormat.of().formatHex(bytes));
    }

    private static ColumnSchema stringColumn() {
        return new ColumnSchema(FieldPath.of("s"), PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL,
                null, 0, 1, 0, new LogicalType.StringType());
    }

    private static ColumnSchema intColumn() {
        return new ColumnSchema(FieldPath.of("i"), PhysicalType.INT32, RepetitionType.OPTIONAL,
                null, 0, 1, 0, null);
    }

    private static ColumnSchema timestampColumn(boolean isUtc, LogicalType.TimeUnit unit) {
        return new ColumnSchema(FieldPath.of("ts"), PhysicalType.INT64, RepetitionType.OPTIONAL,
                null, 0, 1, 0, new LogicalType.TimestampType(isUtc, unit));
    }
}
