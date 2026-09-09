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
import java.util.HexFormat;

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
    void rendersLongStringInFull() {
        String longValue = "abcdefghijklmnopqrstuvwxyz";
        assertThat(IndexValueFormatter.format(longValue.getBytes(StandardCharsets.UTF_8), stringColumn()))
                .isEqualTo(longValue);
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
        assertThat(result).isEqualTo("0x" + "00".repeat(19));
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
    void rendersAbsentValueWithTheSharedMarker() {
        assertThat(IndexValueFormatter.format(null, stringColumn())).isEqualTo(Strings.ABSENT_VALUE);
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
                RepetitionType.OPTIONAL, null, 0, 1, 0, LogicalType.date());
        // epoch day 20202 = 2025-04-24, little-endian INT32
        int day = 20202;
        byte[] bytes = new byte[]{(byte) day, (byte) (day >> 8), (byte) (day >> 16), (byte) (day >> 24)};
        assertThat(IndexValueFormatter.format(bytes, col)).isEqualTo("2025-04-24");
    }

    @Test
    void rendersIntervalLogically() {
        ColumnSchema col = new ColumnSchema(FieldPath.of("iv"), PhysicalType.FIXED_LEN_BYTE_ARRAY,
                RepetitionType.OPTIONAL, null, 0, 1, 0, LogicalType.interval());
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
                RepetitionType.OPTIONAL, null, 0, 1, 0, LogicalType.interval());
        byte[] bytes = new byte[12];
        ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        bb.putInt(1);
        bb.putInt(15);
        bb.putInt(3_600_000);
        assertThat(IndexValueFormatter.format(bytes, col, false))
                .isEqualTo("0x" + HexFormat.of().formatHex(bytes));
    }

    /// GeoParquet 1.x predates the `GEOMETRY` logical type and stores WKB in a
    /// bare `BYTE_ARRAY`, so the writer's min/max are opaque bytes. Rendering
    /// them as lenient UTF-8 turned every bound into mojibake.
    @Test
    void unannotatedBinaryBoundsDoNotRenderAsText() {
        ColumnSchema col = bareByteArrayColumn();

        assertThat(IndexValueFormatter.format(WKB_POINT, col, true))
                .isEqualTo("0x010100000000000000005366c0f71622f0fa1955c0");
    }

    /// A capped cell shows a marked prefix of the hex, the same treatment a
    /// long string gets — enough to tell two bounds apart.
    @Test
    void unannotatedBinaryBoundsHonourAnExplicitBudget() {
        assertThat(IndexValueFormatter.format(WKB_POINT, bareByteArrayColumn(), true, 20))
                .isEqualTo("0x01010000000000000000");
    }

    @Test
    void distinctLongStringsRenderDistinctly() {
        String first = "the-quick-brown-fox-jumps-over-the-lazy-dog-0";
        String second = "the-quick-brown-fox-jumps-over-the-lazy-dog-1";

        assertThat(IndexValueFormatter.format(first.getBytes(StandardCharsets.UTF_8), stringColumn()))
                .isEqualTo(first);
        assertThat(IndexValueFormatter.format(second.getBytes(StandardCharsets.UTF_8), stringColumn()))
                .isEqualTo(second);
    }

    /// A column annotated as text stays text: the placeholder keeps a stray
    /// control character from breaking the table it is rendered into.
    @Test
    void annotatedStringBoundsKeepRenderingAsText() {
        byte[] bytes = {'a', 0x00, 'b'};

        assertThat(IndexValueFormatter.format(bytes, stringColumn())).isEqualTo("a\u00B7b");
    }

    /// A WKB `Point` — the payload GeoParquet 1.x stores in an unannotated
    /// `BYTE_ARRAY` geometry column.
    private static final byte[] WKB_POINT =
            HexFormat.of().parseHex("010100000000000000005366c0f71622f0fa1955c0");

    private static ColumnSchema bareByteArrayColumn() {
        return new ColumnSchema(FieldPath.of("geometry"), PhysicalType.BYTE_ARRAY,
                RepetitionType.OPTIONAL, null, 0, 1, 0, null);
    }

    private static ColumnSchema stringColumn() {
        return new ColumnSchema(FieldPath.of("s"), PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL,
                null, 0, 1, 0, LogicalType.string());
    }

    private static ColumnSchema intColumn() {
        return new ColumnSchema(FieldPath.of("i"), PhysicalType.INT32, RepetitionType.OPTIONAL,
                null, 0, 1, 0, null);
    }

    private static ColumnSchema timestampColumn(boolean isUtc, LogicalType.TimeUnit unit) {
        return new ColumnSchema(FieldPath.of("ts"), PhysicalType.INT64, RepetitionType.OPTIONAL,
                null, 0, 1, 0, LogicalType.timestamp(isUtc, unit));
    }
}
