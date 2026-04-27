/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.internal;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.HexFormat;
import java.util.UUID;

import dev.hardwood.internal.conversion.LogicalTypeConverter;
import dev.hardwood.internal.predicate.StatisticsDecoder;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.schema.ColumnSchema;

/// Formats raw page-index min/max bytes into a displayable string, taking the
/// column's physical and logical type into account. String values are truncated
/// to keep table rows readable; binary values are rendered as hex.
public final class IndexValueFormatter {

    private static final int MAX_STRING_LEN = 20;
    private static final char NON_PRINTABLE_PLACEHOLDER = '\u00B7';

    private IndexValueFormatter() {
    }

    public static String format(byte[] bytes, ColumnSchema col) {
        return format(bytes, col, true, true);
    }

    /// Logical-type-aware variant. When `useLogicalType=false`, dispatch is on
    /// physical type only — TIMESTAMP / DATE / TIME / DECIMAL / UUID columns
    /// then render as raw int / long / hex form, useful for confirming the
    /// underlying storage in the dive UI.
    public static String format(byte[] bytes, ColumnSchema col, boolean useLogicalType) {
        return format(bytes, col, useLogicalType, true);
    }

    /// Untruncated variant. When `truncate=false`, the per-string and per-binary
    /// length cap (`MAX_STRING_LEN`) is bypassed — useful for facts panes /
    /// modals where the full value is wanted regardless of length. Callers who
    /// render into a tight cell should keep the default `truncate=true`.
    public static String format(byte[] bytes, ColumnSchema col,
                                 boolean useLogicalType, boolean truncate) {
        if (bytes == null) {
            return "-";
        }
        if (bytes.length == 0) {
            return isStringLike(col) ? "\"\"" : "";
        }
        LogicalType lt = useLogicalType ? col.logicalType() : null;

        if (lt instanceof LogicalType.DecimalType dt) {
            BigInteger unscaled = switch (col.type()) {
                case INT32 -> BigInteger.valueOf(StatisticsDecoder.decodeInt(bytes));
                case INT64 -> BigInteger.valueOf(StatisticsDecoder.decodeLong(bytes));
                default -> new BigInteger(bytes);
            };
            return new BigDecimal(unscaled, dt.scale()).toPlainString();
        }
        if (lt instanceof LogicalType.TimestampType ts) {
            long raw = col.type() == PhysicalType.INT32
                    ? StatisticsDecoder.decodeInt(bytes)
                    : StatisticsDecoder.decodeLong(bytes);
            Instant instant = switch (ts.unit()) {
                case MILLIS -> Instant.ofEpochMilli(raw);
                case MICROS -> Instant.ofEpochSecond(
                        Math.floorDiv(raw, 1_000_000L),
                        Math.floorMod(raw, 1_000_000L) * 1_000L);
                case NANOS -> Instant.ofEpochSecond(
                        Math.floorDiv(raw, 1_000_000_000L),
                        Math.floorMod(raw, 1_000_000_000L));
            };
            String s = instant.toString();
            return ts.isAdjustedToUTC() ? s : (s.endsWith("Z") ? s.substring(0, s.length() - 1) : s);
        }
        if (lt instanceof LogicalType.DateType) {
            return LocalDate.ofEpochDay(StatisticsDecoder.decodeInt(bytes)).toString();
        }
        if (lt instanceof LogicalType.TimeType t) {
            long raw = col.type() == PhysicalType.INT32
                    ? StatisticsDecoder.decodeInt(bytes)
                    : StatisticsDecoder.decodeLong(bytes);
            long nanosOfDay = switch (t.unit()) {
                case MILLIS -> raw * 1_000_000L;
                case MICROS -> raw * 1_000L;
                case NANOS -> raw;
            };
            return LocalTime.ofNanoOfDay(nanosOfDay).toString();
        }

        return switch (col.type()) {
            case BOOLEAN -> Boolean.toString(StatisticsDecoder.decodeBoolean(bytes));
            case INT32 -> formatInt32(bytes, lt);
            case INT64 -> formatInt64(bytes, lt);
            case FLOAT -> Float.toString(StatisticsDecoder.decodeFloat(bytes));
            case DOUBLE -> Double.toString(StatisticsDecoder.decodeDouble(bytes));
            case INT96 -> HexFormat.of().formatHex(bytes);
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> formatBinary(bytes, lt, col.type(), truncate);
        };
    }

    /// Formats an already-decoded `INT32` dictionary entry. Logical types
    /// `DECIMAL` / `DATE` / `TIME` go through [LogicalTypeConverter]; otherwise
    /// the raw int is rendered honouring the unsigned [LogicalType.IntType].
    public static String formatDecoded(int value, ColumnSchema col) {
        LogicalType lt = col.logicalType();
        if (lt instanceof LogicalType.DecimalType
                || lt instanceof LogicalType.DateType
                || lt instanceof LogicalType.TimeType) {
            return String.valueOf(LogicalTypeConverter.convert(value, col.type(), lt));
        }
        return formatInt32Value(value, lt);
    }

    /// Formats an already-decoded `INT64` dictionary entry. Logical types
    /// `DECIMAL` / `TIME` / `TIMESTAMP` go through [LogicalTypeConverter];
    /// otherwise the raw long is rendered honouring the unsigned
    /// [LogicalType.IntType].
    public static String formatDecoded(long value, ColumnSchema col) {
        LogicalType lt = col.logicalType();
        if (lt instanceof LogicalType.DecimalType
                || lt instanceof LogicalType.TimeType
                || lt instanceof LogicalType.TimestampType) {
            return String.valueOf(LogicalTypeConverter.convert(value, col.type(), lt));
        }
        return formatInt64Value(value, lt);
    }

    public static String formatDecoded(float value) {
        return Float.toString(value);
    }

    public static String formatDecoded(double value) {
        return Double.toString(value);
    }

    public static String formatDecoded(boolean value) {
        return Boolean.toString(value);
    }

    /// Formats an already-decoded byte-array dictionary entry. `null` renders
    /// as `-`; otherwise delegates to the standard `format` pipeline (UUID,
    /// decimal, hex, truncated UTF-8 string, etc.).
    public static String formatDecoded(byte[] value, ColumnSchema col) {
        return format(value, col);
    }

    private static String formatInt32(byte[] bytes, LogicalType lt) {
        return formatInt32Value(StatisticsDecoder.decodeInt(bytes), lt);
    }

    private static String formatInt32Value(int v, LogicalType lt) {
        if (lt instanceof LogicalType.IntType it && !it.isSigned()) {
            return Long.toString(Integer.toUnsignedLong(v));
        }
        return Integer.toString(v);
    }

    private static String formatInt64(byte[] bytes, LogicalType lt) {
        return formatInt64Value(StatisticsDecoder.decodeLong(bytes), lt);
    }

    private static String formatInt64Value(long v, LogicalType lt) {
        if (lt instanceof LogicalType.IntType it && !it.isSigned()) {
            return Long.toUnsignedString(v);
        }
        return Long.toString(v);
    }

    private static String formatBinary(byte[] bytes, LogicalType lt, PhysicalType pt, boolean truncate) {
        if (isStringLogical(lt) || (lt == null && pt == PhysicalType.BYTE_ARRAY)) {
            return formatString(bytes, truncate);
        }
        if (lt instanceof LogicalType.UuidType && bytes.length == 16) {
            ByteBuffer bb = ByteBuffer.wrap(bytes);
            return new UUID(bb.getLong(), bb.getLong()).toString();
        }
        String hex = HexFormat.of().formatHex(bytes);
        return truncate ? truncate(hex) : hex;
    }

    private static String formatString(byte[] bytes, boolean truncate) {
        String utf8 = new String(bytes, StandardCharsets.UTF_8);
        int printable = 0;
        for (int i = 0; i < utf8.length(); i++) {
            if (!Character.isISOControl(utf8.charAt(i))) {
                printable++;
            }
        }
        if (utf8.length() > 0 && printable == 0) {
            String hex = "0x" + HexFormat.of().formatHex(bytes);
            return truncate ? truncate(hex) : hex;
        }
        if (printable == utf8.length()) {
            return truncate ? truncate(utf8) : utf8;
        }
        StringBuilder sb = new StringBuilder(utf8.length());
        for (int i = 0; i < utf8.length(); i++) {
            char c = utf8.charAt(i);
            sb.append(Character.isISOControl(c) ? NON_PRINTABLE_PLACEHOLDER : c);
        }
        String result = sb.toString();
        return truncate ? truncate(result) : result;
    }

    private static boolean isStringLogical(LogicalType lt) {
        return lt instanceof LogicalType.StringType
                || lt instanceof LogicalType.EnumType
                || lt instanceof LogicalType.JsonType
                || lt instanceof LogicalType.BsonType;
    }

    private static boolean isStringLike(ColumnSchema col) {
        return isStringLogical(col.logicalType())
                || (col.logicalType() == null && col.type() == PhysicalType.BYTE_ARRAY);
    }

    private static String truncate(String s) {
        if (s.length() <= MAX_STRING_LEN) {
            return s;
        }
        return s.substring(0, MAX_STRING_LEN - 3) + "...";
    }
}
