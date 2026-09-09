/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.internal;

import java.nio.charset.StandardCharsets;
import java.util.HexFormat;

import dev.hardwood.internal.conversion.LogicalTypeConverter;
import dev.hardwood.internal.predicate.StatisticsDecoder;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.schema.ColumnSchema;

/// Formats raw page-index min/max bytes into a displayable string, taking the
/// column's physical and logical type into account. The rendering is the whole
/// value: a caller with a cell to fill applies its own budget and marks what it
/// cut, because only the caller knows its layout. Binary values go through
/// [BinaryValues], which renders them the same way the value and dictionary
/// formatters do.
public final class IndexValueFormatter {

    private static final char NON_PRINTABLE_PLACEHOLDER = '\u00B7';

    private IndexValueFormatter() {
    }

    public static String format(byte[] bytes, ColumnSchema col) {
        return format(bytes, col, true, BinaryValues.NO_LIMIT);
    }

    /// Logical-type-aware variant. When `useLogicalType=false`, dispatch is on
    /// physical type only — TIMESTAMP / DATE / TIME / DECIMAL / UUID columns
    /// then render as raw int / long / hex form, useful for confirming the
    /// underlying storage in the dive UI.
    public static String format(byte[] bytes, ColumnSchema col, boolean useLogicalType) {
        return format(bytes, col, useLogicalType, BinaryValues.NO_LIMIT);
    }

    /// Variant stating a budget for the binary rendering, for a caller that
    /// displays `maxChars` characters. The hex of a large payload is built only
    /// as far as that budget, so rendering into a cell costs a cell rather than
    /// the whole payload. The result is still the value, not a cut of it: it
    /// runs just past the budget when the payload is longer, so the caller sees
    /// that there is more and marks what it cuts.
    public static String format(byte[] bytes, ColumnSchema col,
                                boolean useLogicalType, int maxChars) {
        if (bytes == null) {
            return Strings.ABSENT_VALUE;
        }
        if (bytes.length == 0) {
            return isByteBacked(col.type()) ? "\"\"" : "";
        }
        LogicalType lt = useLogicalType ? col.logicalType() : null;

        if (lt instanceof LogicalType.DecimalType dt) {
            return (switch (col.type()) {
                case INT32 -> LogicalTypeConverter.longToDecimal(StatisticsDecoder.decodeInt(bytes), dt.scale());
                case INT64 -> LogicalTypeConverter.longToDecimal(StatisticsDecoder.decodeLong(bytes), dt.scale());
                default -> LogicalTypeConverter.bytesToDecimal(bytes, dt.scale());
            }).toPlainString();
        }
        if (lt instanceof LogicalType.TimestampType ts) {
            return LogicalTypeConverter.longToTemporal(decodeIntegral(bytes, col), ts).toString();
        }
        if (lt instanceof LogicalType.DateType) {
            return LogicalTypeConverter.intToDate(StatisticsDecoder.decodeInt(bytes)).toString();
        }
        if (lt instanceof LogicalType.TimeType t) {
            return LogicalTypeConverter.longToTime(decodeIntegral(bytes, col), t.unit()).toString();
        }

        return switch (col.type()) {
            case BOOLEAN -> Boolean.toString(StatisticsDecoder.decodeBoolean(bytes));
            case INT32 -> formatInt32(bytes, lt);
            case INT64 -> formatInt64(bytes, lt);
            case FLOAT -> Float.toString(StatisticsDecoder.decodeFloat(bytes));
            case DOUBLE -> Double.toString(StatisticsDecoder.decodeDouble(bytes));
            case INT96 -> HexFormat.of().formatHex(bytes);
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> formatBinary(bytes, lt, maxChars);
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

    /// Formats an already-decoded `byte[]` dictionary entry. `null` renders
    /// as `-`; otherwise delegates to the standard `format` pipeline (UUID,
    /// decimal, hex, UTF-8 string, etc.).
    public static String formatDecoded(byte[] value, ColumnSchema col) {
        return format(value, col);
    }

    /// A statistic on an `INT32` or `INT64` column, widened to the `long` the
    /// converters take. The date, time and timestamp annotations all read one.
    private static long decodeIntegral(byte[] bytes, ColumnSchema col) {
        return col.type() == PhysicalType.INT32
                ? StatisticsDecoder.decodeInt(bytes)
                : StatisticsDecoder.decodeLong(bytes);
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

    /// A column annotated as text is rendered as text, control characters and
    /// all. Everything else is a payload the annotation either describes
    /// exactly — a UUID, an interval, a half float — or does not describe at
    /// all, and an undescribed payload goes to [BinaryValues], which decides
    /// text vs. binary from the bytes themselves.
    private static String formatBinary(byte[] bytes, LogicalType lt, int maxChars) {
        if (isStringLogical(lt)) {
            return formatString(bytes);
        }
        if (lt instanceof LogicalType.UuidType && bytes.length == 16) {
            return LogicalTypeConverter.bytesToUuid(bytes).toString();
        }
        if (lt instanceof LogicalType.IntervalType && bytes.length == 12) {
            return RowValueFormatter.formatIntervalBytes(bytes);
        }
        if (lt instanceof LogicalType.Float16Type && bytes.length == 2) {
            return Float.toString(LogicalTypeConverter.bytesToFloat16(bytes));
        }
        return BinaryValues.render(bytes, maxChars);
    }

    /// Renders the value of a column annotated as text. Control characters are
    /// replaced with a placeholder rather than emitted raw, since a stray
    /// newline or carriage return in a statistic would break the table it is
    /// rendered into.
    private static String formatString(byte[] bytes) {
        String utf8 = new String(bytes, StandardCharsets.UTF_8);
        int printable = 0;
        for (int i = 0; i < utf8.length(); i++) {
            if (!Character.isISOControl(utf8.charAt(i))) {
                printable++;
            }
        }
        if (!utf8.isEmpty() && printable == 0) {
            return "0x" + HexFormat.of().formatHex(bytes);
        }
        if (printable == utf8.length()) {
            return utf8;
        }
        StringBuilder sb = new StringBuilder(utf8.length());
        for (int i = 0; i < utf8.length(); i++) {
            char c = utf8.charAt(i);
            sb.append(Character.isISOControl(c) ? NON_PRINTABLE_PLACEHOLDER : c);
        }
        return sb.toString();
    }

    private static boolean isStringLogical(LogicalType lt) {
        return lt instanceof LogicalType.StringType
                || lt instanceof LogicalType.EnumType
                || lt instanceof LogicalType.JsonType
                || lt instanceof LogicalType.BsonType;
    }

    /// A zero-length value is only meaningful for the variable-length physical
    /// types; rendering it as `""` distinguishes "present but empty" from the
    /// blank cell an absent statistic leaves behind.
    private static boolean isByteBacked(PhysicalType pt) {
        return pt == PhysicalType.BYTE_ARRAY || pt == PhysicalType.FIXED_LEN_BYTE_ARRAY;
    }
}
