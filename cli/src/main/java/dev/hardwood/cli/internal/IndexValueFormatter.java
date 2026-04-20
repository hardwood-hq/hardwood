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
import java.util.HexFormat;
import java.util.UUID;

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
        if (bytes == null) {
            return "-";
        }
        if (bytes.length == 0) {
            return isStringLike(col) ? "\"\"" : "";
        }
        LogicalType lt = col.logicalType();

        if (lt instanceof LogicalType.DecimalType dt) {
            BigInteger unscaled = switch (col.type()) {
                case INT32 -> BigInteger.valueOf(StatisticsDecoder.decodeInt(bytes));
                case INT64 -> BigInteger.valueOf(StatisticsDecoder.decodeLong(bytes));
                default -> new BigInteger(bytes);
            };
            return new BigDecimal(unscaled, dt.scale()).toPlainString();
        }

        return switch (col.type()) {
            case BOOLEAN -> Boolean.toString(StatisticsDecoder.decodeBoolean(bytes));
            case INT32 -> formatInt32(bytes, lt);
            case INT64 -> formatInt64(bytes, lt);
            case FLOAT -> Float.toString(StatisticsDecoder.decodeFloat(bytes));
            case DOUBLE -> Double.toString(StatisticsDecoder.decodeDouble(bytes));
            case INT96 -> HexFormat.of().formatHex(bytes);
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> formatBinary(bytes, lt, col.type());
        };
    }

    private static String formatInt32(byte[] bytes, LogicalType lt) {
        int v = StatisticsDecoder.decodeInt(bytes);
        if (lt instanceof LogicalType.IntType it && !it.isSigned()) {
            return Long.toString(Integer.toUnsignedLong(v));
        }
        return Integer.toString(v);
    }

    private static String formatInt64(byte[] bytes, LogicalType lt) {
        long v = StatisticsDecoder.decodeLong(bytes);
        if (lt instanceof LogicalType.IntType it && !it.isSigned()) {
            return Long.toUnsignedString(v);
        }
        return Long.toString(v);
    }

    private static String formatBinary(byte[] bytes, LogicalType lt, PhysicalType pt) {
        if (isStringLogical(lt) || (lt == null && pt == PhysicalType.BYTE_ARRAY)) {
            return formatString(bytes);
        }
        if (lt instanceof LogicalType.UuidType && bytes.length == 16) {
            ByteBuffer bb = ByteBuffer.wrap(bytes);
            return new UUID(bb.getLong(), bb.getLong()).toString();
        }
        return truncate(HexFormat.of().formatHex(bytes));
    }

    private static String formatString(byte[] bytes) {
        String utf8 = new String(bytes, StandardCharsets.UTF_8);
        int printable = 0;
        for (int i = 0; i < utf8.length(); i++) {
            if (!Character.isISOControl(utf8.charAt(i))) {
                printable++;
            }
        }
        if (utf8.length() > 0 && printable == 0) {
            return truncate("0x" + HexFormat.of().formatHex(bytes));
        }
        if (printable == utf8.length()) {
            return truncate(utf8);
        }
        StringBuilder sb = new StringBuilder(utf8.length());
        for (int i = 0; i < utf8.length(); i++) {
            char c = utf8.charAt(i);
            sb.append(Character.isISOControl(c) ? NON_PRINTABLE_PLACEHOLDER : c);
        }
        return truncate(sb.toString());
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
