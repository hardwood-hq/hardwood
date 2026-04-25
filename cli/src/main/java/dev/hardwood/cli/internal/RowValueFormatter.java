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

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.SchemaNode;

/// Canonical rendering of Parquet values for display in the `dive` TUI.
///
/// Dispatches on the field's [LogicalType] and produces machine-reparseable text:
/// ISO-8601 timestamps, `LocalDate.toString` for dates, `BigDecimal.toPlainString`
/// for decimals, etc. Two entry points share the same dispatch core:
///
/// - [#format(RowReader, int, SchemaNode)]: Data preview — uses the reader's
///   typed accessors (`getTimestamp`, `getDate`, `getDecimal`, `getUuid`,
///   `getString`). For top-level group fields (structs / lists / maps /
///   variants) falls back to `getValue().toString()`.
/// - [#formatDictionaryValue]: Dictionary — takes a raw primitive (`long` micros,
///   `byte[]`, etc.) because dictionary entries come out of the parsed
///   `Dictionary` records as primitive arrays, with no `RowReader` available.
///
/// Sibling of [IndexValueFormatter], which handles the `byte[]` case for
/// per-page / per-chunk min/max statistics.
public final class RowValueFormatter {

    private RowValueFormatter() {
    }

    /// Data preview entry point. Uses the reader's typed accessors when the
    /// field carries a known logical type; otherwise falls back to the raw
    /// `getValue` + `toString`.
    public static String format(RowReader reader, int fieldIndex, SchemaNode field) {
        if (reader.isNull(fieldIndex)) {
            return "null";
        }
        if (field instanceof SchemaNode.GroupNode) {
            // Nested group — no per-field logical type to dispatch on.
            return String.valueOf(reader.getValue(fieldIndex));
        }
        SchemaNode.PrimitiveNode prim = (SchemaNode.PrimitiveNode) field;
        LogicalType lt = prim.logicalType();
        if (lt instanceof LogicalType.TimestampType ts) {
            return formatTimestamp(reader.getTimestamp(fieldIndex), ts);
        }
        if (lt instanceof LogicalType.DateType) {
            return reader.getDate(fieldIndex).toString();
        }
        if (lt instanceof LogicalType.TimeType) {
            return reader.getTime(fieldIndex).toString();
        }
        if (lt instanceof LogicalType.DecimalType) {
            return reader.getDecimal(fieldIndex).toPlainString();
        }
        if (lt instanceof LogicalType.UuidType) {
            return reader.getUuid(fieldIndex).toString();
        }
        if (lt instanceof LogicalType.StringType
                || lt instanceof LogicalType.EnumType
                || lt instanceof LogicalType.JsonType
                || lt instanceof LogicalType.BsonType) {
            return reader.getString(fieldIndex);
        }
        if (lt instanceof LogicalType.IntType it && !it.isSigned()) {
            long raw = switch (prim.type()) {
                case INT32 -> Integer.toUnsignedLong(reader.getInt(fieldIndex));
                case INT64 -> reader.getLong(fieldIndex);
                default -> ((Number) reader.getValue(fieldIndex)).longValue();
            };
            return Long.toUnsignedString(raw);
        }
        // BYTE_ARRAY / FIXED_LEN_BYTE_ARRAY / INT96 with no string-like logical
        // type fall through here. `getValue` returns the raw byte[]; the default
        // `String.valueOf(byte[])` would emit the JVM's array-hashcode form
        // ([B@...). Render printable UTF-8 as text, else 0x-hex — mirrors how
        // IndexValueFormatter handles raw-byte stats.
        Object raw = reader.getValue(fieldIndex);
        if (raw instanceof byte[] bytes) {
            return formatRawBytes(bytes);
        }
        return String.valueOf(raw);
    }

    /// Dictionary entry point. Converts a raw primitive drawn from a
    /// `Dictionary.*` record into the canonical display form for the column's
    /// logical type. `rawValue` must be one of: `Integer`, `Long`, `Float`,
    /// `Double`, `byte[]` — matching the five `Dictionary` subtypes.
    public static String formatDictionaryValue(Object rawValue, ColumnSchema col) {
        LogicalType lt = col.logicalType();
        return switch (rawValue) {
            case Integer i -> formatInt(i, lt);
            case Long l -> formatLong(l, lt);
            case Float f -> Float.toString(f);
            case Double d -> Double.toString(d);
            case byte[] bytes -> formatBytes(bytes, lt);
            case null -> "null";
            default -> String.valueOf(rawValue);
        };
    }

    private static String formatInt(int raw, LogicalType lt) {
        if (lt instanceof LogicalType.DateType) {
            return LocalDate.ofEpochDay(raw).toString();
        }
        if (lt instanceof LogicalType.TimeType t) {
            return formatTime(raw, t.unit());
        }
        if (lt instanceof LogicalType.IntType it && !it.isSigned()) {
            return Long.toString(Integer.toUnsignedLong(raw));
        }
        return Integer.toString(raw);
    }

    private static String formatLong(long raw, LogicalType lt) {
        if (lt instanceof LogicalType.TimestampType ts) {
            return formatTimestamp(rawToInstant(raw, ts.unit()), ts);
        }
        if (lt instanceof LogicalType.TimeType t) {
            return formatTime(raw, t.unit());
        }
        if (lt instanceof LogicalType.IntType it && !it.isSigned()) {
            return Long.toUnsignedString(raw);
        }
        return Long.toString(raw);
    }

    private static String formatBytes(byte[] raw, LogicalType lt) {
        if (lt instanceof LogicalType.StringType
                || lt instanceof LogicalType.EnumType
                || lt instanceof LogicalType.JsonType
                || lt instanceof LogicalType.BsonType) {
            return new String(raw, StandardCharsets.UTF_8);
        }
        if (lt instanceof LogicalType.DecimalType d) {
            return new BigDecimal(new BigInteger(raw), d.scale()).toPlainString();
        }
        if (lt instanceof LogicalType.UuidType && raw.length == 16) {
            ByteBuffer bb = ByteBuffer.wrap(raw);
            return new UUID(bb.getLong(), bb.getLong()).toString();
        }
        return formatRawBytes(raw);
    }

    /// Renders a raw byte array as either UTF-8 text (when the bytes are
    /// well-formed UTF-8 with no control characters) or `0x`-prefixed
    /// lowercase hex. Truncation is left to the caller (the dive screens
    /// already truncate each rendered cell to a fixed width).
    private static String formatRawBytes(byte[] raw) {
        if (raw.length == 0) {
            return "";
        }
        try {
            String utf8 = StandardCharsets.UTF_8.newDecoder()
                    .decode(ByteBuffer.wrap(raw))
                    .toString();
            for (int i = 0; i < utf8.length(); i++) {
                if (Character.isISOControl(utf8.charAt(i))) {
                    return "0x" + HexFormat.of().formatHex(raw);
                }
            }
            return utf8;
        }
        catch (java.nio.charset.CharacterCodingException e) {
            return "0x" + HexFormat.of().formatHex(raw);
        }
    }

    private static String formatTimestamp(Instant instant, LogicalType.TimestampType type) {
        String s = instant.toString();
        if (!type.isAdjustedToUTC() && s.endsWith("Z")) {
            // Instant always formats with trailing 'Z'; drop it when the annotation
            // says the timestamp is not UTC-adjusted (local-time semantics).
            return s.substring(0, s.length() - 1);
        }
        return s;
    }

    private static String formatTime(long raw, LogicalType.TimeUnit unit) {
        long nanosOfDay = switch (unit) {
            case MILLIS -> raw * 1_000_000L;
            case MICROS -> raw * 1_000L;
            case NANOS -> raw;
        };
        return LocalTime.ofNanoOfDay(nanosOfDay).toString();
    }

    private static Instant rawToInstant(long raw, LogicalType.TimeUnit unit) {
        return switch (unit) {
            case MILLIS -> Instant.ofEpochMilli(raw);
            case MICROS -> Instant.ofEpochSecond(
                    Math.floorDiv(raw, 1_000_000L),
                    Math.floorMod(raw, 1_000_000L) * 1_000L);
            case NANOS -> Instant.ofEpochSecond(
                    Math.floorDiv(raw, 1_000_000_000L),
                    Math.floorMod(raw, 1_000_000_000L));
        };
    }
}
