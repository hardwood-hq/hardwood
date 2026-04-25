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
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
import dev.hardwood.row.PqVariant;
import dev.hardwood.row.PqVariantArray;
import dev.hardwood.row.PqVariantObject;
import dev.hardwood.row.VariantType;
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
    /// `getValue` + `toString`. Equivalent to `format(reader, i, field, true)`.
    public static String format(RowReader reader, int fieldIndex, SchemaNode field) {
        return format(reader, fieldIndex, field, true);
    }

    /// Data preview entry point with explicit logical-type dispatch toggle.
    /// `useLogicalType=true` is the default UX — render timestamps, decimals,
    /// UUIDs, etc. as their canonical logical form. `useLogicalType=false`
    /// skips the logical-type dispatch and renders the underlying physical
    /// value (e.g. `1735689600000000` instead of `2025-01-01T00:00:00Z`),
    /// useful for confirming the raw storage form. Nested groups always
    /// render structurally — the toggle only affects primitive leaves.
    public static String format(RowReader reader, int fieldIndex, SchemaNode field, boolean useLogicalType) {
        if (reader.isNull(fieldIndex)) {
            return "null";
        }
        if (field instanceof SchemaNode.GroupNode) {
            // Nested group — render structurally rather than letting the JVM's
            // default `Object.toString()` print "dev.hardwood.internal...".
            return formatNested(reader.getValue(fieldIndex), 0);
        }
        SchemaNode.PrimitiveNode prim = (SchemaNode.PrimitiveNode) field;
        if (!useLogicalType) {
            return formatPhysical(reader, fieldIndex);
        }
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

    /// Multi-line, fully-expanded variant — no element-count caps and no
    /// depth caps; nested types render with one entry per line and indented
    /// children. Used by the dive record modal's inline-expansion path so
    /// users can read the full value, no `…+N` ellipses.
    public static String formatExpanded(RowReader reader, int fieldIndex, SchemaNode field,
                                        boolean useLogicalType) {
        if (reader.isNull(fieldIndex)) {
            return "null";
        }
        if (field instanceof SchemaNode.GroupNode) {
            return formatNestedPretty(reader.getValue(fieldIndex), 0);
        }
        // For primitive leaves the expanded form is identical to the
        // single-line logical / physical rendering.
        return format(reader, fieldIndex, field, useLogicalType);
    }

    private static String formatNestedPretty(Object value, int indent) {
        if (value == null) {
            return "null";
        }
        if (value instanceof PqList list) {
            return prettyList(list, indent);
        }
        if (value instanceof PqStruct struct) {
            return prettyStruct(struct, indent);
        }
        if (value instanceof PqMap map) {
            return prettyMap(map, indent);
        }
        if (value instanceof PqVariant variant) {
            return prettyVariant(variant, indent);
        }
        if (value instanceof byte[] bytes) {
            return formatRawBytes(bytes);
        }
        return String.valueOf(value);
    }

    private static String prettyList(PqList list, int indent) {
        if (list.isEmpty()) {
            return "[]";
        }
        StringBuilder sb = new StringBuilder("[\n");
        String childPad = pad(indent + 1);
        boolean first = true;
        for (Object element : list.values()) {
            if (!first) {
                sb.append(",\n");
            }
            sb.append(childPad).append(formatNestedPretty(element, indent + 1));
            first = false;
        }
        sb.append("\n").append(pad(indent)).append("]");
        return sb.toString();
    }

    private static String prettyStruct(PqStruct struct, int indent) {
        int count = struct.getFieldCount();
        if (count == 0) {
            return "{}";
        }
        StringBuilder sb = new StringBuilder("{\n");
        String childPad = pad(indent + 1);
        for (int i = 0; i < count; i++) {
            String fieldName = struct.getFieldName(i);
            Object fieldValue = struct.isNull(fieldName) ? null : struct.getValue(fieldName);
            sb.append(childPad).append(fieldName).append(": ")
                    .append(formatNestedPretty(fieldValue, indent + 1));
            if (i < count - 1) {
                sb.append(",");
            }
            sb.append("\n");
        }
        sb.append(pad(indent)).append("}");
        return sb.toString();
    }

    private static String prettyMap(PqMap map, int indent) {
        if (map.isEmpty()) {
            return "{}";
        }
        StringBuilder sb = new StringBuilder("{\n");
        String childPad = pad(indent + 1);
        java.util.List<PqMap.Entry> entries = map.getEntries();
        for (int i = 0; i < entries.size(); i++) {
            PqMap.Entry entry = entries.get(i);
            sb.append(childPad)
                    .append(formatNestedPretty(entry.getKey(), indent + 1))
                    .append(": ")
                    .append(formatNestedPretty(entry.isValueNull() ? null : entry.getValue(), indent + 1));
            if (i < entries.size() - 1) {
                sb.append(",");
            }
            sb.append("\n");
        }
        sb.append(pad(indent)).append("}");
        return sb.toString();
    }

    private static String prettyVariant(PqVariant variant, int indent) {
        VariantType type = variant.type();
        return switch (type) {
            case OBJECT -> prettyVariantObject(variant.asObject(), indent);
            case ARRAY -> prettyVariantArray(variant.asArray(), indent);
            // Primitives use the single-line form.
            default -> formatVariant(variant, indent);
        };
    }

    private static String prettyVariantObject(PqVariantObject obj, int indent) {
        int count = obj.getFieldCount();
        if (count == 0) {
            return "{}";
        }
        StringBuilder sb = new StringBuilder("{\n");
        String childPad = pad(indent + 1);
        for (int i = 0; i < count; i++) {
            String name = obj.getFieldName(i);
            sb.append(childPad).append(name).append(": ")
                    .append(formatNestedPretty(obj.getVariant(name), indent + 1));
            if (i < count - 1) {
                sb.append(",");
            }
            sb.append("\n");
        }
        sb.append(pad(indent)).append("}");
        return sb.toString();
    }

    private static String prettyVariantArray(PqVariantArray array, int indent) {
        int size = array.size();
        if (size == 0) {
            return "[]";
        }
        StringBuilder sb = new StringBuilder("[\n");
        String childPad = pad(indent + 1);
        for (int i = 0; i < size; i++) {
            sb.append(childPad).append(formatNestedPretty(array.get(i), indent + 1));
            if (i < size - 1) {
                sb.append(",");
            }
            sb.append("\n");
        }
        sb.append(pad(indent)).append("]");
        return sb.toString();
    }

    private static String pad(int indent) {
        return "  ".repeat(indent);
    }

    /// Renders a value as its underlying physical-type text. Bypasses
    /// logical-type dispatch — used when the user toggles logical rendering
    /// off to inspect storage form. byte[]s still hex-render so cells aren't
    /// "[B@" — that's not "physical" rendering, just sane fallback.
    private static String formatPhysical(RowReader reader, int fieldIndex) {
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
        return formatDictionaryValue(rawValue, col, true);
    }

    /// Logical-type-aware variant of [#formatDictionaryValue]. When
    /// `useLogicalType=false` the column's logical type is bypassed —
    /// timestamps render as raw long micros, decimals as raw byte hex,
    /// etc. Useful for inspecting the storage form on the dictionary
    /// screen.
    public static String formatDictionaryValue(Object rawValue, ColumnSchema col,
                                                boolean useLogicalType) {
        LogicalType lt = useLogicalType ? col.logicalType() : null;
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

    private static final int MAX_NESTED_ELEMENTS = 3;
    private static final int MAX_NESTED_DEPTH = 3;

    /// Renders a nested value (`PqList`, `PqStruct`, `PqMap`, `PqVariant`,
    /// `byte[]`, or any other [Object]) as compact JSON-like text. Capped at
    /// [#MAX_NESTED_ELEMENTS] visible entries per collection and
    /// [#MAX_NESTED_DEPTH] levels of recursion — the screen further truncates
    /// the result to the cell width budget.
    private static String formatNested(Object value, int depth) {
        if (value == null) {
            return "null";
        }
        if (depth >= MAX_NESTED_DEPTH) {
            return "…";
        }
        if (value instanceof PqList list) {
            return formatList(list, depth);
        }
        if (value instanceof PqStruct struct) {
            return formatStruct(struct, depth);
        }
        if (value instanceof PqMap map) {
            return formatMap(map, depth);
        }
        if (value instanceof PqVariant variant) {
            return formatVariant(variant, depth);
        }
        if (value instanceof byte[] bytes) {
            return formatRawBytes(bytes);
        }
        return String.valueOf(value);
    }

    private static String formatList(PqList list, int depth) {
        if (list.isEmpty()) {
            return "[]";
        }
        StringBuilder sb = new StringBuilder("[");
        int shown = 0;
        for (Object element : list.values()) {
            if (shown == MAX_NESTED_ELEMENTS) {
                sb.append(", …+").append(list.size() - MAX_NESTED_ELEMENTS);
                break;
            }
            if (shown > 0) {
                sb.append(", ");
            }
            sb.append(formatNested(element, depth + 1));
            shown++;
        }
        sb.append("]");
        return sb.toString();
    }

    private static String formatStruct(PqStruct struct, int depth) {
        int count = struct.getFieldCount();
        if (count == 0) {
            return "{}";
        }
        StringBuilder sb = new StringBuilder("{");
        int shown = 0;
        for (int i = 0; i < count; i++) {
            if (shown == MAX_NESTED_ELEMENTS) {
                sb.append(", …+").append(count - MAX_NESTED_ELEMENTS);
                break;
            }
            if (shown > 0) {
                sb.append(", ");
            }
            String fieldName = struct.getFieldName(i);
            Object fieldValue = struct.isNull(fieldName) ? null : struct.getValue(fieldName);
            sb.append(fieldName).append(": ").append(formatNested(fieldValue, depth + 1));
            shown++;
        }
        sb.append("}");
        return sb.toString();
    }

    private static String formatMap(PqMap map, int depth) {
        if (map.isEmpty()) {
            return "{}";
        }
        StringBuilder sb = new StringBuilder("{");
        int shown = 0;
        java.util.List<PqMap.Entry> entries = map.getEntries();
        for (PqMap.Entry entry : entries) {
            if (shown == MAX_NESTED_ELEMENTS) {
                sb.append(", …+").append(entries.size() - MAX_NESTED_ELEMENTS);
                break;
            }
            if (shown > 0) {
                sb.append(", ");
            }
            sb.append(formatNested(entry.getKey(), depth + 1))
                    .append(": ")
                    .append(formatNested(entry.isValueNull() ? null : entry.getValue(), depth + 1));
            shown++;
        }
        sb.append("}");
        return sb.toString();
    }

    private static String formatVariant(PqVariant variant, int depth) {
        VariantType type = variant.type();
        return switch (type) {
            case NULL -> "null";
            case BOOLEAN_TRUE -> "true";
            case BOOLEAN_FALSE -> "false";
            case INT8, INT16, INT32 -> Integer.toString(variant.asInt());
            case INT64 -> Long.toString(variant.asLong());
            case FLOAT -> Float.toString(variant.asFloat());
            case DOUBLE -> Double.toString(variant.asDouble());
            case DECIMAL4, DECIMAL8, DECIMAL16 -> variant.asDecimal().toPlainString();
            case DATE -> variant.asDate().toString();
            case TIME_NTZ -> variant.asTime().toString();
            case TIMESTAMP, TIMESTAMP_NANOS -> variant.asTimestamp().toString();
            case TIMESTAMP_NTZ, TIMESTAMP_NTZ_NANOS -> {
                String s = variant.asTimestamp().toString();
                yield s.endsWith("Z") ? s.substring(0, s.length() - 1) : s;
            }
            case STRING -> variant.asString();
            case BINARY -> formatRawBytes(variant.asBinary());
            case UUID -> variant.asUuid().toString();
            case OBJECT -> formatVariantObject(variant.asObject(), depth);
            case ARRAY -> formatVariantArray(variant.asArray(), depth);
        };
    }

    private static String formatVariantObject(PqVariantObject obj, int depth) {
        int count = obj.getFieldCount();
        if (count == 0) {
            return "{}";
        }
        StringBuilder sb = new StringBuilder("{");
        int shown = 0;
        for (int i = 0; i < count; i++) {
            if (shown == MAX_NESTED_ELEMENTS) {
                sb.append(", …+").append(count - MAX_NESTED_ELEMENTS);
                break;
            }
            if (shown > 0) {
                sb.append(", ");
            }
            String name = obj.getFieldName(i);
            sb.append(name).append(": ").append(formatNested(obj.getVariant(name), depth + 1));
            shown++;
        }
        sb.append("}");
        return sb.toString();
    }

    private static String formatVariantArray(PqVariantArray array, int depth) {
        int size = array.size();
        if (size == 0) {
            return "[]";
        }
        StringBuilder sb = new StringBuilder("[");
        int shown = 0;
        for (int i = 0; i < size; i++) {
            if (shown == MAX_NESTED_ELEMENTS) {
                sb.append(", …+").append(size - MAX_NESTED_ELEMENTS);
                break;
            }
            if (shown > 0) {
                sb.append(", ");
            }
            sb.append(formatNested(array.get(i), depth + 1));
            shown++;
        }
        sb.append("]");
        return sb.toString();
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
