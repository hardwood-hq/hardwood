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
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqInterval;
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
    ///
    /// Exhaustive over the sealed [LogicalType] hierarchy: a new subtype
    /// fails to compile here until an explicit case is added, preventing
    /// silent fall-through to the raw-bytes path.
    public static String format(RowReader reader, int fieldIndex, SchemaNode field, boolean useLogicalType) {
        if (reader.isNull(fieldIndex)) {
            return "null";
        }
        if (field instanceof SchemaNode.GroupNode) {
            // Nested group — render structurally rather than letting the JVM's
            // default `Object.toString()` print "dev.hardwood.internal...".
            // `reader.getValue` and `getRawValue` return the same flyweight for
            // groups; the toggle only changes how primitive leaves inside the
            // group are rendered, which `formatNested` re-dispatches on.
            return formatNested(reader.getValue(fieldIndex), 0, useLogicalType);
        }
        SchemaNode.PrimitiveNode prim = (SchemaNode.PrimitiveNode) field;
        if (!useLogicalType) {
            return formatPhysical(reader, fieldIndex);
        }
        LogicalType lt = prim.logicalType();
        return switch (lt) {
            case null -> formatPhysical(reader, fieldIndex);
            case LogicalType.TimestampType ts -> formatTimestamp(reader.getTimestamp(fieldIndex), ts);
            case LogicalType.DateType d -> reader.getDate(fieldIndex).toString();
            case LogicalType.TimeType t -> reader.getTime(fieldIndex).toString();
            case LogicalType.DecimalType dec -> reader.getDecimal(fieldIndex).toPlainString();
            case LogicalType.UuidType u -> reader.getUuid(fieldIndex).toString();
            case LogicalType.StringType s -> reader.getString(fieldIndex);
            case LogicalType.EnumType e -> reader.getString(fieldIndex);
            case LogicalType.JsonType j -> reader.getString(fieldIndex);
            case LogicalType.BsonType b -> reader.getString(fieldIndex);
            case LogicalType.IntType it when !it.isSigned() -> formatUnsignedInt(reader, fieldIndex, prim);
            // Signed IntType still goes through getRawValue / String.valueOf —
            // matches the pre-refactor behavior.
            case LogicalType.IntType it -> formatPhysical(reader, fieldIndex);
            case LogicalType.IntervalType i -> formatInterval(reader.getInterval(fieldIndex));
            // FLOAT16 was previously hex-rendered because nothing matched the
            // logical type and getRawValue returns the FLBA(2) bytes; getFloat
            // handles the half→single widening transparently so 1.5 prints as
            // "1.5" instead of "0x003c".
            case LogicalType.Float16Type f16 -> Float.toString(reader.getFloat(fieldIndex));
            // Geometry / Geography carry opaque WKB / WKT binary payloads with
            // no decoder yet — hex-render explicitly rather than relying on a
            // raw-bytes fall-through.
            case LogicalType.GeometryType g -> formatPhysical(reader, fieldIndex);
            case LogicalType.GeographyType g -> formatPhysical(reader, fieldIndex);
            // NullType columns are all-null; the `isNull` short-circuit above
            // means a non-null value here would be a malformed-file signal.
            case LogicalType.NullType n -> throw structuralReached(field, lt);
            // Structural / self-describing logical types are carried on group
            // nodes and short-circuited above; reaching them here means the
            // schema is malformed.
            case LogicalType.ListType l -> throw structuralReached(field, lt);
            case LogicalType.MapType m -> throw structuralReached(field, lt);
            case LogicalType.VariantType v -> throw structuralReached(field, lt);
        };
    }

    private static String formatUnsignedInt(RowReader reader, int fieldIndex, SchemaNode.PrimitiveNode prim) {
        long raw = switch (prim.type()) {
            case INT32 -> Integer.toUnsignedLong(reader.getInt(fieldIndex));
            case INT64 -> reader.getLong(fieldIndex);
            default -> ((Number) reader.getRawValue(fieldIndex)).longValue();
        };
        return Long.toUnsignedString(raw);
    }

    private static IllegalStateException structuralReached(SchemaNode field, LogicalType lt) {
        return new IllegalStateException(
                "Group logical type " + lt + " reached primitive formatter path on field '"
                        + field.name() + "'");
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
            return formatNestedPretty(reader.getValue(fieldIndex), 0, useLogicalType);
        }
        // For primitive leaves the expanded form is identical to the
        // single-line logical / physical rendering.
        return format(reader, fieldIndex, field, useLogicalType);
    }

    private static String formatNestedPretty(Object value, int indent, boolean useLogicalType) {
        if (value == null) {
            return "null";
        }
        if (value instanceof PqList list) {
            return prettyList(list, indent, useLogicalType);
        }
        if (value instanceof PqStruct struct) {
            return prettyStruct(struct, indent, useLogicalType);
        }
        if (value instanceof PqMap map) {
            return prettyMap(map, indent, useLogicalType);
        }
        if (value instanceof PqVariant variant) {
            return prettyVariant(variant, indent, useLogicalType);
        }
        if (value instanceof byte[] bytes) {
            return formatRawBytes(bytes);
        }
        if (value instanceof PqInterval interval) {
            return formatInterval(interval);
        }
        if (value instanceof BigDecimal decimal) {
            return decimal.toPlainString();
        }
        if (value instanceof Instant instant) {
            return instant.toString();
        }
        return String.valueOf(value);
    }

    private static String prettyList(PqList list, int indent, boolean useLogicalType) {
        if (list.isEmpty()) {
            return "[]";
        }
        StringBuilder sb = new StringBuilder("[\n");
        String childPad = pad(indent + 1);
        // Indexed iteration so a null element is rendered as `null` via isNull(i)
        // — `list.values()` skips that distinction. List elements have no raw
        // form distinct from get(i), so `useLogicalType` only affects nested
        // primitives reached through formatNestedPretty.
        for (int i = 0; i < list.size(); i++) {
            if (i > 0) {
                sb.append(",\n");
            }
            Object element = list.isNull(i) ? null : list.get(i);
            sb.append(childPad).append(formatNestedPretty(element, indent + 1, useLogicalType));
        }
        sb.append("\n").append(pad(indent)).append("]");
        return sb.toString();
    }

    private static String prettyStruct(PqStruct struct, int indent, boolean useLogicalType) {
        int count = struct.getFieldCount();
        if (count == 0) {
            return "{}";
        }
        StringBuilder sb = new StringBuilder("{\n");
        String childPad = pad(indent + 1);
        for (int i = 0; i < count; i++) {
            String fieldName = struct.getFieldName(i);
            Object fieldValue = struct.isNull(fieldName) ? null
                    : (useLogicalType ? struct.getValue(fieldName) : struct.getRawValue(fieldName));
            sb.append(childPad).append(fieldName).append(": ")
                    .append(formatNestedPretty(fieldValue, indent + 1, useLogicalType));
            if (i < count - 1) {
                sb.append(",");
            }
            sb.append("\n");
        }
        sb.append(pad(indent)).append("}");
        return sb.toString();
    }

    private static String prettyMap(PqMap map, int indent, boolean useLogicalType) {
        if (map.isEmpty()) {
            return "{}";
        }
        StringBuilder sb = new StringBuilder("{\n");
        String childPad = pad(indent + 1);
        java.util.List<PqMap.Entry> entries = map.getEntries();
        for (int i = 0; i < entries.size(); i++) {
            PqMap.Entry entry = entries.get(i);
            Object key = useLogicalType ? entry.getKey() : entry.getRawKey();
            Object value = entry.isValueNull() ? null
                    : (useLogicalType ? entry.getValue() : entry.getRawValue());
            sb.append(childPad)
                    .append(formatNestedPretty(key, indent + 1, useLogicalType))
                    .append(": ")
                    .append(formatNestedPretty(value, indent + 1, useLogicalType));
            if (i < entries.size() - 1) {
                sb.append(",");
            }
            sb.append("\n");
        }
        sb.append(pad(indent)).append("}");
        return sb.toString();
    }

    private static String prettyVariant(PqVariant variant, int indent, boolean useLogicalType) {
        VariantType type = variant.type();
        return switch (type) {
            case OBJECT -> prettyVariantObject(variant.asObject(), indent, useLogicalType);
            case ARRAY -> prettyVariantArray(variant.asArray(), indent, useLogicalType);
            // Primitives use the single-line form.
            default -> formatVariant(variant, indent, useLogicalType);
        };
    }

    private static String prettyVariantObject(PqVariantObject obj, int indent, boolean useLogicalType) {
        int count = obj.getFieldCount();
        if (count == 0) {
            return "{}";
        }
        StringBuilder sb = new StringBuilder("{\n");
        String childPad = pad(indent + 1);
        for (int i = 0; i < count; i++) {
            String name = obj.getFieldName(i);
            sb.append(childPad).append(name).append(": ")
                    .append(formatNestedPretty(obj.getVariant(name), indent + 1, useLogicalType));
            if (i < count - 1) {
                sb.append(",");
            }
            sb.append("\n");
        }
        sb.append(pad(indent)).append("}");
        return sb.toString();
    }

    private static String prettyVariantArray(PqVariantArray array, int indent, boolean useLogicalType) {
        int size = array.size();
        if (size == 0) {
            return "[]";
        }
        StringBuilder sb = new StringBuilder("[\n");
        String childPad = pad(indent + 1);
        for (int i = 0; i < size; i++) {
            sb.append(childPad).append(formatNestedPretty(array.get(i), indent + 1, useLogicalType));
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
        Object raw = reader.getRawValue(fieldIndex);
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

    /// INT32 dictionary entries can only carry logical types backed by `INT32`:
    /// `DATE`, `TIME(MILLIS)`, `DECIMAL(precision ≤ 9)`, and the `INT(8|16|32)`
    /// family. The `default` arm fails fast on any other logical type — it's
    /// either physically incompatible (the caller mismatched primitive/logical)
    /// or a brand-new sealed subtype whose handling hasn't been considered.
    private static String formatInt(int raw, LogicalType lt) {
        return switch (lt) {
            case null -> Integer.toString(raw);
            case LogicalType.DateType d -> LocalDate.ofEpochDay(raw).toString();
            case LogicalType.TimeType t -> formatTime(raw, t.unit());
            case LogicalType.IntType it when !it.isSigned() -> Long.toString(Integer.toUnsignedLong(raw));
            case LogicalType.IntType it -> Integer.toString(raw);
            case LogicalType.DecimalType d -> new BigDecimal(BigInteger.valueOf(raw), d.scale()).toPlainString();
            default -> throw notBackedBy(lt, "INT32");
        };
    }

    /// INT64 dictionary entries can only carry logical types backed by `INT64`:
    /// `TIMESTAMP`, `TIME(MICROS|NANOS)`, `DECIMAL(10 ≤ precision ≤ 18)`, and the
    /// `INT_64` logical type. See [#formatInt] for the `default` rationale.
    private static String formatLong(long raw, LogicalType lt) {
        return switch (lt) {
            case null -> Long.toString(raw);
            case LogicalType.TimestampType ts -> formatTimestamp(rawToInstant(raw, ts.unit()), ts);
            case LogicalType.TimeType t -> formatTime(raw, t.unit());
            case LogicalType.IntType it when !it.isSigned() -> Long.toUnsignedString(raw);
            case LogicalType.IntType it -> Long.toString(raw);
            case LogicalType.DecimalType d -> new BigDecimal(BigInteger.valueOf(raw), d.scale()).toPlainString();
            default -> throw notBackedBy(lt, "INT64");
        };
    }

    /// `BYTE_ARRAY` / `FIXED_LEN_BYTE_ARRAY` dictionary entries can carry the
    /// byte-backed logical types — strings, BSON, UUID(16), INTERVAL(12),
    /// FLOAT16(2), DECIMAL, plus Geometry / Geography WKB. See [#formatInt]
    /// for the `default` rationale.
    private static String formatBytes(byte[] raw, LogicalType lt) {
        return switch (lt) {
            case null -> formatRawBytes(raw);
            case LogicalType.StringType s -> new String(raw, StandardCharsets.UTF_8);
            case LogicalType.EnumType e -> new String(raw, StandardCharsets.UTF_8);
            case LogicalType.JsonType j -> new String(raw, StandardCharsets.UTF_8);
            case LogicalType.BsonType b -> new String(raw, StandardCharsets.UTF_8);
            case LogicalType.DecimalType d -> new BigDecimal(new BigInteger(raw), d.scale()).toPlainString();
            case LogicalType.UuidType u when raw.length == 16 -> {
                ByteBuffer bb = ByteBuffer.wrap(raw);
                yield new UUID(bb.getLong(), bb.getLong()).toString();
            }
            case LogicalType.UuidType u -> formatRawBytes(raw);
            case LogicalType.IntervalType i when raw.length == 12 -> formatIntervalBytes(raw);
            case LogicalType.IntervalType i -> formatRawBytes(raw);
            case LogicalType.Float16Type f when raw.length == 2 ->
                    Float.toString(LogicalTypeConverter.convertToFloat16(raw, PhysicalType.FIXED_LEN_BYTE_ARRAY));
            case LogicalType.Float16Type f -> formatRawBytes(raw);
            case LogicalType.GeometryType g -> formatRawBytes(raw);
            case LogicalType.GeographyType g -> formatRawBytes(raw);
            default -> throw notBackedBy(lt, "BYTE_ARRAY");
        };
    }

    private static IllegalStateException notBackedBy(LogicalType lt, String physical) {
        return new IllegalStateException(
                "Logical type " + lt + " is not backed by " + physical
                        + "; dictionary value of mismatched primitive type passed");
    }

    /// Decode a 12-byte FIXED_LEN_BYTE_ARRAY INTERVAL payload (as used in
    /// page/dictionary stats) and render it via [#formatInterval(PqInterval)].
    public static String formatIntervalBytes(byte[] bytes) {
        return formatInterval(LogicalTypeConverter.convertToInterval(bytes, PhysicalType.FIXED_LEN_BYTE_ARRAY));
    }

    public static String formatInterval(PqInterval interval) {
        if (interval == null) {
            return "null";
        }
        if (interval.months() == 0 && interval.days() == 0 && interval.milliseconds() == 0) {
            return "0ms";
        }
        StringBuilder sb = new StringBuilder();
        if (interval.months() != 0) {
            sb.append(interval.months()).append("mo");
        }
        if (interval.days() != 0) {
            if (!sb.isEmpty()) {
                sb.append(' ');
            }
            sb.append(interval.days()).append('d');
        }
        if (interval.milliseconds() != 0) {
            if (!sb.isEmpty()) {
                sb.append(' ');
            }
            sb.append(interval.milliseconds()).append("ms");
        }
        return sb.toString();
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
    private static String formatNested(Object value, int depth, boolean useLogicalType) {
        if (value == null) {
            return "null";
        }
        if (depth >= MAX_NESTED_DEPTH) {
            return "…";
        }
        if (value instanceof PqList list) {
            return formatList(list, depth, useLogicalType);
        }
        if (value instanceof PqStruct struct) {
            return formatStruct(struct, depth, useLogicalType);
        }
        if (value instanceof PqMap map) {
            return formatMap(map, depth, useLogicalType);
        }
        if (value instanceof PqVariant variant) {
            return formatVariant(variant, depth, useLogicalType);
        }
        if (value instanceof byte[] bytes) {
            return formatRawBytes(bytes);
        }
        if (value instanceof PqInterval interval) {
            return formatInterval(interval);
        }
        if (value instanceof BigDecimal decimal) {
            return decimal.toPlainString();
        }
        if (value instanceof Instant instant) {
            return instant.toString();
        }
        return String.valueOf(value);
    }

    private static String formatList(PqList list, int depth, boolean useLogicalType) {
        if (list.isEmpty()) {
            return "[]";
        }
        StringBuilder sb = new StringBuilder("[");
        int shown = 0;
        int size = list.size();
        for (int i = 0; i < size; i++) {
            if (shown == MAX_NESTED_ELEMENTS) {
                sb.append(", …+").append(size - MAX_NESTED_ELEMENTS);
                break;
            }
            if (shown > 0) {
                sb.append(", ");
            }
            Object element = list.isNull(i) ? null : list.get(i);
            sb.append(formatNested(element, depth + 1, useLogicalType));
            shown++;
        }
        sb.append("]");
        return sb.toString();
    }

    private static String formatStruct(PqStruct struct, int depth, boolean useLogicalType) {
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
            Object fieldValue = struct.isNull(fieldName) ? null
                    : (useLogicalType ? struct.getValue(fieldName) : struct.getRawValue(fieldName));
            sb.append(fieldName).append(": ").append(formatNested(fieldValue, depth + 1, useLogicalType));
            shown++;
        }
        sb.append("}");
        return sb.toString();
    }

    private static String formatMap(PqMap map, int depth, boolean useLogicalType) {
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
            Object key = useLogicalType ? entry.getKey() : entry.getRawKey();
            Object value = entry.isValueNull() ? null
                    : (useLogicalType ? entry.getValue() : entry.getRawValue());
            sb.append(formatNested(key, depth + 1, useLogicalType))
                    .append(": ")
                    .append(formatNested(value, depth + 1, useLogicalType));
            shown++;
        }
        sb.append("}");
        return sb.toString();
    }

    private static String formatVariant(PqVariant variant, int depth, boolean useLogicalType) {
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
            case OBJECT -> formatVariantObject(variant.asObject(), depth, useLogicalType);
            case ARRAY -> formatVariantArray(variant.asArray(), depth, useLogicalType);
        };
    }

    private static String formatVariantObject(PqVariantObject obj, int depth, boolean useLogicalType) {
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
            sb.append(name).append(": ").append(formatNested(obj.getVariant(name), depth + 1, useLogicalType));
            shown++;
        }
        sb.append("}");
        return sb.toString();
    }

    private static String formatVariantArray(PqVariantArray array, int depth, boolean useLogicalType) {
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
            sb.append(formatNested(array.get(i), depth + 1, useLogicalType));
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
