/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.internal;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

import dev.hardwood.internal.conversion.LogicalTypeConverter;
import dev.hardwood.internal.predicate.StatisticsDecoder;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqInterval;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
import dev.hardwood.row.PqVariant;
import dev.hardwood.row.PqVariantArray;
import dev.hardwood.row.PqVariantObject;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.SchemaNode;

/// Canonical rendering of Parquet values for every CLI surface — `print`,
/// `convert`, the `inspect` commands and the `dive` TUI. A logical type spells
/// the same text from every source: ISO-8601 timestamps, `yyyy-MM-dd` dates,
/// plain-string decimals and canonical UUIDs.
///
/// The entry points differ in where the value comes from:
///
/// - [#formatReader(RowReader, int, SchemaNode, boolean, Style, int)] — typed
///   accessors on a [RowReader]: dive preview cells, the dive record modal and
///   `convert --format json` leaves.
/// - [#formatValue(Object, SchemaNode, Style, int)] — a materialised value:
///   `print` cells and `convert` CSV cells.
/// - [#formatDictionary(Object, ColumnSchema, boolean, int)] — a raw primitive
///   out of a parsed `Dictionary`.
/// - `formatBytes` — raw statistics bytes.
/// - `formatDecoded` — an already-decoded dictionary entry.
///
/// `formatReader` and `formatValue` render nested values through one walker, so
/// a struct, list, map or Variant reads the same from both; [Style] picks the
/// layout, whether control characters are sanitised, and whether nested values
/// are written as JSON. Statistics and
/// dictionary entries are display surfaces: a fixed-width payload of the wrong
/// length renders there as `0x` hex, while the value paths throw on it.
///
/// Budgets are measured in terminal display cells; [BinaryValues#NO_LIMIT]
/// renders the whole value. Text budgets never cut — a caller that truncates
/// applies [Strings#truncateRight] and marks the cut — but they do bound hex
/// building for binary payloads.
public final class ValueFormatter {

    /// The widest a dive preview cell can be. The rendered rows are cached ahead of
    /// layout, so the terminal's actual width is not available here and cannot
    /// become part of the cache key; this is simply wider than any terminal, so
    /// it never clips a cell that would have been shown and still holds a
    /// multi-megabyte payload to a few kilobytes of hex.
    public static final int PREVIEW_CELL_BUDGET = 4096;

    private static final int MAX_NESTED_ELEMENTS = 3;
    private static final int MAX_NESTED_DEPTH = 3;

    private static final int UUID_LENGTH = 16;
    private static final int INTERVAL_LENGTH = 12;
    private static final int INT96_LENGTH = 12;
    private static final int FLOAT16_LENGTH = 2;

    private ValueFormatter() {
    }

    /// How a rendered value is laid out, and whether its text may carry control
    /// characters. The one-line styles write nested values in the display
    /// grammar: `{ a : 1 }` for structs, maps and Variant objects, `[1, 2]` for
    /// lists and Variant arrays.
    public enum Style {
        /// One line with every element; control characters sanitised. `print` cells.
        COMPACT,
        /// One line with at most three entries per collection, the rest marked
        /// `…+N`, and three levels of nesting, deeper values marked `…`; control
        /// characters sanitised. Dive preview cells.
        PREVIEW,
        /// One entry per line, indented two spaces per level; control characters
        /// sanitised. The dive record modal.
        EXPANDED,
        /// One line with every element, nested values as JSON: structs and maps
        /// as objects, lists as arrays, leaves typed as `isJsonScalar` decides.
        /// Strings are verbatim; a top-level leaf is plain text for the export
        /// format to quote. `convert` output.
        EXPORT
    }

    // ==================== reader source ====================

    /// Renders the field at `fieldIndex` through the reader's typed accessors.
    /// `useLogicalType=true` renders timestamps, decimals, UUIDs, etc. in their
    /// canonical logical form; `useLogicalType=false` renders the stored physical
    /// value (e.g. `1735689600000000` instead of `2025-01-01T00:00:00Z`). Groups
    /// and repeated fields render structurally in both modes; the toggle decides
    /// how their leaves are read.
    public static String formatReader(RowReader reader, int fieldIndex, SchemaNode field,
                                      boolean useLogicalType, Style style, int budget) {
        Objects.requireNonNull(reader, "reader");
        Objects.requireNonNull(field, "field");
        Objects.requireNonNull(style, "style");
        requireBudget(budget);
        if (reader.isNull(fieldIndex)) {
            return "null";
        }
        if (isNested(field)) {
            // `getValue` and `getRawValue` return the same flyweight for a group or list.
            return render(reader.getValue(fieldIndex), field, useLogicalType, style, 0, budget);
        }
        SchemaNode.PrimitiveNode prim = (SchemaNode.PrimitiveNode) field;
        if (!useLogicalType) {
            return formatPhysical(reader, fieldIndex, budget);
        }
        if (prim.type() == PhysicalType.INT96) {
            return reader.getTimestamp(fieldIndex).toString();
        }
        LogicalType lt = prim.logicalType();
        return switch (lt) {
            case null -> formatPhysical(reader, fieldIndex, budget);
            case LogicalType.TimestampType ts -> ts.isAdjustedToUTC()
                    ? reader.getTimestamp(fieldIndex).toString()
                    : reader.getLocalTimestamp(fieldIndex).toString();
            case LogicalType.DateType d -> reader.getDate(fieldIndex).toString();
            case LogicalType.TimeType t -> reader.getTime(fieldIndex).toString();
            case LogicalType.DecimalType dec -> reader.getDecimal(fieldIndex).toPlainString();
            case LogicalType.UuidType u -> reader.getUuid(fieldIndex).toString();
            case LogicalType.StringType s -> text(reader.getString(fieldIndex), style);
            case LogicalType.EnumType e -> text(reader.getString(fieldIndex), style);
            case LogicalType.JsonType j -> text(reader.getString(fieldIndex), style);
            case LogicalType.BsonType b -> text(reader.getString(fieldIndex), style);
            case LogicalType.IntType it when !it.isSigned() -> formatUnsignedInt(reader, fieldIndex, prim);
            case LogicalType.IntType it -> formatPhysical(reader, fieldIndex, budget);
            case LogicalType.IntervalType i -> formatInterval(reader.getInterval(fieldIndex));
            // getFloat widens the two-byte half-precision payload.
            case LogicalType.Float16Type f16 -> Float.toString(reader.getFloat(fieldIndex));
            // Geometry and geography carry WKB payloads with no decoder: they render as bytes.
            case LogicalType.GeometryType g -> formatPhysical(reader, fieldIndex, budget);
            case LogicalType.GeographyType g -> formatPhysical(reader, fieldIndex, budget);
            // A NULL column holds only nulls, which the check above has returned.
            case LogicalType.NullType n -> throw structuralReached(field, lt);
            // These annotate group nodes, which the check above has walked.
            case LogicalType.ListType l -> throw structuralReached(field, lt);
            case LogicalType.MapType m -> throw structuralReached(field, lt);
            case LogicalType.VariantType v -> throw structuralReached(field, lt);
        };
    }

    private static String formatUnsignedInt(RowReader reader, int fieldIndex, SchemaNode.PrimitiveNode prim) {
        return switch (prim.type()) {
            case INT32 -> Long.toString(Integer.toUnsignedLong(reader.getInt(fieldIndex)));
            case INT64 -> Long.toUnsignedString(reader.getLong(fieldIndex));
            default -> throw new IllegalStateException("Field '" + prim.name() + "' has an unsigned INT annotation on "
                    + prim.type() + ", which only INT32 and INT64 can carry");
        };
    }

    /// The stored physical value: a byte array through [BinaryValues], anything
    /// else as its Java text.
    private static String formatPhysical(RowReader reader, int fieldIndex, int budget) {
        Object raw = reader.getRawValue(fieldIndex);
        return raw instanceof byte[] bytes ? BinaryValues.render(bytes, budget) : String.valueOf(raw);
    }

    private static IllegalStateException structuralReached(SchemaNode field, LogicalType lt) {
        return new IllegalStateException(
                "Group logical type " + lt + " reached primitive formatter path on field '"
                        + field.name() + "'");
    }

    // ==================== materialised source ====================

    /// Renders a materialised value — whatever the reader's `getValue` hands back
    /// for one field. `field` is the value's own schema node; it decides how
    /// byte-backed leaves decode (annotated strings, UUID, decimal, INTERVAL,
    /// INT96) and whether integers render unsigned. A `null` or non-group schema
    /// walks a nested value schema-less, the way legacy list and map layouts
    /// without resolvable child schemas render. A group schema paired with a
    /// scalar value throws, and so does a UUID, INTERVAL or INT96 payload of the
    /// wrong length.
    public static String formatValue(Object value, SchemaNode field, Style style, int budget) {
        Objects.requireNonNull(style, "style");
        requireBudget(budget);
        return render(value, field, true, style, 0, budget);
    }

    // ==================== the nested walker ====================

    /// Renders `value` against its schema node: a `PqStruct`, `PqList`, `PqMap`
    /// or `PqVariant` entry by entry, anything else as a leaf.
    /// `useLogicalType=false` reads struct fields and map entries through their
    /// raw accessors. In `EXPORT` a nested value is JSON, and a leaf inside one
    /// a JSON value.
    private static String render(Object value, SchemaNode schema, boolean useLogicalType, Style style,
                                 int depth, int budget) {
        if (value == null) {
            return "null";
        }
        if (style == Style.PREVIEW && depth >= MAX_NESTED_DEPTH) {
            return String.valueOf(Strings.ELLIPSIS);
        }
        return switch (value) {
            case PqVariant variant -> style == Style.EXPORT
                    ? variantJson(variant)
                    : renderVariant(variant, style, depth, budget);
            case PqStruct struct -> renderStruct(struct, asGroup(schema), useLogicalType, style, depth, budget);
            case PqList list -> renderList(list, listElementSchema(schema), useLogicalType, style, depth, budget);
            case PqMap map -> renderMap(map, asGroup(schema), useLogicalType, style, depth, budget);
            default -> style == Style.EXPORT && depth > 0
                    ? jsonLeaf(value, schema, useLogicalType, budget)
                    : renderLeaf(value, schema, useLogicalType, style, budget);
        };
    }

    private static String renderStruct(PqStruct struct, SchemaNode.GroupNode schema, boolean useLogicalType,
                                       Style style, int depth, int budget) {
        int count = struct.getFieldCount();
        int shown = shownEntries(count, style);
        List<String> entries = new ArrayList<>(shown);
        for (int i = 0; i < shown; i++) {
            String name = struct.getFieldName(i);
            Object fieldValue = struct.isNull(name) ? null
                    : useLogicalType ? struct.getValue(name) : struct.getRawValue(name);
            entries.add(entry(style == Style.EXPORT ? jsonString(name) : text(name, style),
                    render(fieldValue, findChildSchema(schema, name), useLogicalType, style, depth + 1, budget),
                    style));
        }
        return enclose(true, entries, count, style, depth);
    }

    private static String renderList(PqList list, SchemaNode elementSchema, boolean useLogicalType,
                                     Style style, int depth, int budget) {
        int size = list.size();
        int shown = shownEntries(size, style);
        List<String> entries = new ArrayList<>(shown);
        for (int i = 0; i < shown; i++) {
            Object element = list.isNull(i) ? null : useLogicalType ? list.get(i) : list.getRaw(i);
            entries.add(render(element, elementSchema, useLogicalType, style, depth + 1, budget));
        }
        return enclose(false, entries, size, style, depth);
    }

    private static String renderMap(PqMap map, SchemaNode.GroupNode schema, boolean useLogicalType,
                                    Style style, int depth, int budget) {
        SchemaNode keySchema = null;
        SchemaNode valueSchema = null;
        if (schema != null && !schema.children().isEmpty()
                && schema.children().get(0) instanceof SchemaNode.GroupNode keyValue
                && keyValue.children().size() >= 2) {
            keySchema = keyValue.children().get(0);
            valueSchema = keyValue.children().get(1);
        }
        List<PqMap.Entry> all = map.getEntries();
        int shown = shownEntries(all.size(), style);
        List<String> entries = new ArrayList<>(shown);
        for (int i = 0; i < shown; i++) {
            PqMap.Entry entry = all.get(i);
            Object key = useLogicalType ? entry.getKey() : entry.getRawKey();
            Object value = entry.isValueNull() ? null
                    : useLogicalType ? entry.getValue() : entry.getRawValue();
            // A JSON object key is a string: the map key's text, whatever its type.
            String keyText = style == Style.EXPORT
                    ? jsonString(render(key, keySchema, useLogicalType, style, 0, budget))
                    : render(key, keySchema, useLogicalType, style, depth + 1, budget);
            entries.add(entry(keyText, render(value, valueSchema, useLogicalType, style, depth + 1, budget), style));
        }
        return enclose(true, entries, all.size(), style, depth);
    }

    private static String renderVariant(PqVariant variant, Style style, int depth, int budget) {
        return switch (variant.type()) {
            case OBJECT -> renderVariantObject(variant.asObject(), style, depth, budget);
            case ARRAY -> renderVariantArray(variant.asArray(), style, depth, budget);
            default -> variantScalarText(variant, style, budget);
        };
    }

    private static String renderVariantObject(PqVariantObject object, Style style, int depth, int budget) {
        int count = object.getFieldCount();
        int shown = shownEntries(count, style);
        List<String> entries = new ArrayList<>(shown);
        for (int i = 0; i < shown; i++) {
            // The raw name looks the value up; only the rendered name is sanitised.
            String name = object.getFieldName(i);
            entries.add(entry(text(name, style),
                    render(object.getVariant(name), null, true, style, depth + 1, budget), style));
        }
        return enclose(true, entries, count, style, depth);
    }

    private static String renderVariantArray(PqVariantArray array, Style style, int depth, int budget) {
        int size = array.size();
        int shown = shownEntries(size, style);
        List<String> entries = new ArrayList<>(shown);
        for (int i = 0; i < shown; i++) {
            entries.add(render(array.get(i), null, true, style, depth + 1, budget));
        }
        return enclose(false, entries, size, style, depth);
    }

    /// One-line text for a Variant scalar: unquoted strings, the same spelling the
    /// display grammar uses everywhere.
    private static String variantScalarText(PqVariant variant, Style style, int budget) {
        return switch (variant.type()) {
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
            case TIMESTAMP, TIMESTAMP_NANOS, TIMESTAMP_NTZ, TIMESTAMP_NTZ_NANOS -> variantTimestampText(variant);
            case STRING -> text(variant.asString(), style);
            case BINARY -> BinaryValues.render(variant.asBinary(), budget);
            case UUID -> variant.asUuid().toString();
            case OBJECT, ARRAY -> throw new IllegalStateException(
                    "Variant " + variant.type() + " is not a scalar: walk it as a collection");
        };
    }

    /// A Variant timestamp without a time zone carries wall-clock time in an
    /// `Instant`; it renders as a local date-time, the same text a `TIMESTAMP`
    /// column not adjusted to UTC shows.
    private static String variantTimestampText(PqVariant variant) {
        return switch (variant.type()) {
            case TIMESTAMP_NTZ, TIMESTAMP_NTZ_NANOS ->
                    LocalDateTime.ofInstant(variant.asTimestamp(), ZoneOffset.UTC).toString();
            default -> variant.asTimestamp().toString();
        };
    }

    private static String renderLeaf(Object value, SchemaNode schema, boolean useLogicalType, Style style,
                                     int budget) {
        if (schema instanceof SchemaNode.GroupNode) {
            throw new IllegalStateException("Field '" + schema.name() + "' is a group in the schema, but the"
                    + " value is a " + value.getClass().getName());
        }
        return switch (value) {
            case byte[] bytes -> useLogicalType
                    ? formatMaterialisedBytes(bytes, schema, style, budget)
                    : BinaryValues.render(bytes, budget);
            case String s -> text(s, style);
            case BigDecimal decimal -> decimal.toPlainString();
            case PqInterval interval -> formatInterval(interval);
            case Integer i when useLogicalType && isUnsigned(schema) -> Long.toString(Integer.toUnsignedLong(i));
            case Long l when useLogicalType && isUnsigned(schema) -> Long.toUnsignedString(l);
            default -> String.valueOf(value);
        };
    }

    /// Renders a byte-backed leaf. Annotated strings decode as UTF-8; UUID,
    /// decimal, INTERVAL and INT96 decode through their converters and throw on
    /// a payload of the wrong length rather than render a different value;
    /// anything else goes through [BinaryValues].
    private static String formatMaterialisedBytes(byte[] bytes, SchemaNode schema, Style style, int budget) {
        if (!(schema instanceof SchemaNode.PrimitiveNode pn)) {
            // No annotation to decode with: the bytes are the only evidence, the
            // same as for an unannotated column.
            return BinaryValues.render(bytes, budget);
        }
        LogicalType lt = pn.logicalType();
        if (isAnnotatedString(lt)) {
            return text(LogicalTypeConverter.bytesToString(bytes), style);
        }
        if (lt instanceof LogicalType.UuidType) {
            requireLength(pn, "UUID", UUID_LENGTH, bytes);
            return LogicalTypeConverter.bytesToUuid(bytes).toString();
        }
        if (lt instanceof LogicalType.DecimalType dt) {
            return LogicalTypeConverter.bytesToDecimal(bytes, dt.scale()).toPlainString();
        }
        if (lt instanceof LogicalType.IntervalType) {
            requireLength(pn, "INTERVAL", INTERVAL_LENGTH, bytes);
            return formatIntervalBytes(bytes);
        }
        if (pn.type() == PhysicalType.INT96) {
            requireLength(pn, "INT96", INT96_LENGTH, bytes);
            return LogicalTypeConverter.int96ToInstant(bytes).toString();
        }
        return BinaryValues.render(bytes, budget);
    }

    private static void requireLength(SchemaNode.PrimitiveNode field, String type, int expected, byte[] bytes) {
        if (bytes.length != expected) {
            throw new IllegalArgumentException("Field '" + field.name() + "': " + type + " requires exactly "
                    + expected + " bytes, got " + bytes.length);
        }
    }

    private static int shownEntries(int total, Style style) {
        return style == Style.PREVIEW ? Math.min(total, MAX_NESTED_ELEMENTS) : total;
    }

    private static String entry(String key, String value, Style style) {
        return key + (style == Style.EXPANDED || style == Style.EXPORT ? ": " : " : ") + value;
    }

    /// Wraps rendered entries in braces (`object`) or brackets. The one-line
    /// styles separate them with `, `; the display styles pad braces as
    /// `{ a : 1 }`, while JSON braces and all brackets stay tight. A preview
    /// marks the entries it left out as `…+N`. `EXPANDED` puts each entry on its
    /// own line, one level deeper than `depth`.
    private static String enclose(boolean object, List<String> entries, int total, Style style, int depth) {
        String open = object ? "{" : "[";
        String close = object ? "}" : "]";
        if (total == 0) {
            return open + close;
        }
        if (style == Style.EXPANDED) {
            String childPad = pad(depth + 1);
            StringJoiner lines = new StringJoiner(",\n", open + "\n", "\n" + pad(depth) + close);
            for (String entry : entries) {
                lines.add(childPad + entry);
            }
            return lines.toString();
        }
        String inner = object && style != Style.EXPORT ? " " : "";
        StringJoiner line = new StringJoiner(", ", open + inner, inner + close);
        for (String entry : entries) {
            line.add(entry);
        }
        if (entries.size() < total) {
            line.add(Strings.ELLIPSIS + "+" + (total - entries.size()));
        }
        return line.toString();
    }

    private static String pad(int depth) {
        return "  ".repeat(depth);
    }

    private static SchemaNode.GroupNode asGroup(SchemaNode field) {
        return field instanceof SchemaNode.GroupNode group ? group : null;
    }

    private static SchemaNode findChildSchema(SchemaNode.GroupNode groupNode, String name) {
        if (groupNode == null) {
            return null;
        }
        for (SchemaNode child : groupNode.children()) {
            if (child.name().equals(name)) {
                return child;
            }
        }
        return null;
    }

    private static boolean isUnsigned(SchemaNode schema) {
        return schema instanceof SchemaNode.PrimitiveNode pn
                && pn.logicalType() instanceof LogicalType.IntType it
                && !it.isSigned();
    }

    private static boolean isAnnotatedString(LogicalType lt) {
        return lt instanceof LogicalType.StringType
                || lt instanceof LogicalType.EnumType
                || lt instanceof LogicalType.JsonType
                || lt instanceof LogicalType.BsonType;
    }

    /// The text of a string leaf: sanitised for display, verbatim for export.
    private static String text(String s, Style style) {
        return style == Style.EXPORT ? s : Strings.sanitizeControls(s);
    }

    /// A leaf inside an exported value: a JSON number or boolean where
    /// `isJsonScalar` holds for its schema node, or where a schema-less value is
    /// a Java number or boolean; a JSON string otherwise. A non-finite float is a
    /// string, since JSON numbers cannot express it.
    private static String jsonLeaf(Object value, SchemaNode schema, boolean useLogicalType, int budget) {
        String text = renderLeaf(value, schema, useLogicalType, Style.EXPORT, budget);
        boolean scalar = schema instanceof SchemaNode.PrimitiveNode pn
                ? isJsonScalar(pn)
                : value instanceof Boolean || (value instanceof Number && !(value instanceof BigDecimal));
        boolean nonFinite = (value instanceof Float f && !Float.isFinite(f))
                || (value instanceof Double d && !Double.isFinite(d));
        return scalar && !nonFinite ? text : jsonString(text);
    }

    /// Whether a primitive field's values are JSON numbers or booleans in
    /// `convert --format json`: `BOOLEAN`, `INT32`, `INT64`, `FLOAT` or `DOUBLE`
    /// with no logical annotation or an `INT` one. Every other value is a JSON
    /// string.
    public static boolean isJsonScalar(SchemaNode.PrimitiveNode field) {
        LogicalType lt = field.logicalType();
        return (lt == null || lt instanceof LogicalType.IntType) && switch (field.type()) {
            case BOOLEAN, INT32, INT64, FLOAT, DOUBLE -> true;
            case INT96, BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> false;
        };
    }

    /// Whether a field renders as a nested value: a group, or a repeated
    /// primitive, the legacy list whose elements are the primitive itself.
    public static boolean isNested(SchemaNode field) {
        return field instanceof SchemaNode.GroupNode || field.repetitionType() == RepetitionType.REPEATED;
    }

    private static SchemaNode listElementSchema(SchemaNode schema) {
        if (schema instanceof SchemaNode.GroupNode group) {
            return group.getListElement();
        }
        // A repeated primitive is its own element.
        return schema instanceof SchemaNode.PrimitiveNode pn && pn.repetitionType() == RepetitionType.REPEATED
                ? pn
                : null;
    }

    private static String jsonString(String s) {
        return '"' + JsonStrings.escape(s) + '"';
    }

    // ==================== Variant JSON ====================

    /// A Variant as JSON — quoted keys and strings, native numbers, booleans,
    /// objects and arrays — the `EXPORT` form of a Variant. Strings are written
    /// verbatim through JSON escaping; `NaN` and `±Infinity`, which JSON numbers
    /// cannot express, are strings.
    private static String variantJson(PqVariant variant) {
        if (variant == null) {
            return "null";
        }
        StringBuilder sb = new StringBuilder();
        appendVariantJson(sb, variant);
        return sb.toString();
    }

    private static void appendVariantJson(StringBuilder sb, PqVariant variant) {
        switch (variant.type()) {
            case NULL -> sb.append("null");
            case BOOLEAN_TRUE -> sb.append("true");
            case BOOLEAN_FALSE -> sb.append("false");
            case INT8, INT16, INT32 -> sb.append(variant.asInt());
            case INT64 -> sb.append(variant.asLong());
            case FLOAT -> appendJsonNumber(sb, variant.asFloat());
            case DOUBLE -> appendJsonNumber(sb, variant.asDouble());
            case DECIMAL4, DECIMAL8, DECIMAL16 -> sb.append(variant.asDecimal().toPlainString());
            case STRING -> appendJsonString(sb, variant.asString());
            case BINARY -> appendJsonString(sb, BinaryValues.render(variant.asBinary()));
            case DATE -> appendJsonString(sb, variant.asDate().toString());
            case TIME_NTZ -> appendJsonString(sb, variant.asTime().toString());
            case TIMESTAMP, TIMESTAMP_NTZ, TIMESTAMP_NANOS, TIMESTAMP_NTZ_NANOS ->
                    appendJsonString(sb, variantTimestampText(variant));
            case UUID -> appendJsonString(sb, variant.asUuid().toString());
            case OBJECT -> appendVariantJsonObject(sb, variant.asObject());
            case ARRAY -> appendVariantJsonArray(sb, variant.asArray());
        }
    }

    private static void appendVariantJsonObject(StringBuilder sb, PqVariantObject object) {
        sb.append('{');
        int fieldCount = object.getFieldCount();
        for (int i = 0; i < fieldCount; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            String name = object.getFieldName(i);
            appendJsonString(sb, name);
            sb.append(": ");
            PqVariant fieldValue = object.getVariant(name);
            if (fieldValue == null) {
                sb.append("null");
            }
            else {
                appendVariantJson(sb, fieldValue);
            }
        }
        sb.append('}');
    }

    private static void appendVariantJsonArray(StringBuilder sb, PqVariantArray array) {
        sb.append('[');
        int size = array.size();
        for (int i = 0; i < size; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            PqVariant element = array.get(i);
            if (element == null) {
                sb.append("null");
            }
            else {
                appendVariantJson(sb, element);
            }
        }
        sb.append(']');
    }

    private static void appendJsonNumber(StringBuilder sb, float f) {
        if (Float.isFinite(f)) {
            sb.append(f);
        }
        else {
            appendJsonString(sb, Float.toString(f));
        }
    }

    private static void appendJsonNumber(StringBuilder sb, double d) {
        if (Double.isFinite(d)) {
            sb.append(d);
        }
        else {
            appendJsonString(sb, Double.toString(d));
        }
    }

    private static void appendJsonString(StringBuilder sb, String s) {
        sb.append(jsonString(s));
    }

    // ==================== dictionary source ====================

    /// Renders a raw primitive drawn from a parsed `Dictionary` — an `Integer`,
    /// `Long`, `Float`, `Double` or `byte[]`, matching the five `Dictionary`
    /// subtypes — in the column's canonical display form. `useLogicalType=false`
    /// bypasses the logical type: timestamps render as their stored long,
    /// decimals as their unscaled bytes. A fixed-width payload of the wrong
    /// length renders as `0x` hex. `budget` bounds hex building in display cells;
    /// text renders whole.
    public static String formatDictionary(Object rawValue, ColumnSchema col,
                                          boolean useLogicalType, int budget) {
        Objects.requireNonNull(col, "col");
        requireBudget(budget);
        if (col.type() == PhysicalType.INT96 && rawValue instanceof byte[] bytes) {
            return formatInt96(bytes, useLogicalType, budget);
        }
        LogicalType lt = useLogicalType ? col.logicalType() : null;
        return switch (rawValue) {
            case Integer i -> formatInt(i, lt, col);
            case Long l -> formatLong(l, lt, col);
            case Float f -> Float.toString(f);
            case Double d -> Double.toString(d);
            case byte[] bytes -> formatDictionaryBytes(bytes, lt, col, budget);
            case null -> "null";
            default -> throw unknownDictionaryPrimitive(rawValue);
        };
    }

    private static IllegalStateException unknownDictionaryPrimitive(Object rawValue) {
        return new IllegalStateException(
                "Dictionary records carry Integer, Long, Float, Double or byte[] values, got "
                        + rawValue.getClass().getName());
    }

    /// INT32 entries can only carry logical types backed by `INT32`: `DATE`,
    /// `TIME(MILLIS)`, `DECIMAL(precision ≤ 9)` and the `INT(8|16|32)` family.
    /// The `default` arm rejects any other logical type as one the physical type
    /// cannot carry.
    private static String formatInt(int raw, LogicalType lt, ColumnSchema col) {
        return switch (lt) {
            case null -> Integer.toString(raw);
            case LogicalType.DateType d -> LogicalTypeConverter.intToDate(raw).toString();
            case LogicalType.TimeType t -> timeText(raw, t.unit());
            case LogicalType.IntType it when !it.isSigned() -> Long.toString(Integer.toUnsignedLong(raw));
            case LogicalType.IntType it -> Integer.toString(raw);
            case LogicalType.DecimalType d -> LogicalTypeConverter.longToDecimal(raw, d.scale()).toPlainString();
            default -> throw notBackedBy(col, lt, "INT32");
        };
    }

    /// INT64 entries can only carry logical types backed by `INT64`: `TIMESTAMP`,
    /// `TIME(MICROS|NANOS)`, `DECIMAL(10 ≤ precision ≤ 18)` and `INT(64)`. See
    /// [#formatInt] for the `default` arm.
    private static String formatLong(long raw, LogicalType lt, ColumnSchema col) {
        return switch (lt) {
            case null -> Long.toString(raw);
            case LogicalType.TimestampType ts -> LogicalTypeConverter.longToTemporal(raw, ts).toString();
            case LogicalType.TimeType t -> timeText(raw, t.unit());
            case LogicalType.IntType it when !it.isSigned() -> Long.toUnsignedString(raw);
            case LogicalType.IntType it -> Long.toString(raw);
            case LogicalType.DecimalType d -> LogicalTypeConverter.longToDecimal(raw, d.scale()).toPlainString();
            default -> throw notBackedBy(col, lt, "INT64");
        };
    }

    /// `BYTE_ARRAY` / `FIXED_LEN_BYTE_ARRAY` entries can carry the byte-backed
    /// logical types: strings, BSON, UUID, INTERVAL, FLOAT16, DECIMAL, plus
    /// geometry and geography WKB. A UUID, INTERVAL or FLOAT16 payload of the
    /// wrong length is not the value its type claims and renders as `0x` hex. See
    /// [#formatInt] for the `default` arm.
    private static String formatDictionaryBytes(byte[] raw, LogicalType lt, ColumnSchema col, int budget) {
        return switch (lt) {
            case null -> BinaryValues.render(raw, budget);
            case LogicalType.StringType s -> Strings.sanitizeControls(LogicalTypeConverter.bytesToString(raw));
            case LogicalType.EnumType e -> Strings.sanitizeControls(LogicalTypeConverter.bytesToString(raw));
            case LogicalType.JsonType j -> Strings.sanitizeControls(LogicalTypeConverter.bytesToString(raw));
            case LogicalType.BsonType b -> Strings.sanitizeControls(LogicalTypeConverter.bytesToString(raw));
            case LogicalType.DecimalType d -> LogicalTypeConverter.bytesToDecimal(raw, d.scale()).toPlainString();
            case LogicalType.UuidType u when raw.length == UUID_LENGTH ->
                    LogicalTypeConverter.bytesToUuid(raw).toString();
            case LogicalType.UuidType u -> BinaryValues.toHex(raw, budget);
            case LogicalType.IntervalType i when raw.length == INTERVAL_LENGTH -> formatIntervalBytes(raw);
            case LogicalType.IntervalType i -> BinaryValues.toHex(raw, budget);
            case LogicalType.Float16Type f when raw.length == FLOAT16_LENGTH ->
                    Float.toString(LogicalTypeConverter.bytesToFloat16(raw));
            case LogicalType.Float16Type f -> BinaryValues.toHex(raw, budget);
            case LogicalType.GeometryType g -> BinaryValues.render(raw, budget);
            case LogicalType.GeographyType g -> BinaryValues.render(raw, budget);
            default -> throw notBackedBy(col, lt, "BYTE_ARRAY");
        };
    }

    /// INT96 carries no logical annotation: logical mode renders the instant;
    /// physical mode, and a payload that is not the 12 bytes an INT96 is, render
    /// the raw `0x` hex.
    private static String formatInt96(byte[] bytes, boolean useLogicalType, int budget) {
        return useLogicalType && bytes.length == INT96_LENGTH
                ? LogicalTypeConverter.int96ToInstant(bytes).toString()
                : BinaryValues.toHex(bytes, budget);
    }

    private static IllegalStateException notBackedBy(ColumnSchema col, LogicalType lt, String physical) {
        return new IllegalStateException(
                "Column '" + col.fieldPath() + "' has logical type " + lt + ", which " + physical
                        + " values cannot carry");
    }

    private static String formatIntervalBytes(byte[] bytes) {
        return formatInterval(LogicalTypeConverter.bytesToInterval(bytes));
    }

    // ==================== raw statistics bytes ====================

    /// Renders statistics bytes whole: the four-argument `formatBytes` with
    /// [BinaryValues#NO_LIMIT].
    public static String formatBytes(byte[] bytes, ColumnSchema col, boolean useLogicalType) {
        return formatBytes(bytes, col, useLogicalType, BinaryValues.NO_LIMIT);
    }

    /// Renders raw statistics bytes — a min/max bound from a column chunk or the
    /// page index — for a caller that fills a cell. The hex of a large payload is
    /// built only as far as `budget`, running just past it when the payload is
    /// longer so the caller sees there is more and marks what it cut. Absent
    /// statistics (`null` bytes) render as `-`; empty ones render as `""` on a
    /// byte-array column. `useLogicalType=false` dispatches on the physical type
    /// only, so TIMESTAMP / DATE / TIME / DECIMAL / UUID columns render in their
    /// stored int, long or hex form. A fixed-width payload of the wrong length
    /// renders as `0x` hex.
    public static String formatBytes(byte[] bytes, ColumnSchema col,
                                     boolean useLogicalType, int budget) {
        requireBudget(budget);
        Objects.requireNonNull(col, "col");
        if (bytes == null) {
            return "-";
        }
        if (bytes.length == 0) {
            return isByteBacked(col.type()) ? "\"\"" : "";
        }
        if (!hasPhysicalWidth(bytes, col.type())) {
            // Bytes of the wrong width are not a value of the physical type.
            return BinaryValues.toHex(bytes, budget);
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
            return timeText(decodeIntegral(bytes, col), t.unit());
        }

        return switch (col.type()) {
            case BOOLEAN -> Boolean.toString(StatisticsDecoder.decodeBoolean(bytes));
            case INT32 -> formatInt32Value(StatisticsDecoder.decodeInt(bytes), lt);
            case INT64 -> formatInt64Value(StatisticsDecoder.decodeLong(bytes), lt);
            case FLOAT -> Float.toString(StatisticsDecoder.decodeFloat(bytes));
            case DOUBLE -> Double.toString(StatisticsDecoder.decodeDouble(bytes));
            case INT96 -> formatInt96(bytes, useLogicalType, budget);
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> formatDictionaryBytes(bytes, lt, col, budget);
        };
    }

    /// A statistic on an `INT32` or `INT64` column, widened to the `long` the
    /// converters take. The date, time and timestamp annotations all read one.
    private static long decodeIntegral(byte[] bytes, ColumnSchema col) {
        return col.type() == PhysicalType.INT32
                ? StatisticsDecoder.decodeInt(bytes)
                : StatisticsDecoder.decodeLong(bytes);
    }

    /// A zero-length value is only meaningful for the variable-length physical
    /// types; rendering it as `""` distinguishes "present but empty" from the
    /// blank cell an absent statistic leaves behind.
    private static boolean isByteBacked(PhysicalType pt) {
        return pt == PhysicalType.BYTE_ARRAY || pt == PhysicalType.FIXED_LEN_BYTE_ARRAY;
    }

    /// Whether `bytes` have the width a value of the physical type occupies.
    /// Byte arrays have any width; their logical decoders check their own.
    private static boolean hasPhysicalWidth(byte[] bytes, PhysicalType type) {
        return switch (type) {
            case BOOLEAN -> bytes.length == 1;
            case INT32, FLOAT -> bytes.length == Integer.BYTES;
            case INT64, DOUBLE -> bytes.length == Long.BYTES;
            case INT96 -> bytes.length == INT96_LENGTH;
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> true;
        };
    }

    // ==================== decoded dictionary entries ====================

    /// Formats an already-decoded `INT32` dictionary entry. Logical types
    /// `DECIMAL` / `DATE` / `TIME` go through [LogicalTypeConverter]; otherwise
    /// the raw int is rendered honouring the unsigned [LogicalType.IntType].
    public static String formatDecoded(int value, ColumnSchema col) {
        Objects.requireNonNull(col, "col");
        LogicalType lt = col.logicalType();
        if (lt instanceof LogicalType.DecimalType) {
            return decimalText(LogicalTypeConverter.convert(value, col.type(), lt));
        }
        if (lt instanceof LogicalType.DateType) {
            return LogicalTypeConverter.intToDate(value).toString();
        }
        if (lt instanceof LogicalType.TimeType t) {
            return timeText(value, t.unit());
        }
        return formatInt32Value(value, lt);
    }

    /// Formats an already-decoded `INT64` dictionary entry. Logical types
    /// `DECIMAL` / `TIME` / `TIMESTAMP` go through [LogicalTypeConverter];
    /// otherwise the raw long is rendered honouring the unsigned
    /// [LogicalType.IntType].
    public static String formatDecoded(long value, ColumnSchema col) {
        Objects.requireNonNull(col, "col");
        LogicalType lt = col.logicalType();
        if (lt instanceof LogicalType.DecimalType) {
            return decimalText(LogicalTypeConverter.convert(value, col.type(), lt));
        }
        if (lt instanceof LogicalType.TimeType t) {
            return timeText(value, t.unit());
        }
        if (lt instanceof LogicalType.TimestampType ts) {
            return LogicalTypeConverter.longToTemporal(value, ts).toString();
        }
        return formatInt64Value(value, lt);
    }

    public static String formatDecoded(float value) {
        return Float.toString(value);
    }

    public static String formatDecoded(double value) {
        return Double.toString(value);
    }

    /// `LogicalTypeConverter.convert` yields a `BigDecimal` for DECIMAL
    /// columns; the canonical text is its plain string, never scientific
    /// notation.
    private static String decimalText(Object converted) {
        return ((BigDecimal) converted).toPlainString();
    }

    private static String formatInt32Value(int v, LogicalType lt) {
        if (lt instanceof LogicalType.IntType it && !it.isSigned()) {
            return Long.toString(Integer.toUnsignedLong(v));
        }
        return Integer.toString(v);
    }

    private static String formatInt64Value(long v, LogicalType lt) {
        if (lt instanceof LogicalType.IntType it && !it.isSigned()) {
            return Long.toUnsignedString(v);
        }
        return Long.toString(v);
    }

    // ==================== shared leaf helpers ====================

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

    /// A TIME value as its canonical text, or as its stored integer when it lies
    /// outside a day: statistics and dictionary entries come from the file
    /// unchecked, and no time of day is that value.
    private static String timeText(long raw, LogicalType.TimeUnit unit) {
        long perDay = switch (unit) {
            case MILLIS -> 86_400_000L;
            case MICROS -> 86_400_000_000L;
            case NANOS -> 86_400_000_000_000L;
        };
        return raw >= 0 && raw < perDay ? LogicalTypeConverter.longToTime(raw, unit).toString() : Long.toString(raw);
    }

    /// The budget every entry point accepts: [BinaryValues#NO_LIMIT] for the
    /// whole value, or a positive number of terminal display cells. `0` and
    /// negative values below the sentinel have no faithful rendering.
    private static void requireBudget(int budget) {
        if (budget == BinaryValues.NO_LIMIT) {
            return;
        }
        if (budget < 1) {
            throw new IllegalArgumentException(
                    "budget must be BinaryValues.NO_LIMIT (-1, unlimited) or a positive number of"
                            + " terminal cells, got " + budget);
        }
    }
}
