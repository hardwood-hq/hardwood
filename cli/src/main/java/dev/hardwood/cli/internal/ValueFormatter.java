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
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Objects;
import java.util.UUID;

import dev.hardwood.internal.conversion.LogicalTypeConverter;
import dev.hardwood.internal.predicate.StatisticsDecoder;
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

/// Canonical rendering of Parquet values for every CLI surface — `print`,
/// `convert`, the `inspect` commands and the `dive` TUI. One dispatch per
/// source keeps the same logical type spelling the same text everywhere:
/// ISO-8601 timestamps, `LocalDate.toString` for dates, `BigDecimal.toPlainString`
/// for decimals, and control characters sanitised through
/// [Strings#sanitizeControls] so no cell ever emits one raw.
///
/// The static entry points differ only in where the value comes from:
///
/// - [#formatReader(RowReader, int, SchemaNode, boolean, NestedStyle, int)] — typed
///   accessors on a [RowReader]: dive preview cells (COMPACT) and the record
///   modal (EXPANDED).
/// - [#formatValue(Object, SchemaNode, int)] — a materialised value: print
///   cells and `convert` CSV cells.
/// - [#formatDictionary(Object, ColumnSchema, boolean, int)] — a raw primitive
///   out of parsed `Dictionary` records, which surface as primitive arrays with
///   no `RowReader` available.
///
/// Budgets are measured in terminal display cells; [BinaryValues#NO_LIMIT]
/// renders the whole value. Text budgets never cut — a caller that truncates
/// applies [Strings#truncateRight] and marks the cut — but they do bound hex
/// building for binary payloads. Every switch over [LogicalType] is exhaustive
/// over the sealed hierarchy: a new subtype fails to compile until an explicit
/// case is added, preventing silent fall-through to the raw-bytes path.
public final class ValueFormatter {

    /// The widest a dive preview cell can be. The rendered rows are cached ahead of
    /// layout, so the terminal's actual width is not available here and cannot
    /// become part of the cache key; this is simply wider than any terminal, so
    /// it never clips a cell that would have been shown and still holds a
    /// multi-megabyte payload to a few kilobytes of hex.
    public static final int PREVIEW_CELL_BUDGET = 4096;

    private ValueFormatter() {
    }

    /// How multi-line-capable nested values render: `COMPACT` is the single-line
    /// form used by table cells, `EXPANDED` the indented multi-line form used by
    /// the dive record modal.
    public enum NestedStyle {
        COMPACT,
        EXPANDED
    }

    /// Renders the field at `fieldIndex` through the reader's typed accessors.
    /// `useLogicalType=true` renders timestamps, decimals, UUIDs, etc. as their
    /// canonical logical form; `useLogicalType=false` renders the underlying
    /// physical value (e.g. `1735689600000000` instead of `2025-01-01T00:00:00Z`).
    /// Nested groups always render structurally — the toggle only affects
    /// primitive leaves.
    ///
    /// `COMPACT` is the single-line table-cell form; with a finite budget the
    /// nested walk is capped at a few elements and depth levels (a preview
    /// cell shows a capped view), while `NO_LIMIT` renders every element —
    /// the export leaf path. `EXPANDED` renders every element on its own
    /// indented line.
    public static String formatReader(RowReader reader, int fieldIndex, SchemaNode field,
                                      boolean useLogicalType, NestedStyle style, int budget) {
        Objects.requireNonNull(reader, "reader");
        Objects.requireNonNull(field, "field");
        Objects.requireNonNull(style, "style");
        requireBudget(budget);
        if (style == NestedStyle.EXPANDED) {
            return formatExpanded(reader, fieldIndex, field, useLogicalType, budget);
        }
        return format(reader, fieldIndex, field, useLogicalType, budget);
    }

    private static String format(RowReader reader, int fieldIndex, SchemaNode field,
                                 boolean useLogicalType, int maxChars) {
        if (reader.isNull(fieldIndex)) {
            return "null";
        }
        if (field instanceof SchemaNode.GroupNode) {
            // Nested group — render structurally rather than letting the JVM's
            // default `Object.toString()` print "dev.hardwood.internal...".
            // `reader.getValue` and `getRawValue` return the same flyweight for
            // groups; the toggle only changes how primitive leaves inside the
            // group are rendered, which `formatNested` re-dispatches on.
            return formatNested(reader.getValue(fieldIndex), 0, useLogicalType, maxChars);
        }
        SchemaNode.PrimitiveNode prim = (SchemaNode.PrimitiveNode) field;
        if (!useLogicalType) {
            return formatPhysical(reader, fieldIndex, maxChars);
        }
        LogicalType lt = prim.logicalType();
        return switch (lt) {
            case null -> formatPhysical(reader, fieldIndex, maxChars);
            case LogicalType.TimestampType ts -> ts.isAdjustedToUTC()
                    ? reader.getTimestamp(fieldIndex).toString()
                    : reader.getLocalTimestamp(fieldIndex).toString();
            case LogicalType.DateType d -> reader.getDate(fieldIndex).toString();
            case LogicalType.TimeType t -> reader.getTime(fieldIndex).toString();
            case LogicalType.DecimalType dec -> reader.getDecimal(fieldIndex).toPlainString();
            case LogicalType.UuidType u -> reader.getUuid(fieldIndex).toString();
            case LogicalType.StringType s -> Strings.sanitizeControls(reader.getString(fieldIndex));
            case LogicalType.EnumType e -> Strings.sanitizeControls(reader.getString(fieldIndex));
            case LogicalType.JsonType j -> Strings.sanitizeControls(reader.getString(fieldIndex));
            case LogicalType.BsonType b -> Strings.sanitizeControls(reader.getString(fieldIndex));
            case LogicalType.IntType it when !it.isSigned() -> formatUnsignedInt(reader, fieldIndex, prim);
            // Signed IntType still goes through getRawValue / String.valueOf —
            // matches the pre-refactor behavior.
            case LogicalType.IntType it -> formatPhysical(reader, fieldIndex, maxChars);
            case LogicalType.IntervalType i -> formatInterval(reader.getInterval(fieldIndex));
            // FLOAT16 was previously hex-rendered because nothing matched the
            // logical type and getRawValue returns the FLBA(2) bytes; getFloat
            // handles the half→single widening transparently so 1.5 prints as
            // "1.5" instead of "0x003c".
            case LogicalType.Float16Type f16 -> Float.toString(reader.getFloat(fieldIndex));
            // Geometry / Geography carry opaque WKB / WKT binary payloads with
            // no decoder yet — hex-render explicitly rather than relying on a
            // raw-bytes fall-through.
            case LogicalType.GeometryType g -> formatPhysical(reader, fieldIndex, maxChars);
            case LogicalType.GeographyType g -> formatPhysical(reader, fieldIndex, maxChars);
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
    private static String formatExpanded(RowReader reader, int fieldIndex, SchemaNode field,
                                         boolean useLogicalType, int budget) {
        if (reader.isNull(fieldIndex)) {
            return "null";
        }
        if (field instanceof SchemaNode.GroupNode) {
            return formatNestedPretty(reader.getValue(fieldIndex), 0, useLogicalType, budget);
        }
        // For primitive leaves the expanded form is identical to the
        // single-line logical / physical rendering, except that the modal shows
        // the value whole however long it is.
        return format(reader, fieldIndex, field, useLogicalType, budget);
    }

    private static String formatNestedPretty(Object value, int indent, boolean useLogicalType, int maxChars) {
        if (value == null) {
            return "null";
        }
        if (value instanceof PqList list) {
            return prettyList(list, indent, useLogicalType, maxChars);
        }
        if (value instanceof PqStruct struct) {
            return prettyStruct(struct, indent, useLogicalType, maxChars);
        }
        if (value instanceof PqMap map) {
            return prettyMap(map, indent, useLogicalType, maxChars);
        }
        if (value instanceof PqVariant variant) {
            return prettyVariant(variant, indent, useLogicalType, maxChars);
        }
        if (value instanceof byte[] bytes) {
            return formatRawBytes(bytes, maxChars);
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
        if (value instanceof LocalDateTime ldt) {
            return ldt.toString();
        }
        if (value instanceof String s) {
            return Strings.sanitizeControls(s);
        }
        return String.valueOf(value);
    }

    private static String prettyList(PqList list, int indent, boolean useLogicalType, int maxChars) {
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
            sb.append(childPad).append(formatNestedPretty(element, indent + 1, useLogicalType, maxChars));
        }
        sb.append("\n").append(pad(indent)).append("]");
        return sb.toString();
    }

    private static String prettyStruct(PqStruct struct, int indent, boolean useLogicalType, int maxChars) {
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
                    .append(formatNestedPretty(fieldValue, indent + 1, useLogicalType, maxChars));
            if (i < count - 1) {
                sb.append(",");
            }
            sb.append("\n");
        }
        sb.append(pad(indent)).append("}");
        return sb.toString();
    }

    private static String prettyMap(PqMap map, int indent, boolean useLogicalType, int maxChars) {
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
                    .append(formatNestedPretty(key, indent + 1, useLogicalType, maxChars))
                    .append(": ")
                    .append(formatNestedPretty(value, indent + 1, useLogicalType, maxChars));
            if (i < entries.size() - 1) {
                sb.append(",");
            }
            sb.append("\n");
        }
        sb.append(pad(indent)).append("}");
        return sb.toString();
    }

    private static String prettyVariant(PqVariant variant, int indent, boolean useLogicalType, int maxChars) {
        VariantType type = variant.type();
        return switch (type) {
            case OBJECT -> prettyVariantObject(variant.asObject(), indent, useLogicalType, maxChars);
            case ARRAY -> prettyVariantArray(variant.asArray(), indent, useLogicalType, maxChars);
            // Primitives use the single-line form.
            default -> formatVariant(variant, indent, maxChars);
        };
    }

    private static String prettyVariantObject(PqVariantObject obj, int indent, boolean useLogicalType, int maxChars) {
        int count = obj.getFieldCount();
        if (count == 0) {
            return "{}";
        }
        StringBuilder sb = new StringBuilder("{\n");
        String childPad = pad(indent + 1);
        for (int i = 0; i < count; i++) {
            String name = obj.getFieldName(i);
            sb.append(childPad).append(name).append(": ")
                    .append(formatNestedPretty(obj.getVariant(name), indent + 1, useLogicalType, maxChars));
            if (i < count - 1) {
                sb.append(",");
            }
            sb.append("\n");
        }
        sb.append(pad(indent)).append("}");
        return sb.toString();
    }

    private static String prettyVariantArray(PqVariantArray array, int indent, boolean useLogicalType, int maxChars) {
        int size = array.size();
        if (size == 0) {
            return "[]";
        }
        StringBuilder sb = new StringBuilder("[\n");
        String childPad = pad(indent + 1);
        for (int i = 0; i < size; i++) {
            sb.append(childPad).append(formatNestedPretty(array.get(i), indent + 1, useLogicalType, maxChars));
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

    /// Renders a materialised value — whatever the reader's `getValue` hands
    /// back for one field — as its canonical display text. Nested values
    /// (structs, lists, maps, variants) render whole in the display grammar:
    /// `{ a : 1 }`, `[1, 2]`, unquoted Variant keys. `field` is the value's own
    /// schema node; it decides how byte-backed leaves decode (annotated
    /// strings, UUID, decimal, INTERVAL, INT96) and whether integers render
    /// unsigned. A null or non-group schema walks the value schema-less, the
    /// way legacy list and map layouts without resolvable child schemas always
    /// have; a group schema with a scalar value is a mismatch and throws
    /// rather than rendering a misleading string.
    public static String formatValue(Object value, SchemaNode field, int budget) {
        requireBudget(budget);
        Objects.requireNonNull(field, "field");
        if (value == null) {
            return "null";
        }
        if (value instanceof PqVariant variant) {
            return formatVariantDisplay(variant, budget);
        }
        if (value instanceof PqStruct struct) {
            return formatStructDisplay(struct, asGroup(field), budget);
        }
        if (value instanceof PqList list) {
            return formatListDisplay(list, asGroup(field), budget);
        }
        if (value instanceof PqMap map) {
            return formatMapDisplay(map, asGroup(field), budget);
        }
        if (field instanceof SchemaNode.GroupNode) {
            throw new IllegalStateException("Field '" + field.name() + "' is a group in the schema, but the"
                    + " value is a " + value.getClass().getName());
        }
        if (value instanceof byte[] bytes) {
            return formatMaterialisedBytes(bytes, field, budget);
        }
        if (value instanceof String s) {
            return Strings.sanitizeControls(s);
        }
        if (value instanceof BigDecimal decimal) {
            return decimal.toPlainString();
        }
        if (value instanceof Instant instant) {
            return instant.toString();
        }
        if (value instanceof LocalDateTime ldt) {
            return ldt.toString();
        }
        if (value instanceof PqInterval interval) {
            return formatInterval(interval);
        }
        if (field instanceof SchemaNode.PrimitiveNode pn && pn.logicalType() instanceof LogicalType.IntType it
                && !it.isSigned()) {
            if (value instanceof Integer i) {
                return Long.toString(Integer.toUnsignedLong(i));
            }
            if (value instanceof Long l) {
                return Long.toUnsignedString(l);
            }
        }
        return String.valueOf(value);
    }

    private static SchemaNode.GroupNode asGroup(SchemaNode field) {
        return field instanceof SchemaNode.GroupNode group ? group : null;
    }

    // ==================== display-grammar nested walkers (whole values) ====================

    /// Walks a materialised nested value whole: no element or depth caps — the
    /// caps are a dive-preview-cell device, and a `print` or `convert` cell
    /// renders the value in full. The budget only bounds binary hex at leaves.
    private static String formatStructDisplay(PqStruct struct, SchemaNode.GroupNode schemaNode, int maxChars) {
        StringBuilder sb = new StringBuilder("{ ");
        int fieldCount = struct.getFieldCount();
        for (int i = 0; i < fieldCount; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            String name = struct.getFieldName(i);
            sb.append(name).append(" : ")
                    .append(formatValue(struct.getValue(name), findChildSchema(schemaNode, name), maxChars));
        }
        return sb.append(" }").toString();
    }

    private static String formatListDisplay(PqList list, SchemaNode.GroupNode schemaNode, int maxChars) {
        SchemaNode elementSchema = schemaNode != null ? schemaNode.getListElement() : null;
        StringBuilder sb = new StringBuilder("[");
        int size = list.size();
        for (int i = 0; i < size; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(formatValue(list.get(i), elementSchema, maxChars));
        }
        return sb.append("]").toString();
    }

    private static String formatMapDisplay(PqMap map, SchemaNode.GroupNode schemaNode, int maxChars) {
        SchemaNode keySchema = null;
        SchemaNode valueSchema = null;
        if (schemaNode != null && !schemaNode.children().isEmpty()) {
            SchemaNode.GroupNode keyValueGroup = (SchemaNode.GroupNode) schemaNode.children().get(0);
            if (keyValueGroup.children().size() >= 2) {
                keySchema = keyValueGroup.children().get(0);
                valueSchema = keyValueGroup.children().get(1);
            }
        }
        StringBuilder sb = new StringBuilder("{ ");
        boolean first = true;
        for (PqMap.Entry entry : map.getEntries()) {
            if (!first) {
                sb.append(", ");
            }
            first = false;
            sb.append(formatValue(entry.getKey(), keySchema, maxChars))
                    .append(" : ")
                    .append(formatValue(entry.getValue(), valueSchema, maxChars));
        }
        return sb.append(" }").toString();
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

    /// Whole-value display grammar for a Variant: objects `{ k : v }`, arrays
    /// `[v, ...]`, unquoted scalars. The export grammar is [#variantJson].
    private static String formatVariantDisplay(PqVariant variant, int maxChars) {
        return switch (variant.type()) {
            case OBJECT -> variantDisplayObject(variant.asObject(), maxChars);
            case ARRAY -> variantDisplayArray(variant.asArray(), maxChars);
            default -> variantScalarText(variant, maxChars);
        };
    }

    private static String variantDisplayObject(PqVariantObject object, int maxChars) {
        int fieldCount = object.getFieldCount();
        if (fieldCount == 0) {
            return "{}";
        }
        StringBuilder sb = new StringBuilder("{ ");
        for (int i = 0; i < fieldCount; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            String name = object.getFieldName(i);
            sb.append(name).append(" : ").append(formatVariantDisplay(object.getVariant(name), maxChars));
        }
        return sb.append(" }").toString();
    }

    private static String variantDisplayArray(PqVariantArray array, int maxChars) {
        int size = array.size();
        if (size == 0) {
            return "[]";
        }
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < size; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(formatVariantDisplay(array.get(i), maxChars));
        }
        return sb.append("]").toString();
    }

    // ==================== export-only JSON grammar ====================

    /// Export-only JSON grammar for Variant values — quoted keys and strings
    /// with JSON escapes — used by `convert --format json`, where the output
    /// must parse as JSON. STRING leaves are sanitised before escaping, so
    /// parsed JSON contains `·`, never the original control character.
    public static String variantJson(PqVariant variant) {
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
            case FLOAT -> sb.append(variant.asFloat());
            case DOUBLE -> sb.append(variant.asDouble());
            case DECIMAL4, DECIMAL8, DECIMAL16 -> sb.append(variant.asDecimal().toPlainString());
            case STRING -> appendJsonString(sb, Strings.sanitizeControls(variant.asString()));
            case BINARY -> appendJsonString(sb, BinaryValues.render(variant.asBinary()));
            case DATE -> appendJsonString(sb, variant.asDate().toString());
            case TIME_NTZ -> appendJsonString(sb, variant.asTime().toString());
            case TIMESTAMP, TIMESTAMP_NTZ, TIMESTAMP_NANOS, TIMESTAMP_NTZ_NANOS ->
                appendJsonString(sb, variant.asTimestamp().toString());
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

    private static void appendJsonString(StringBuilder sb, String s) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                default -> {
                    if (c < 0x20) {
                        sb.append(Fmt.fmt("\\u%04x", (int) c));
                    }
                    else {
                        sb.append(c);
                    }
                }
            }
        }
        sb.append('"');
    }

    /// Renders a byte-backed materialised leaf. Annotated strings decode as
    /// UTF-8 (sanitised); UUID, decimal, INTERVAL and INT96 decode through their
    /// converters and fail on a malformed payload length rather than render a
    /// misleading value; anything else goes through [BinaryValues].
    private static String formatMaterialisedBytes(byte[] bytes, SchemaNode schema, int budget) {
        if (isAnnotatedStringField(schema)) {
            return Strings.sanitizeControls(new String(bytes, StandardCharsets.UTF_8));
        }
        SchemaNode.PrimitiveNode pn = (SchemaNode.PrimitiveNode) schema;
        LogicalType lt = pn.logicalType();
        if (lt instanceof LogicalType.UuidType) {
            if (bytes.length != 16) {
                throw new IllegalArgumentException("UUID requires exactly 16 bytes, got " + bytes.length);
            }
            ByteBuffer bb = ByteBuffer.wrap(bytes);
            return new UUID(bb.getLong(), bb.getLong()).toString();
        }
        if (lt instanceof LogicalType.DecimalType dt) {
            return new BigDecimal(new BigInteger(bytes), dt.scale()).toPlainString();
        }
        if (lt instanceof LogicalType.IntervalType) {
            if (bytes.length != 12) {
                throw new IllegalArgumentException("INTERVAL requires exactly 12 bytes, got " + bytes.length);
            }
            return formatIntervalBytes(bytes);
        }
        if (pn.type() == PhysicalType.INT96) {
            return LogicalTypeConverter.int96ToInstant(bytes).toString();
        }
        return BinaryValues.render(bytes, budget);
    }

    private static boolean isAnnotatedStringField(SchemaNode node) {
        if (!(node instanceof SchemaNode.PrimitiveNode pn)) {
            return false;
        }
        LogicalType lt = pn.logicalType();
        return lt instanceof LogicalType.BsonType
                || lt instanceof LogicalType.StringType
                || lt instanceof LogicalType.EnumType
                || lt instanceof LogicalType.JsonType;
    }

    /// Dictionary entry point. Converts a raw primitive drawn from a
    /// `Dictionary.*` record into the canonical display form for the column's
    /// logical type. `rawValue` must be one of: `Integer`, `Long`, `Float`,
    /// `Double`, `byte[]` — matching the five `Dictionary` subtypes.
    ///
    /// When `useLogicalType=false` the column's logical type is bypassed —
    /// timestamps render as raw long micros, decimals as raw byte hex, etc.
    /// `budget` bounds binary hex building in display cells; text renders whole.
    public static String formatDictionary(Object rawValue, ColumnSchema col,
                                          boolean useLogicalType, int budget) {
        Objects.requireNonNull(col, "col");
        requireBudget(budget);
        LogicalType lt = useLogicalType ? col.logicalType() : null;
        return switch (rawValue) {
            case Integer i -> formatInt(i, lt);
            case Long l -> formatLong(l, lt);
            case Float f -> Float.toString(f);
            case Double d -> Double.toString(d);
            case byte[] bytes -> formatDictionaryBytes(bytes, lt, budget);
            case null -> "null";
            default -> throw unknownDictionaryPrimitive(rawValue);
        };
    }

    private static IllegalStateException unknownDictionaryPrimitive(Object rawValue) {
        return new IllegalStateException(
                "Dictionary records carry Integer, Long, Float, Double or byte[] values, got "
                        + rawValue.getClass().getName());
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
            case LogicalType.TimestampType ts -> (ts.isAdjustedToUTC()
                    ? LogicalTypeConverter.convertToTimestamp(raw, PhysicalType.INT64, ts)
                    : LogicalTypeConverter.convertToLocalTimestamp(raw, PhysicalType.INT64, ts)).toString();
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
    private static String formatDictionaryBytes(byte[] raw, LogicalType lt, int maxChars) {
        requireFixedByteLength(raw, lt);
        return switch (lt) {
            case null -> formatRawBytes(raw, maxChars);
            case LogicalType.StringType s -> Strings.sanitizeControls(new String(raw, StandardCharsets.UTF_8));
            case LogicalType.EnumType e -> Strings.sanitizeControls(new String(raw, StandardCharsets.UTF_8));
            case LogicalType.JsonType j -> Strings.sanitizeControls(new String(raw, StandardCharsets.UTF_8));
            case LogicalType.BsonType b -> Strings.sanitizeControls(new String(raw, StandardCharsets.UTF_8));
            case LogicalType.DecimalType d -> new BigDecimal(new BigInteger(raw), d.scale()).toPlainString();
            case LogicalType.UuidType u when raw.length == 16 -> {
                ByteBuffer bb = ByteBuffer.wrap(raw);
                yield new UUID(bb.getLong(), bb.getLong()).toString();
            }
            case LogicalType.UuidType u -> throw malformedFixedLength("UUID", 16, raw.length);
            case LogicalType.IntervalType i when raw.length == 12 -> formatIntervalBytes(raw);
            case LogicalType.IntervalType i -> throw malformedFixedLength("INTERVAL", 12, raw.length);
            case LogicalType.Float16Type f when raw.length == 2 ->
                    Float.toString(LogicalTypeConverter.convertToFloat16(raw, PhysicalType.FIXED_LEN_BYTE_ARRAY));
            case LogicalType.Float16Type f -> formatRawBytes(raw, maxChars);
            case LogicalType.GeometryType g -> formatRawBytes(raw, maxChars);
            case LogicalType.GeographyType g -> formatRawBytes(raw, maxChars);
            default -> throw notBackedBy(lt, "BYTE_ARRAY");
        };
    }

    private static void requireFixedByteLength(byte[] bytes, LogicalType lt) {
        if (lt instanceof LogicalType.UuidType && bytes.length != 16) {
            throw malformedFixedLength("UUID", 16, bytes.length);
        }
        if (lt instanceof LogicalType.IntervalType && bytes.length != 12) {
            throw malformedFixedLength("INTERVAL", 12, bytes.length);
        }
    }

    private static IllegalArgumentException malformedFixedLength(String type, int expected, int actual) {
        return new IllegalArgumentException(type + " requires exactly " + expected + " bytes, got " + actual);
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

    // ==================== raw statistics bytes ====================

    /// Raw statistics bytes entry point. The whole-value default, equivalent
    /// to `formatBytes(bytes, col, true, NO_LIMIT)`.
    public static String formatBytes(byte[] bytes, ColumnSchema col) {
        return formatBytes(bytes, col, true, BinaryValues.NO_LIMIT);
    }

    /// Logical-type-aware variant of [#formatBytes]. When `useLogicalType=false`,
    /// dispatch is on physical type only — TIMESTAMP / DATE / TIME / DECIMAL /
    /// UUID columns then render as raw int / long / hex form, useful for
    /// confirming the underlying storage in the dive UI.
    public static String formatBytes(byte[] bytes, ColumnSchema col, boolean useLogicalType) {
        return formatBytes(bytes, col, useLogicalType, BinaryValues.NO_LIMIT);
    }

    /// Variant bounding the binary rendering in display cells, for a caller
    /// that fills a cell. The hex of a large payload is built only as far as
    /// that budget, so rendering into a cell costs a cell rather than the
    /// whole payload. The result is still the value, not a cut of it: it runs
    /// just past the budget when the payload is longer, so the caller sees
    /// that there is more and marks what it cut. Absent statistics (`null`
    /// bytes) render as `-`.
    public static String formatBytes(byte[] bytes, ColumnSchema col,
                                     boolean useLogicalType, int budget) {
        requireBudget(budget);
        Objects.requireNonNull(col, "col");
        if (bytes == null) {
            return "-";
        }
        LogicalType lt = useLogicalType ? col.logicalType() : null;
        requireFixedByteLength(bytes, lt);
        if (bytes.length == 0) {
            return isByteBacked(col.type()) ? "\"\"" : "";
        }

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
            return (ts.isAdjustedToUTC()
                    ? LogicalTypeConverter.convertToTimestamp(raw, PhysicalType.INT64, ts)
                    : LogicalTypeConverter.convertToLocalTimestamp(raw, PhysicalType.INT64, ts)).toString();
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
            case INT32 -> formatIndexInt32(bytes, lt);
            case INT64 -> formatIndexInt64(bytes, lt);
            case FLOAT -> Float.toString(StatisticsDecoder.decodeFloat(bytes));
            case DOUBLE -> Double.toString(StatisticsDecoder.decodeDouble(bytes));
            // INT96 carries no logical annotation: logical mode renders the
            // instant, physical mode the raw `0x` hex every other byte-backed
            // type uses.
            case INT96 -> useLogicalType
                    ? LogicalTypeConverter.int96ToInstant(bytes).toString()
                    : BinaryValues.toHex(bytes, budget);
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> formatDictionaryBytes(bytes, lt, budget);
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
        if (lt instanceof LogicalType.DateType || lt instanceof LogicalType.TimeType) {
            return String.valueOf(LogicalTypeConverter.convert(value, col.type(), lt));
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
        if (lt instanceof LogicalType.TimeType || lt instanceof LogicalType.TimestampType) {
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
    /// as `-`; otherwise delegates to the [#formatBytes] pipeline (UUID,
    /// decimal, hex, UTF-8 string, etc.).
    public static String formatDecoded(byte[] value, ColumnSchema col) {
        return formatBytes(value, col);
    }

    /// `LogicalTypeConverter.convert` yields a `BigDecimal` for DECIMAL
    /// columns; the canonical text is its plain string, never scientific
    /// notation.
    private static String decimalText(Object converted) {
        return ((BigDecimal) converted).toPlainString();
    }

    private static String formatIndexInt32(byte[] bytes, LogicalType lt) {
        return formatInt32Value(StatisticsDecoder.decodeInt(bytes), lt);
    }

    private static String formatInt32Value(int v, LogicalType lt) {
        if (lt instanceof LogicalType.IntType it && !it.isSigned()) {
            return Long.toString(Integer.toUnsignedLong(v));
        }
        return Integer.toString(v);
    }

    private static String formatIndexInt64(byte[] bytes, LogicalType lt) {
        return formatInt64Value(StatisticsDecoder.decodeLong(bytes), lt);
    }

    /// A zero-length value is only meaningful for the variable-length physical
    /// types; rendering it as `""` distinguishes "present but empty" from the
    /// blank cell an absent statistic leaves behind.
    private static boolean isByteBacked(PhysicalType pt) {
        return pt == PhysicalType.BYTE_ARRAY || pt == PhysicalType.FIXED_LEN_BYTE_ARRAY;
    }

    private static String formatInt64Value(long v, LogicalType lt) {
        if (lt instanceof LogicalType.IntType it && !it.isSigned()) {
            return Long.toUnsignedString(v);
        }
        return Long.toString(v);
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

    /// Renders a raw byte array through [BinaryValues], which decides text
    /// vs. binary from the bytes. Truncation is left to the caller — the dive
    /// screens already cap each rendered cell to its width — but `maxChars`
    /// keeps a large payload from being hexed far past what the caller can use.
    private static String formatRawBytes(byte[] raw, int maxChars) {
        return BinaryValues.render(raw, maxChars);
    }

    private static final int MAX_NESTED_ELEMENTS = 3;
    private static final int MAX_NESTED_DEPTH = 3;

    /// Whether the COMPACT walk caps its element and depth counts. A preview
    /// cell with a finite budget shows a capped view — the screen clips it
    /// anyway; a `NO_LIMIT` rendering (`convert --format json` leaves) shows
    /// every element. The caps are never applied to the materialised walker,
    /// which renders `print` / CSV cells whole.
    private static boolean capped(int maxChars) {
        return maxChars != BinaryValues.NO_LIMIT;
    }

    /// Renders a nested value (`PqList`, `PqStruct`, `PqMap`, `PqVariant`,
    /// `byte[]`, or any other [Object]) as compact display-grammar text —
    /// `{ a : 1 }`, `[1, 2]` — the same spelling the materialised walker uses.
    /// When the budget is finite the walk is capped at [#MAX_NESTED_ELEMENTS]
    /// visible entries per collection and [#MAX_NESTED_DEPTH] levels of
    /// recursion; the screen further truncates the result to the cell budget.
    private static String formatNested(Object value, int depth, boolean useLogicalType, int maxChars) {
        if (value == null) {
            return "null";
        }
        if (capped(maxChars) && depth >= MAX_NESTED_DEPTH) {
            return "…";
        }
        if (value instanceof PqList list) {
            return formatList(list, depth, useLogicalType, maxChars);
        }
        if (value instanceof PqStruct struct) {
            return formatStruct(struct, depth, useLogicalType, maxChars);
        }
        if (value instanceof PqMap map) {
            return formatMap(map, depth, useLogicalType, maxChars);
        }
        if (value instanceof PqVariant variant) {
            return formatVariant(variant, depth, maxChars);
        }
        if (value instanceof byte[] bytes) {
            return formatRawBytes(bytes, maxChars);
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
        if (value instanceof LocalDateTime ldt) {
            return ldt.toString();
        }
        if (value instanceof String s) {
            return Strings.sanitizeControls(s);
        }
        return String.valueOf(value);
    }

    private static String formatList(PqList list, int depth, boolean useLogicalType, int maxChars) {
        if (list.isEmpty()) {
            return "[]";
        }
        StringBuilder sb = new StringBuilder("[");
        int shown = 0;
        int size = list.size();
        for (int i = 0; i < size; i++) {
            if (capped(maxChars) && shown == MAX_NESTED_ELEMENTS) {
                sb.append(", …+").append(size - MAX_NESTED_ELEMENTS);
                break;
            }
            if (shown > 0) {
                sb.append(", ");
            }
            Object element = list.isNull(i) ? null : list.get(i);
            sb.append(formatNested(element, depth + 1, useLogicalType, maxChars));
            shown++;
        }
        sb.append("]");
        return sb.toString();
    }

    private static String formatStruct(PqStruct struct, int depth, boolean useLogicalType, int maxChars) {
        int count = struct.getFieldCount();
        if (count == 0) {
            return "{}";
        }
        StringBuilder sb = new StringBuilder("{ ");
        int shown = 0;
        for (int i = 0; i < count; i++) {
            if (capped(maxChars) && shown == MAX_NESTED_ELEMENTS) {
                sb.append(", …+").append(count - MAX_NESTED_ELEMENTS);
                break;
            }
            if (shown > 0) {
                sb.append(", ");
            }
            String fieldName = struct.getFieldName(i);
            Object fieldValue = struct.isNull(fieldName) ? null
                    : (useLogicalType ? struct.getValue(fieldName) : struct.getRawValue(fieldName));
            sb.append(fieldName).append(" : ").append(formatNested(fieldValue, depth + 1, useLogicalType, maxChars));
            shown++;
        }
        sb.append(" }");
        return sb.toString();
    }

    private static String formatMap(PqMap map, int depth, boolean useLogicalType, int maxChars) {
        if (map.isEmpty()) {
            return "{}";
        }
        StringBuilder sb = new StringBuilder("{ ");
        int shown = 0;
        java.util.List<PqMap.Entry> entries = map.getEntries();
        for (PqMap.Entry entry : entries) {
            if (capped(maxChars) && shown == MAX_NESTED_ELEMENTS) {
                sb.append(", …+").append(entries.size() - MAX_NESTED_ELEMENTS);
                break;
            }
            if (shown > 0) {
                sb.append(", ");
            }
            Object key = useLogicalType ? entry.getKey() : entry.getRawKey();
            Object value = entry.isValueNull() ? null
                    : (useLogicalType ? entry.getValue() : entry.getRawValue());
            sb.append(formatNested(key, depth + 1, useLogicalType, maxChars))
                    .append(" : ")
                    .append(formatNested(value, depth + 1, useLogicalType, maxChars));
            shown++;
        }
        sb.append(" }");
        return sb.toString();
    }

    private static String formatVariant(PqVariant variant, int depth, int maxChars) {
        return switch (variant.type()) {
            case OBJECT -> formatVariantObject(variant.asObject(), depth, maxChars);
            case ARRAY -> formatVariantArray(variant.asArray(), depth, maxChars);
            default -> variantScalarText(variant, maxChars);
        };
    }

    /// One-line display text for a Variant scalar — unquoted strings, the same
    /// spelling the display grammar uses everywhere.
    private static String variantScalarText(PqVariant variant, int maxChars) {
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
            case TIMESTAMP, TIMESTAMP_NANOS -> variant.asTimestamp().toString();
            case TIMESTAMP_NTZ, TIMESTAMP_NTZ_NANOS -> {
                String s = variant.asTimestamp().toString();
                yield s.endsWith("Z") ? s.substring(0, s.length() - 1) : s;
            }
            case STRING -> Strings.sanitizeControls(variant.asString());
            case BINARY -> formatRawBytes(variant.asBinary(), maxChars);
            case UUID -> variant.asUuid().toString();
            default -> throw new IllegalStateException(
                    "Variant " + variant.type() + " is not a scalar: walk it as a collection");
        };
    }

    private static String formatVariantObject(PqVariantObject obj, int depth, int maxChars) {
        int count = obj.getFieldCount();
        if (count == 0) {
            return "{}";
        }
        StringBuilder sb = new StringBuilder("{ ");
        int shown = 0;
        for (int i = 0; i < count; i++) {
            if (capped(maxChars) && shown == MAX_NESTED_ELEMENTS) {
                sb.append(", …+").append(count - MAX_NESTED_ELEMENTS);
                break;
            }
            if (shown > 0) {
                sb.append(", ");
            }
            String name = obj.getFieldName(i);
            sb.append(name).append(" : ").append(formatNested(obj.getVariant(name), depth + 1, true, maxChars));
            shown++;
        }
        sb.append(" }");
        return sb.toString();
    }

    private static String formatVariantArray(PqVariantArray array, int depth, int maxChars) {
        int size = array.size();
        if (size == 0) {
            return "[]";
        }
        StringBuilder sb = new StringBuilder("[");
        int shown = 0;
        for (int i = 0; i < size; i++) {
            if (capped(maxChars) && shown == MAX_NESTED_ELEMENTS) {
                sb.append(", …+").append(size - MAX_NESTED_ELEMENTS);
                break;
            }
            if (shown > 0) {
                sb.append(", ");
            }
            sb.append(formatNested(array.get(i), depth + 1, true, maxChars));
            shown++;
        }
        sb.append("]");
        return sb.toString();
    }

    private static String formatTime(long raw, LogicalType.TimeUnit unit) {
        long nanosOfDay = switch (unit) {
            case MILLIS -> raw * 1_000_000L;
            case MICROS -> raw * 1_000L;
            case NANOS -> raw;
        };
        return LocalTime.ofNanoOfDay(nanosOfDay).toString();
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

    /// Renders a value as its underlying physical-type text. Bypasses
    /// logical-type dispatch — used when the user toggles logical rendering
    /// off to inspect storage form. byte[]s still hex-render so cells aren't
    /// "[B@" — that's not "physical" rendering, just sane fallback.
    private static String formatPhysical(RowReader reader, int fieldIndex, int maxChars) {
        Object raw = reader.getRawValue(fieldIndex);
        if (raw instanceof byte[] bytes) {
            return formatRawBytes(bytes, maxChars);
        }
        return String.valueOf(raw);
    }
}
