/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.internal;

import java.math.BigDecimal;
import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.Supplier;

import dev.hardwood.internal.conversion.FixedWidths;
import dev.hardwood.internal.conversion.IntOutOfRangeException;
import dev.hardwood.internal.conversion.LogicalTypeConverter;
import dev.hardwood.internal.predicate.StatisticsDecoder;
import dev.hardwood.internal.reader.row.FieldSchemaAccessor;
import dev.hardwood.internal.schema.AnnotationKind;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqInterval;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
import dev.hardwood.row.PqVariant;
import dev.hardwood.row.PqVariantArray;
import dev.hardwood.row.PqVariantObject;
import dev.hardwood.row.StructAccessor;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.SchemaNode;

/// Canonical rendering of Parquet values for every CLI surface — `print`,
/// `convert`, the `inspect` commands and the `dive` TUI. A logical type spells
/// the same text from every source: ISO-8601 timestamps, `yyyy-MM-dd` dates,
/// plain-string decimals and canonical UUIDs.
///
/// The entry points differ in where the value comes from:
///
/// - [#formatField(StructAccessor, int, SchemaNode, boolean, Style, int)] — a
///   field of a row or struct: `print` cells, `convert` cells, dive preview
///   cells and the dive record modal.
/// - [#formatDictionary(Object, ColumnSchema, boolean, int)] — a raw primitive
///   out of a parsed `Dictionary`.
/// - `formatBytes` — raw statistics bytes.
///
/// Core decodes every value: a field is read through `getValue`, and the
/// dictionary and statistics sources decode their stored value through
/// [LogicalTypeConverter#convert]. One leaf renderer spells the decoded Java
/// value, so the annotation decides text or bytes in core alone. Nested values
/// render through one walker; [Style] picks the layout, whether control characters are
/// sanitised, and whether nested values are written as JSON. Statistics and
/// dictionary entries come from the file unchecked: a value its annotation
/// cannot decode renders there in its stored form.
///
/// A viewing style renders a value core refuses as no value of its annotation (an
/// `INT(8)` or `INT(16)` outside its bit width, a `TIME` outside a day, a shredded
/// Variant whose `typed_value` is such an integer) as a marker naming the stored value and
/// the reason, such as `<300: outside INT_8 range>`, and renders the rest of the row.
/// `EXPORT` lets the refusal propagate, so `convert` writes no value the file does not hold.
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

    // ==================== fields ====================

    /// Renders the field at `fieldIndex` of a row or struct — a [RowReader] on its
    /// current row, or a [PqStruct]. `field` is the field's schema node; it
    /// decides how nested values walk and whether integers render unsigned. A
    /// group schema paired with a scalar value throws.
    ///
    /// `useLogicalType=true` renders timestamps, decimals, UUIDs, etc. in their
    /// canonical logical form; `useLogicalType=false` renders the stored physical
    /// value (e.g. `1735689600000000` instead of `2025-01-01T00:00:00Z`). Groups
    /// and repeated fields render structurally in both modes; the toggle decides
    /// how their leaves are read.
    public static String formatField(StructAccessor accessor, int fieldIndex, SchemaNode field,
                                     boolean useLogicalType, Style style, int budget) {
        return renderField(accessor, fieldIndex, field, useLogicalType, style, budget).text();
    }

    /// A field's text, and whether it holds a marker for a value core refused, which a
    /// renderer styles as an error.
    ///
    /// @param text the field's text, as [#formatField] returns it
    /// @param undecodable whether the text holds a marker for a value core refused
    public record RenderedField(String text, boolean undecodable) {
    }

    /// Renders the field as [#formatField] does, reporting whether it holds a marker for a
    /// value core refused.
    public static RenderedField renderField(StructAccessor accessor, int fieldIndex, SchemaNode field,
                                            boolean useLogicalType, Style style, int budget) {
        Objects.requireNonNull(accessor, "accessor");
        Objects.requireNonNull(field, "field");
        Objects.requireNonNull(style, "style");
        requireBudget(budget);
        Marks marks = new Marks();
        String text = render(read(accessor, fieldIndex, field, useLogicalType, style), field, useLogicalType,
                style, 0, budget, marks);
        return new RenderedField(text, marks.undecodable);
    }

    /// The value of the field at `fieldIndex` as the formatter renders it for export: decoded,
    /// apart from a `FILE` group, which renders as its stored fields so that a file holding an
    /// invalid reference still prints.
    public static Object valueOf(StructAccessor accessor, int fieldIndex, SchemaNode field) {
        return read(accessor, fieldIndex, field, true, Style.EXPORT);
    }

    /// The field at `fieldIndex`: `null` when absent, read through the raw accessor
    /// when [#readsRaw], decoded otherwise.
    private static Object read(StructAccessor accessor, int fieldIndex, SchemaNode field, boolean useLogicalType,
                               Style style) {
        if (accessor.isNull(fieldIndex)) {
            return null;
        }
        try {
            return readsRaw(field, useLogicalType) ? accessor.getRawValue(fieldIndex) : accessor.getValue(fieldIndex);
        }
        catch (ParquetReadException | DateTimeException e) {
            return undecodable(e, field, style, () -> accessor.getRawValue(fieldIndex));
        }
    }

    /// Element `i` of a list, read as [#read] reads a field.
    private static Object element(PqList list, int i, SchemaNode schema, boolean useLogicalType, Style style) {
        if (list.isNull(i)) {
            return null;
        }
        try {
            return readsRaw(schema, useLogicalType) ? list.getRaw(i) : list.get(i);
        }
        catch (ParquetReadException | DateTimeException e) {
            return undecodable(e, schema, style, () -> list.getRaw(i));
        }
    }

    /// The key of a map entry, read as [#read] reads a field.
    private static Object mapKey(PqMap.Entry entry, SchemaNode schema, boolean useLogicalType, Style style) {
        try {
            return readsRaw(schema, useLogicalType) ? entry.getRawKey() : entry.getKey();
        }
        catch (ParquetReadException | DateTimeException e) {
            return undecodable(e, schema, style, entry::getRawKey);
        }
    }

    /// The value of a map entry, read as [#read] reads a field.
    private static Object mapValue(PqMap.Entry entry, SchemaNode schema, boolean useLogicalType, Style style) {
        if (entry.isValueNull()) {
            return null;
        }
        try {
            return readsRaw(schema, useLogicalType) ? entry.getRawValue() : entry.getValue();
        }
        catch (ParquetReadException | DateTimeException e) {
            return undecodable(e, schema, style, entry::getRawValue);
        }
    }

    /// A value core refused as no value of its annotation, which a viewing style renders as a
    /// marker naming the stored value and the reason.
    private record Undecodable(String text) {
    }

    /// Whether a render produced an [Undecodable] marker.
    private static final class Marks {
        private boolean undecodable;
    }

    /// The marker for a value core refused as no value of its annotation, in a viewing style:
    /// `<300: outside INT_8 range>` for an `INT(8)` / `INT(16)` outside its bit width (the value
    /// unsigned under an unsigned annotation), `<variant: 300 outside INT_8 range>` for a shredded
    /// Variant whose `typed_value` is one, and `<86400000: outside a day>` for a `TIME` outside a
    /// day. `stored` reads the value as the file stores it. The failure is rethrown in `EXPORT`,
    /// and wherever it is not one of these refusals: any other failure is a fault the read reports.
    private static Undecodable undecodable(RuntimeException e, SchemaNode schema, Style style,
                                           Supplier<Object> stored) {
        if (style == Style.EXPORT) {
            throw e;
        }
        IntOutOfRangeException range = IntOutOfRangeException.in(e);
        boolean variant = schema instanceof SchemaNode.GroupNode group && group.isVariant();
        LogicalType lt = schema instanceof SchemaNode.PrimitiveNode pn ? pn.logicalType() : null;
        if (range != null && (variant || lt instanceof LogicalType.IntType)) {
            String outside = "outside " + range.annotation() + " range";
            return new Undecodable(variant
                    ? "<variant: " + range.value() + " " + outside + ">"
                    : "<" + range.value() + ": " + outside + ">");
        }
        if (e instanceof DateTimeException && lt instanceof LogicalType.TimeType) {
            return new Undecodable("<" + leaf(stored.get(), lt, Style.COMPACT, BinaryValues.NO_LIMIT)
                    + ": outside a day>");
        }
        throw e;
    }

    /// Whether a value is read through the raw accessors: every value when `useLogicalType` is
    /// off, and a `FILE` group always, which renders as its stored fields so that a file holding
    /// an invalid reference still prints.
    private static boolean readsRaw(SchemaNode schema, boolean useLogicalType) {
        return !useLogicalType || isFile(schema);
    }

    private static boolean isFile(SchemaNode schema) {
        return schema instanceof SchemaNode.GroupNode group && group.isFile();
    }

    // ==================== the nested walker ====================

    /// Renders `value` against its schema node: a `PqStruct`, `PqList`, `PqMap`
    /// or `PqVariant` entry by entry, anything else as a leaf.
    /// `useLogicalType=false` reads struct fields and map entries through their
    /// raw accessors. In `EXPORT` a nested value is JSON, and a leaf inside one
    /// a JSON value.
    private static String render(Object value, SchemaNode schema, boolean useLogicalType, Style style,
                                 int depth, int budget, Marks marks) {
        if (value == null) {
            return "null";
        }
        if (value instanceof Undecodable undecodable) {
            marks.undecodable = true;
            return undecodable.text();
        }
        if (style == Style.PREVIEW && depth >= MAX_NESTED_DEPTH) {
            return String.valueOf(Strings.ELLIPSIS);
        }
        return switch (value) {
            case PqVariant variant -> renderVariant(variant, style, depth, budget, marks);
            case PqStruct struct -> renderStruct(struct, useLogicalType, style, depth, budget, marks);
            case PqList list -> renderList(list, listElementSchema(schema), useLogicalType, style, depth, budget,
                    marks);
            case PqMap map -> renderMap(map, asGroup(schema), useLogicalType, style, depth, budget, marks);
            default -> style == Style.EXPORT && depth > 0
                    ? jsonLeaf(value, schema, useLogicalType, budget)
                    : renderLeaf(value, schema, useLogicalType, style, budget);
        };
    }

    /// Reads every field by position: two sibling fields may share a name, and a projected
    /// struct's positions follow the projection rather than the file's group, so only the struct
    /// knows which schema node each position holds.
    private static String renderStruct(PqStruct struct, boolean useLogicalType,
                                       Style style, int depth, int budget, Marks marks) {
        int count = struct.getFieldCount();
        int shown = shownEntries(count, style);
        List<String> entries = new ArrayList<>(shown);
        for (int i = 0; i < shown; i++) {
            SchemaNode childSchema = fieldSchema(struct, i);
            entries.add(entry(key(struct.getFieldName(i), style),
                    render(read(struct, i, childSchema, useLogicalType, style), childSchema, useLogicalType, style,
                            depth + 1, budget, marks),
                    style));
        }
        return enclose(true, entries, count, style, depth);
    }

    private static String renderList(PqList list, SchemaNode elementSchema, boolean useLogicalType,
                                     Style style, int depth, int budget, Marks marks) {
        int size = list.size();
        int shown = shownEntries(size, style);
        List<String> entries = new ArrayList<>(shown);
        for (int i = 0; i < shown; i++) {
            Object element = element(list, i, elementSchema, useLogicalType, style);
            entries.add(render(element, elementSchema, useLogicalType, style, depth + 1, budget, marks));
        }
        return enclose(false, entries, size, style, depth);
    }

    private static String renderMap(PqMap map, SchemaNode.GroupNode schema, boolean useLogicalType,
                                    Style style, int depth, int budget, Marks marks) {
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
            Object key = mapKey(entry, keySchema, useLogicalType, style);
            Object value = mapValue(entry, valueSchema, useLogicalType, style);
            // A JSON object key is a string: the map key's text, whatever its type.
            String keyText = style == Style.EXPORT
                    ? jsonString(render(key, keySchema, useLogicalType, style, 0, budget, marks))
                    : render(key, keySchema, useLogicalType, style, depth + 1, budget, marks);
            entries.add(entry(keyText, render(value, valueSchema, useLogicalType, style, depth + 1, budget, marks),
                    style));
        }
        return enclose(true, entries, all.size(), style, depth);
    }

    private static String renderVariant(PqVariant variant, Style style, int depth, int budget, Marks marks) {
        return switch (variant.type()) {
            case OBJECT -> renderVariantObject(variant.asObject(), style, depth, budget, marks);
            case ARRAY -> renderVariantArray(variant.asArray(), style, depth, budget, marks);
            default -> style == Style.EXPORT
                    ? variantJsonScalar(variant, budget)
                    : variantScalarText(variant, style, budget);
        };
    }

    private static String renderVariantObject(PqVariantObject object, Style style, int depth, int budget,
                                              Marks marks) {
        int count = object.getFieldCount();
        int shown = shownEntries(count, style);
        List<String> entries = new ArrayList<>(shown);
        for (int i = 0; i < shown; i++) {
            // The raw name looks the value up; only the rendered name is sanitised.
            String name = object.getFieldName(i);
            entries.add(entry(key(name, style),
                    render(object.getVariant(name), null, true, style, depth + 1, budget, marks), style));
        }
        return enclose(true, entries, count, style, depth);
    }

    private static String renderVariantArray(PqVariantArray array, Style style, int depth, int budget,
                                             Marks marks) {
        int size = array.size();
        int shown = shownEntries(size, style);
        List<String> entries = new ArrayList<>(shown);
        for (int i = 0; i < shown; i++) {
            entries.add(render(array.get(i), null, true, style, depth + 1, budget, marks));
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

    /// A Variant scalar as JSON, at any depth: `null`, booleans, integers and
    /// decimals native, a finite float or double a number, and everything else,
    /// `NaN` and `±Infinity` included, a string.
    private static String variantJsonScalar(PqVariant variant, int budget) {
        String text = variantScalarText(variant, Style.EXPORT, budget);
        boolean nativeJson = switch (variant.type()) {
            case NULL, BOOLEAN_TRUE, BOOLEAN_FALSE, INT8, INT16, INT32, INT64, DECIMAL4, DECIMAL8, DECIMAL16 -> true;
            case FLOAT -> Float.isFinite(variant.asFloat());
            case DOUBLE -> Double.isFinite(variant.asDouble());
            default -> false;
        };
        return nativeJson ? text : jsonString(text);
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
        LogicalType lt = useLogicalType && schema instanceof SchemaNode.PrimitiveNode pn ? pn.logicalType() : null;
        return leaf(value, lt, style, budget);
    }

    /// The text of a decoded leaf. Core has decided its Java type: a `String` is
    /// text, a `byte[]` a payload with no decoder, which [BinaryValues] renders.
    /// `lt` is the annotation the value was decoded under, or `null` for a
    /// physical value; an unsigned `INT` renders unsigned.
    private static String leaf(Object value, LogicalType lt, Style style, int budget) {
        return switch (value) {
            case byte[] bytes -> BinaryValues.render(bytes, budget);
            case String s -> text(s, style);
            case BigDecimal decimal -> decimal.toPlainString();
            case PqInterval interval -> formatInterval(interval);
            case Integer i when AnnotationKind.ordersUnsigned(lt) -> Long.toString(Integer.toUnsignedLong(i));
            case Long l when AnnotationKind.ordersUnsigned(lt) -> Long.toUnsignedString(l);
            default -> String.valueOf(value);
        };
    }

    private static int shownEntries(int total, Style style) {
        return style == Style.PREVIEW ? Math.min(total, MAX_NESTED_ELEMENTS) : total;
    }

    /// An object key: a JSON string in `EXPORT`, sanitised text otherwise.
    private static String key(String name, Style style) {
        return style == Style.EXPORT ? jsonString(name) : text(name, style);
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

    /// The schema node of the struct's field at `fieldIndex`, or `null` for a struct the reader
    /// did not produce, which renders like any value formatted without a schema.
    private static SchemaNode fieldSchema(PqStruct struct, int fieldIndex) {
        return struct instanceof FieldSchemaAccessor accessor ? accessor.getFieldSchema(fieldIndex) : null;
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

    // ==================== dictionary and statistics sources ====================

    /// Renders a raw primitive drawn from a parsed `Dictionary` — an `Integer`,
    /// `Long`, `Float`, `Double` or `byte[]`, matching the five `Dictionary`
    /// subtypes — in the column's canonical display form. `useLogicalType=false`
    /// bypasses the logical type: timestamps render as their stored long,
    /// decimals as their unscaled bytes. A value its annotation cannot decode
    /// renders in its stored form. `budget` bounds hex building in display
    /// cells; text renders whole.
    public static String formatDictionary(Object rawValue, ColumnSchema col,
                                          boolean useLogicalType, int budget) {
        Objects.requireNonNull(col, "col");
        requireBudget(budget);
        if (rawValue == null) {
            return "null";
        }
        if (!(rawValue instanceof Integer || rawValue instanceof Long || rawValue instanceof Float
                || rawValue instanceof Double || rawValue instanceof byte[])) {
            throw new IllegalStateException(
                    "Dictionary records carry Integer, Long, Float, Double or byte[] values, got "
                            + rawValue.getClass().getName());
        }
        return formatStored(rawValue, col, useLogicalType, budget);
    }

    /// Renders statistics bytes whole: the four-argument `formatBytes` with
    /// [BinaryValues#NO_LIMIT].
    public static String formatBytes(byte[] bytes, ColumnSchema col, boolean useLogicalType) {
        return formatBytes(bytes, col, useLogicalType, BinaryValues.NO_LIMIT);
    }

    /// Renders raw statistics bytes — a min/max bound from a column chunk or the
    /// page index — for a caller that fills a cell. The hex of a large payload is
    /// built only as far as `budget`, running just past it when the payload is
    /// longer so the caller sees there is more and marks what it cut. Absent
    /// statistics (`null` bytes) render as [Strings#ABSENT_VALUE]; empty ones render as `""` on a
    /// byte-array column. `useLogicalType=false` dispatches on the physical type
    /// only, so TIMESTAMP / DATE / TIME / DECIMAL / UUID columns render in their
    /// stored int, long or hex form. Bytes of the wrong width for the physical
    /// type, and a value its annotation cannot decode, render in stored form.
    public static String formatBytes(byte[] bytes, ColumnSchema col,
                                     boolean useLogicalType, int budget) {
        requireBudget(budget);
        Objects.requireNonNull(col, "col");
        if (bytes == null) {
            return Strings.ABSENT_VALUE;
        }
        if (bytes.length == 0) {
            return isByteBacked(col.type()) ? "\"\"" : "";
        }
        if (!hasPhysicalWidth(bytes, col.type())) {
            // Bytes of the wrong width are not a value of the physical type.
            return BinaryValues.toHex(bytes, budget);
        }
        Object stored = switch (col.type()) {
            case BOOLEAN -> StatisticsDecoder.decodeBoolean(bytes);
            case INT32 -> StatisticsDecoder.decodeInt(bytes);
            case INT64 -> StatisticsDecoder.decodeLong(bytes);
            case FLOAT -> StatisticsDecoder.decodeFloat(bytes);
            case DOUBLE -> StatisticsDecoder.decodeDouble(bytes);
            case INT96, BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> bytes;
        };
        return formatStored(stored, col, useLogicalType, budget);
    }

    /// A value as the file stores it, decoded the way the reader decodes it and
    /// rendered as a leaf. These values come from the file unchecked, so one the
    /// annotation cannot decode — a payload of the wrong width, a `TIME` outside
    /// a day, a `TIMESTAMP` past the range of the Java type, any value of a
    /// `NULL` column, an `INT(8)` or `INT(16)` outside its bit width — renders in its stored
    /// form: the number (unsigned under an unsigned `INT`), or the bytes as
    /// `0x` hex. An INT96 timestamp decodes to its instant, and renders as hex in
    /// physical mode.
    private static String formatStored(Object stored, ColumnSchema col, boolean useLogicalType, int budget) {
        if (!useLogicalType) {
            return col.type() == PhysicalType.INT96
                    ? BinaryValues.toHex((byte[]) stored, budget)
                    : leaf(stored, null, Style.COMPACT, budget);
        }
        LogicalType lt = col.logicalType();
        boolean int96 = LogicalTypeConverter.isLegacyInt96Timestamp(col.type(), lt);
        if (stored instanceof byte[] bytes && bytes.length != decodedWidth(int96, lt, bytes.length)) {
            return BinaryValues.toHex(bytes, budget);
        }
        try {
            Object value = int96
                    ? LogicalTypeConverter.int96ToInstant((byte[]) stored)
                    : LogicalTypeConverter.convert(stored, col.type(), lt);
            return leaf(value, lt, Style.COMPACT, budget);
        }
        catch (DateTimeException | ParquetReadException e) {
            // A stored number renders through leaf, which reads it unsigned under an unsigned INT
            return stored instanceof byte[] bytes
                    ? BinaryValues.toHex(bytes, budget)
                    : leaf(stored, lt, Style.COMPACT, budget);
        }
    }

    /// The width a byte-backed value must have for its decoder: fixed for INT96
    /// and the UUID, INTERVAL, FLOAT16 and `FIXED_LEN_BYTE_ARRAY` TIMESTAMP
    /// annotations, `length` itself for any other.
    private static int decodedWidth(boolean int96, LogicalType lt, int length) {
        if (int96) {
            return FixedWidths.INT96;
        }
        return switch (lt) {
            case LogicalType.UuidType u -> FixedWidths.UUID;
            case LogicalType.IntervalType i -> FixedWidths.INTERVAL;
            case LogicalType.Float16Type f -> FixedWidths.FLOAT16;
            case LogicalType.TimestampType t -> FixedWidths.FLBA12_TIMESTAMP;
            case null, default -> length;
        };
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
            case INT32, FLOAT, INT64, DOUBLE, INT96 -> bytes.length == FixedWidths.of(type);
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> true;
        };
    }

    // ==================== shared leaf helpers ====================

    private static String formatInterval(PqInterval interval) {
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
