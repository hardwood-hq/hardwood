/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.internal.table;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import dev.hardwood.internal.conversion.LogicalTypeConverter;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
import dev.hardwood.row.PqVariant;
import dev.hardwood.row.PqVariantArray;
import dev.hardwood.row.PqVariantObject;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;

public final class RowTable {

    private RowTable() {
    }

    static String[] topLevelFieldNames(FileSchema schema) {
        return topLevelFieldNames(schema, ColumnProjection.all());
    }

    public static String[] topLevelFieldNames(FileSchema schema, ColumnProjection projection) {
        List<SchemaNode> children = schema.getRootNode().children();
        if (projection.projectsAll()) {
            String[] names = new String[children.size()];
            for (int i = 0; i < children.size(); i++) {
                names[i] = children.get(i).name();
            }
            return names;
        }
        Set<String> projectedNames = projection.getProjectedColumnNames();
        return children.stream()
                .map(SchemaNode::name)
                .filter(name -> projectedNames.stream()
                        .anyMatch(p -> p.equals(name) || p.startsWith(name + ".")))
                .toArray(String[]::new);
    }

    public static String renderField(RowReader rowReader, int fieldIndex, SchemaNode fieldSchema) {
        if (isAnnotatedStringField(fieldSchema)) {
            String s = rowReader.getString(fieldIndex);
            return s != null ? s : "null";
        }
        return renderValue(rowReader.getValue(fieldIndex), fieldSchema);
    }

    private static boolean isAnnotatedStringField(SchemaNode node) {
        if (!(node instanceof SchemaNode.PrimitiveNode pn)) {
            return false;
        }
        LogicalType lt = pn.logicalType();
        return lt instanceof LogicalType.StringType
                || lt instanceof LogicalType.EnumType
                || lt instanceof LogicalType.JsonType;
    }

    private static boolean isBareByteArray(SchemaNode node) {
        return node instanceof SchemaNode.PrimitiveNode pn
                && pn.type() == PhysicalType.BYTE_ARRAY
                && pn.logicalType() == null;
    }

    private static boolean isValidUtf8(byte[] bytes) {
        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        try {
            decoder.decode(ByteBuffer.wrap(bytes));
            return true;
        } catch (CharacterCodingException e) {
            return false;
        }
    }

    public static String renderValue(Object value, SchemaNode schema) {
        if (value == null) {
            return "null";
        }
        if (value instanceof PqVariant variant) {
            return renderVariant(variant);
        }
        if (value instanceof byte[] bytes) {
            return renderBytes(bytes, schema);
        }
        if (value instanceof PqStruct struct) {
            return renderStruct(struct, schema instanceof SchemaNode.GroupNode g ? g : null);
        }
        if (value instanceof PqList list) {
            return renderList(list, schema instanceof SchemaNode.GroupNode g ? g : null);
        }
        if (value instanceof PqMap map) {
            return renderMap(map, schema instanceof SchemaNode.GroupNode g ? g : null);
        }

        if (schema instanceof SchemaNode.PrimitiveNode pn && pn.logicalType() instanceof LogicalType.IntType it
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

    private static String renderBytes(byte[] bytes, SchemaNode schema) {
        if (isAnnotatedStringField(schema)) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        if (isBareByteArray(schema)) {
            // Schema omits the STRING annotation, so we can't trust the column to
            // be text. Opportunistically decode when the bytes are valid UTF-8 (the
            // common case for older writers) and summarise otherwise, so binary
            // payloads aren't silently rendered with U+FFFD replacement characters.
            return isValidUtf8(bytes)
                    ? new String(bytes, StandardCharsets.UTF_8)
                    : "<" + bytes.length + " bytes>";
        }
        if (!(schema instanceof SchemaNode.PrimitiveNode pn)) {
            return "<" + bytes.length + " bytes>";
        }
        LogicalType lt = pn.logicalType();
        if (lt instanceof LogicalType.UuidType) {
            ByteBuffer bb = ByteBuffer.wrap(bytes);
            return new UUID(bb.getLong(), bb.getLong()).toString();
        }
        if (lt instanceof LogicalType.DecimalType dt) {
            return new BigDecimal(new BigInteger(bytes), dt.scale()).toPlainString();
        }
        return switch (pn.type()) {
            case INT96 -> decodeInt96Timestamp(bytes);
            case FIXED_LEN_BYTE_ARRAY -> HexFormat.of().formatHex(bytes);
            default -> "<" + bytes.length + " bytes>";
        };
    }

    private static String decodeInt96Timestamp(byte[] bytes) {
        return LogicalTypeConverter.int96ToInstant(bytes).toString();
    }

    private static String renderStruct(PqStruct struct, SchemaNode.GroupNode schemaNode) {
        StringBuilder sb = new StringBuilder("{ ");
        int fieldCount = struct.getFieldCount();
        for (int i = 0; i < fieldCount; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            String name = struct.getFieldName(i);
            SchemaNode childSchema = findChildSchema(schemaNode, name);
            sb.append(name).append(" : ").append(renderValue(struct.getValue(name), childSchema));
        }
        return sb.append(" }").toString();
    }

    private static String renderList(PqList list, SchemaNode.GroupNode schemaNode) {
        SchemaNode elementSchema = schemaNode != null ? schemaNode.getListElement() : null;
        StringBuilder sb = new StringBuilder("[");
        int size = list.size();
        for (int i = 0; i < size; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(renderValue(list.get(i), elementSchema));
        }
        return sb.append("]").toString();
    }

    /// Render a Variant value as a JSON-like text fragment. Matches the example
    /// output shown in the `print` and `convert` commands' specs: objects render
    /// as `{"k": v, ...}`, arrays as `[v, ...]`, scalars as their JSON form, and
    /// the Variant `NULL` type as the literal `null`.
    public static String renderVariant(PqVariant variant) {
        if (variant == null) {
            return "null";
        }
        StringBuilder sb = new StringBuilder();
        appendVariant(sb, variant);
        return sb.toString();
    }

    private static void appendVariant(StringBuilder sb, PqVariant variant) {
        switch (variant.type()) {
            case NULL -> sb.append("null");
            case BOOLEAN_TRUE -> sb.append("true");
            case BOOLEAN_FALSE -> sb.append("false");
            case INT8, INT16, INT32 -> sb.append(variant.asInt());
            case INT64 -> sb.append(variant.asLong());
            case FLOAT -> sb.append(variant.asFloat());
            case DOUBLE -> sb.append(variant.asDouble());
            case DECIMAL4, DECIMAL8, DECIMAL16 -> sb.append(variant.asDecimal().toPlainString());
            case STRING -> appendJsonString(sb, variant.asString());
            case BINARY -> appendJsonString(sb, HexFormat.of().formatHex(variant.asBinary()));
            case DATE -> appendJsonString(sb, variant.asDate().toString());
            case TIME_NTZ -> appendJsonString(sb, variant.asTime().toString());
            case TIMESTAMP, TIMESTAMP_NTZ, TIMESTAMP_NANOS, TIMESTAMP_NTZ_NANOS ->
                appendJsonString(sb, variant.asTimestamp().toString());
            case UUID -> appendJsonString(sb, variant.asUuid().toString());
            case OBJECT -> appendVariantObject(sb, variant.asObject());
            case ARRAY -> appendVariantArray(sb, variant.asArray());
        }
    }

    private static void appendVariantObject(StringBuilder sb, PqVariantObject object) {
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
                appendVariant(sb, fieldValue);
            }
        }
        sb.append('}');
    }

    private static void appendVariantArray(StringBuilder sb, PqVariantArray array) {
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
                appendVariant(sb, element);
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
                        sb.append(String.format("\\u%04x", (int) c));
                    }
                    else {
                        sb.append(c);
                    }
                }
            }
        }
        sb.append('"');
    }

    private static String renderMap(PqMap map, SchemaNode.GroupNode schemaNode) {
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
            sb.append(renderValue(entry.getKey(), keySchema)).append(" : ").append(renderValue(entry.getValue(), valueSchema));
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

    public static String renderTable(String[] headers, List<String[]> rows) {
        return renderTable(headers, rows, Collections.emptyList(), Collections.emptyList());
    }

    /// Renders a table like [renderTable(String[], List)], but inserts a horizontal
    /// border line before each row whose index appears in `separatorsBefore`. Indices
    /// refer to positions within `rows` (0 = first data row). Rows listed in
    /// `heavySeparatorsBefore` get a heavier separator (`=` instead of `-`) to visually
    /// distinguish summary sections such as totals.
    ///
    /// Column widths are computed from terminal display width so that East Asian
    /// wide characters (CJK, Hangul, Kana, Fullwidth forms) contribute 2 cells each,
    /// keeping alignment correct across rows that mix Latin and wide-character text.
    public static String renderTable(String[] headers, List<String[]> rows,
                                     List<Integer> separatorsBefore,
                                     List<Integer> heavySeparatorsBefore) {
        int cols = headers.length;
        int[] widths = new int[cols];
        for (int i = 0; i < cols; i++) {
            widths[i] = displayWidth(headers[i]);
        }
        for (String[] row : rows) {
            for (int i = 0; i < cols; i++) {
                widths[i] = Math.max(widths[i], displayWidth(row[i]));
            }
        }

        String lightBorder = buildBorder(widths, '-');
        String heavyBorder = buildBorder(widths, '=');
        Set<Integer> lightSet = new HashSet<>(separatorsBefore);
        Set<Integer> heavySet = new HashSet<>(heavySeparatorsBefore);

        StringBuilder sb = new StringBuilder();
        sb.append(lightBorder).append('\n');
        sb.append(renderCells(headers, widths, false)).append('\n');
        sb.append(lightBorder).append('\n');
        for (int r = 0; r < rows.size(); r++) {
            if (heavySet.contains(r)) {
                sb.append(heavyBorder).append('\n');
            }
            else if (lightSet.contains(r)) {
                sb.append(lightBorder).append('\n');
            }
            sb.append(renderCells(rows.get(r), widths, true)).append('\n');
        }
        sb.append(lightBorder);
        return sb.toString();
    }

    private static String buildBorder(int[] widths, char fill) {
        StringBuilder sb = new StringBuilder();
        sb.append('+');
        for (int w : widths) {
            for (int i = 0; i < w + 2; i++) {
                sb.append(fill);
            }
            sb.append('+');
        }
        return sb.toString();
    }

    private static String renderCells(String[] cells, int[] widths, boolean rightAlign) {
        StringBuilder sb = new StringBuilder();
        sb.append('|');
        for (int i = 0; i < cells.length; i++) {
            String cell = cells[i];
            int padding = widths[i] - displayWidth(cell);
            sb.append(' ');
            if (rightAlign) {
                appendSpaces(sb, padding);
                sb.append(cell);
            }
            else {
                sb.append(cell);
                appendSpaces(sb, padding);
            }
            sb.append(' ');
            sb.append('|');
        }
        return sb.toString();
    }

    private static void appendSpaces(StringBuilder sb, int count) {
        for (int i = 0; i < count; i++) {
            sb.append(' ');
        }
    }

    /// Returns the number of terminal cells the string occupies. East Asian wide
    /// characters (CJK ideographs, Hangul, Kana, Fullwidth forms) count as 2; other
    /// characters count as 1. Surrogate pairs are counted once per code point.
    static int displayWidth(String s) {
        int width = 0;
        int i = 0;
        int len = s.length();
        while (i < len) {
            int cp = s.codePointAt(i);
            width += isWideCodePoint(cp) ? 2 : 1;
            i += Character.charCount(cp);
        }
        return width;
    }

    private static boolean isWideCodePoint(int cp) {
        return (cp >= 0x1100 && cp <= 0x115F)     // Hangul Jamo
                || (cp >= 0x2E80 && cp <= 0x303E) // CJK Radicals, Kangxi, CJK Symbols & Punctuation
                || (cp >= 0x3041 && cp <= 0x33FF) // Hiragana, Katakana, Bopomofo, Hangul Compat, CJK Strokes
                || (cp >= 0x3400 && cp <= 0x4DBF) // CJK Unified Ideographs Extension A
                || (cp >= 0x4E00 && cp <= 0x9FFF) // CJK Unified Ideographs
                || (cp >= 0xA000 && cp <= 0xA4CF) // Yi Syllables & Radicals
                || (cp >= 0xAC00 && cp <= 0xD7A3) // Hangul Syllables
                || (cp >= 0xF900 && cp <= 0xFAFF) // CJK Compatibility Ideographs
                || (cp >= 0xFE30 && cp <= 0xFE4F) // CJK Compatibility Forms
                || (cp >= 0xFF00 && cp <= 0xFF60) // Fullwidth Forms
                || (cp >= 0xFFE0 && cp <= 0xFFE6) // Fullwidth Signs
                || (cp >= 0x20000 && cp <= 0x2FFFD) // CJK Extensions B–F
                || (cp >= 0x30000 && cp <= 0x3FFFD); // CJK Extension G
    }
}
