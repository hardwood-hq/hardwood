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
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.github.freva.asciitable.AsciiTable;

import dev.hardwood.internal.conversion.LogicalTypeConverter;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
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
        if (isStringField(fieldSchema)) {
            String s = rowReader.getString(fieldIndex);
            return s != null ? s : "null";
        }
        return renderValue(rowReader.getValue(fieldIndex), fieldSchema);
    }

    private static boolean isStringField(SchemaNode node) {
        if (!(node instanceof SchemaNode.PrimitiveNode pn)) {
            return false;
        }
        LogicalType lt = pn.logicalType();
        if (lt instanceof LogicalType.StringType
                || lt instanceof LogicalType.EnumType
                || lt instanceof LogicalType.JsonType) {
            return true;
        }
        // BYTE_ARRAY with no logical type is treated as a string, consistent with
        // ValueConverter.convertValue() fallback behavior.
        return lt == null && pn.type() == PhysicalType.BYTE_ARRAY;
    }

    public static String renderValue(Object value, SchemaNode schema) {
        if (value == null) {
            return "null";
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
        if (isStringField(schema)) {
            return new String(bytes, StandardCharsets.UTF_8);
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
        Object[][] data = rows.toArray(new String[0][]);
        return AsciiTable.getTable(AsciiTable.BASIC_ASCII_NO_DATA_SEPARATORS, headers, null, data);
    }

    /// Renders a table like [renderTable(String[], List)], but inserts a horizontal
    /// border line before each row whose index appears in `separatorsBefore`. Indices
    /// refer to positions within `rows` (0 = first data row). Rows listed in
    /// `heavySeparatorsBefore` get a heavier separator (`=` instead of `-`) to visually
    /// distinguish summary sections such as totals.
    public static String renderTable(String[] headers, List<String[]> rows,
                                     List<Integer> separatorsBefore,
                                     List<Integer> heavySeparatorsBefore) {
        String table = renderTable(headers, rows);
        if (separatorsBefore.isEmpty() && heavySeparatorsBefore.isEmpty()) {
            return table;
        }
        String[] lines = table.split("\n", -1);
        String lightBorder = lines[0];
        String heavyBorder = lightBorder.replace('-', '=');
        Set<Integer> lightSet = new HashSet<>(separatorsBefore);
        Set<Integer> heavySet = new HashSet<>(heavySeparatorsBefore);
        int extraLines = separatorsBefore.size() + heavySeparatorsBefore.size();
        StringBuilder sb = new StringBuilder(table.length() + (lightBorder.length() + 1) * extraLines);
        for (int i = 0; i < lines.length; i++) {
            if (i >= 3 && i < lines.length - 1) {
                int dataRowIdx = i - 3;
                if (heavySet.contains(dataRowIdx)) {
                    sb.append(heavyBorder).append('\n');
                }
                else if (lightSet.contains(dataRowIdx)) {
                    sb.append(lightBorder).append('\n');
                }
            }
            sb.append(lines[i]);
            if (i < lines.length - 1) {
                sb.append('\n');
            }
        }
        return sb.toString();
    }
}
