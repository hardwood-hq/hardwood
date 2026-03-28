/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.command;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;

import com.github.freva.asciitable.AsciiTable;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;
import picocli.CommandLine.Model.CommandSpec;

final class RowTable {

    private RowTable() {
    }

    static String[] topLevelFieldNames(FileSchema schema) {
        List<SchemaNode> children = schema.getRootNode().children();
        String[] names = new String[children.size()];
        for (int i = 0; i < children.size(); i++) {
            names[i] = children.get(i).name();
        }
        return names;
    }

    static String renderField(RowReader rowReader, int fieldIndex, SchemaNode fieldSchema) {
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

    static String renderValue(Object value, SchemaNode schema) {
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

    // INT96 = legacy Spark timestamp: bytes 0–7 LE nanoseconds-of-day, bytes 8–11 LE Julian day number
    private static final long JULIAN_EPOCH_OFFSET_DAYS = 2440588L;

    private static String decodeInt96Timestamp(byte[] bytes) {
        ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        long nanosOfDay = bb.getLong(0);
        int julianDay = bb.getInt(8);
        long daysFromEpoch = (long) julianDay - JULIAN_EPOCH_OFFSET_DAYS;
        long secondsFromEpoch = daysFromEpoch * 86400L + nanosOfDay / 1_000_000_000L;
        long nanoAdjust = nanosOfDay % 1_000_000_000L;
        return Instant.ofEpochSecond(secondsFromEpoch, nanoAdjust).toString();
    }

    private static String renderStruct(PqStruct struct, SchemaNode.GroupNode schemaNode) {
        StringBuilder sb = new StringBuilder("{");
        int fieldCount = struct.getFieldCount();
        for (int i = 0; i < fieldCount; i++) {
            if (i > 0)
                sb.append(", ");
            String name = struct.getFieldName(i);
            SchemaNode childSchema = findChildSchema(schemaNode, name);
            sb.append(name).append(": ").append(renderValue(struct.getValue(name), childSchema));
        }
        return sb.append("}").toString();
    }

    private static String renderList(PqList list, SchemaNode.GroupNode schemaNode) {
        SchemaNode elementSchema = schemaNode != null ? schemaNode.getListElement() : null;
        StringBuilder sb = new StringBuilder("[");
        int size = list.size();
        for (int i = 0; i < size; i++) {
            if (i > 0)
                sb.append(", ");
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
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (PqMap.Entry entry : map.getEntries()) {
            if (!first)
                sb.append(", ");
            first = false;
            sb.append(renderValue(entry.getKey(), keySchema)).append(": ").append(renderValue(entry.getValue(), valueSchema));
        }
        return sb.append("}").toString();
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

    static String renderTable(String[] headers, List<String[]> rows) {
        Object[][] data = rows.toArray(new String[0][]);
        return AsciiTable.getTable(AsciiTable.BASIC_ASCII_NO_DATA_SEPARATORS, headers, null, data);
    }

    static void print(CommandSpec spec, String[] headers, List<String[]> rows) {
        spec.commandLine().getOut().println(renderTable(headers, rows));
    }
}
