/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.command;

import java.util.List;

import com.github.freva.asciitable.AsciiTable;

import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqList;
import dev.hardwood.row.PqMap;
import dev.hardwood.row.PqStruct;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;
import picocli.CommandLine.Model.CommandSpec;

class RowTable {

    static final int MAX_CELL_WIDTH = 40;

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

    static void rowToTableRow(int fieldCount, List<String[]> rows, RowReader rowReader, int count) {
        int read = 0;
        while (rowReader.hasNext() && read < count) {
            rowReader.next();
            String[] row = new String[fieldCount];
            for (int i = 0; i < fieldCount; i++) {
                row[i] = RowTable.formatCell(rowReader.getValue(i));
            }
            rows.add(row);
            read++;
        }
    }

    static String formatCell(Object value) {
        String s = renderValue(value);
        if (s.length() <= MAX_CELL_WIDTH) {
            return s;
        }
        return s.substring(0, MAX_CELL_WIDTH - 1) + "…";
    }

    static String renderValue(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof byte[] bytes) {
            return "<" + bytes.length + " bytes>";
        }
        if (value instanceof PqStruct struct) {
            return renderStruct(struct);
        }
        if (value instanceof PqList list) {
            return renderList(list);
        }
        if (value instanceof PqMap map) {
            return renderMap(map);
        }
        return String.valueOf(value);
    }

    private static String renderStruct(PqStruct struct) {
        StringBuilder sb = new StringBuilder("{");
        int fieldCount = struct.getFieldCount();
        for (int i = 0; i < fieldCount; i++) {
            if (i > 0)
                sb.append(", ");
            String name = struct.getFieldName(i);
            sb.append(name).append(": ").append(renderValue(struct.getValue(name)));
        }
        return sb.append("}").toString();
    }

    private static String renderList(PqList list) {
        StringBuilder sb = new StringBuilder("[");
        int size = list.size();
        for (int i = 0; i < size; i++) {
            if (i > 0)
                sb.append(", ");
            sb.append(renderValue(list.get(i)));
        }
        return sb.append("]").toString();
    }

    private static String renderMap(PqMap map) {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (PqMap.Entry entry : map.getEntries()) {
            if (!first)
                sb.append(", ");
            first = false;
            sb.append(renderValue(entry.getKey())).append(": ").append(renderValue(entry.getValue()));
        }
        return sb.append("}").toString();
    }

    static void print(CommandSpec spec, String[] headers, List<String[]> rows) {
        Object[][] data = rows.toArray(new String[0][]);
         spec.commandLine().getOut().println(AsciiTable.getTable(headers, data));
    }
}
