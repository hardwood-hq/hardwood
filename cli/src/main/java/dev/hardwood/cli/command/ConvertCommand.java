/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.invocation.CommandInvocation;
import org.aesh.command.option.Mixin;
import org.aesh.command.option.Option;

import dev.hardwood.InputFile;
import dev.hardwood.cli.internal.table.RowTable;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqStruct;
import dev.hardwood.row.PqVariant;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;

@CommandDefinition(name = "convert", description = "Convert Parquet file to CSV or JSON.", generateHelp = true)
public class ConvertCommand implements Command<CommandInvocation> {

    enum Format {
        CSV,
        JSON
    }

    @Mixin
    FileMixin fileMixin;

    @Option(shortName = 'F', name = "format", required = true, description = "Output format: csv, json.")
    Format format;

    @Option(shortName = 'o', name = "output", description = "Output file path (default: stdout).")
    String outputFile;

    @Option(shortName = 'c', name = "columns", description = "Comma-separated list of columns to include. Supports nested fields via dot notation (e.g. 'account.id').")
    String columns;

    @Option(shortName = 'n', name = "rows", defaultValue = RowLimits.ALL, description = "Number of rows to convert. Positive values convert the first N rows (head), negative values convert the last N rows (tail), 'ALL' converts every row.")
    String n;

    @Override
    public CommandResult execute(CommandInvocation ci) {
        InputFile inputFile = fileMixin.toInputFile();
        if (inputFile == null) {
            return CommandResult.FAILURE;
        }

        int rowLimit = RowLimits.parse(n);
        ColumnProjection projection = parseColumnProjection();

        try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
            FileSchema fileSchema = reader.getFileSchema();
            List<SchemaNode> fields = projectedFields(fileSchema, projection);

            PrintWriter out = openOutput();
            try (RowReader rowReader = RowLimits.buildRowReader(reader, projection, rowLimit)) {
                switch (format) {
                    case CSV -> writeCsv(out, fields, rowReader);
                    case JSON -> writeJson(out, fields, rowReader);
                }
            }
            if (outputFile != null) {
                out.close();
            }
            else {
                out.flush();
            }
        }
        catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            return CommandResult.FAILURE;
        }
        catch (IOException e) {
            System.err.println("Error reading file: " + e.getMessage());
            return CommandResult.FAILURE;
        }

        return CommandResult.SUCCESS;
    }

    private ColumnProjection parseColumnProjection() {
        if (columns == null) {
            return ColumnProjection.all();
        }
        String[] names = columns.split(",");
        for (int i = 0; i < names.length; i++) {
            names[i] = names[i].trim();
        }
        return ColumnProjection.columns(names);
    }

    private static List<SchemaNode> projectedFields(FileSchema schema, ColumnProjection projection) {
        List<SchemaNode> allChildren = schema.getRootNode().children();
        if (projection.projectsAll()) {
            return allChildren;
        }
        return allChildren.stream()
                .filter(child -> projection.getProjectedColumnNames().stream()
                        .anyMatch(name -> name.equals(child.name()) || name.startsWith(child.name() + ".")))
                .toList();
    }

    private PrintWriter openOutput() throws IOException {
        if (outputFile != null) {
            return new PrintWriter(new FileWriter(outputFile));
        }
        return new PrintWriter(System.out, true);
    }

    // ==================== CSV ====================

    private static void writeCsv(PrintWriter out, List<SchemaNode> fields, RowReader rowReader) {
        List<String> flatHeaders = new ArrayList<>();
        for (SchemaNode field : fields) {
            flattenHeaders(field, field.name(), flatHeaders);
        }
        out.println(csvRow(flatHeaders.toArray(new String[0])));

        while (rowReader.hasNext()) {
            rowReader.next();
            List<String> flatValues = new ArrayList<>();
            for (int i = 0; i < fields.size(); i++) {
                flattenValues(rowReader.getValue(i), fields.get(i), flatValues);
            }
            out.println(csvRow(flatValues.toArray(new String[0])));
        }
    }

    private static void flattenHeaders(SchemaNode node, String prefix, List<String> headers) {
        if (node instanceof SchemaNode.GroupNode group && !group.isList() && !group.isMap() && !group.isVariant()) {
            for (SchemaNode child : group.children()) {
                flattenHeaders(child, prefix + "." + child.name(), headers);
            }
        } else {
            headers.add(prefix);
        }
    }

    private static void flattenValues(Object value, SchemaNode schema, List<String> values) {
        if (schema instanceof SchemaNode.GroupNode group && !group.isList() && !group.isMap() && !group.isVariant()) {
            if (value == null) {
                for (SchemaNode child : group.children()) {
                    flattenNulls(child, values);
                }
            } else if (value instanceof PqStruct struct) {
                for (int i = 0; i < struct.getFieldCount(); i++) {
                    String name = struct.getFieldName(i);
                    SchemaNode childSchema = findChildSchema(group, name);
                    flattenValues(struct.getValue(name), childSchema, values);
                }
            }
        } else {
            values.add(RowTable.renderValue(value, schema));
        }
    }

    private static void flattenNulls(SchemaNode schema, List<String> values) {
        if (schema instanceof SchemaNode.GroupNode group && !group.isList() && !group.isMap() && !group.isVariant()) {
            for (SchemaNode child : group.children()) {
                flattenNulls(child, values);
            }
        } else {
            values.add("null");
        }
    }

    private static SchemaNode findChildSchema(SchemaNode.GroupNode groupNode, String name) {
        for (SchemaNode child : groupNode.children()) {
            if (child.name().equals(name)) {
                return child;
            }
        }
        return null;
    }

    // ==================== JSON ====================

    private static void writeJson(PrintWriter out, List<SchemaNode> fields, RowReader rowReader) {
        String[] headers = fields.stream().map(SchemaNode::name).toArray(String[]::new);
        out.print("[");
        boolean first = true;
        while (rowReader.hasNext()) {
            rowReader.next();
            if (!first) {
                out.print(",");
            }
            first = false;
            out.print("\n  {");
            for (int i = 0; i < headers.length; i++) {
                if (i > 0)
                    out.print(",");
                SchemaNode fieldSchema = fields.get(i);
                out.print("\"" + jsonEscape(headers[i]) + "\":");
                if (fieldSchema instanceof SchemaNode.GroupNode group && group.isVariant()) {
                    PqVariant variant = rowReader.getVariant(fieldSchema.name());
                    out.print(RowTable.renderVariant(variant));
                } else {
                    String val = RowTable.renderField(rowReader, i, fieldSchema);
                    out.print("\"" + jsonEscape(val) + "\"");
                }
            }
            out.print("}");
        }
        out.println("\n]");
    }

    // ==================== Formatting Helpers ====================

    private static String csvRow(String[] values) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < values.length; i++) {
            if (i > 0)
                sb.append(',');
            sb.append(csvField(values[i]));
        }
        return sb.toString();
    }

    private static String csvField(String value) {
        if (value.indexOf(',') < 0 && value.indexOf('"') < 0 && value.indexOf('\n') < 0) {
            return value;
        }
        return "\"" + value.replace("\"", "\"\"") + "\"";
    }

    private static String jsonEscape(String value) {
        return value
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }
}
