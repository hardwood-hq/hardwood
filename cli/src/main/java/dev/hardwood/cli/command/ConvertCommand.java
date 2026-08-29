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
import dev.hardwood.cli.internal.BinaryValues;
import dev.hardwood.cli.internal.JsonStrings;
import dev.hardwood.cli.internal.ValueFormatter;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetReadException;
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

    @Option(name = "null-string", description = "CSV field to write for a null value (default: an empty field). CSV output only.")
    String nullString;

    @Override
    public CommandResult execute(CommandInvocation ci) {
        InputFile inputFile = fileMixin.toInputFile();
        if (inputFile == null) {
            return CommandResult.FAILURE;
        }
        if (format == Format.JSON && nullString != null) {
            System.err.println("--null-string applies to CSV output only; JSON output writes null.");
            return CommandResult.FAILURE;
        }

        try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
            int rowLimit = RowLimits.parse(n);
            ColumnProjection projection = parseColumnProjection();
            FileSchema fileSchema = reader.getFileSchema();
            List<SchemaNode> fields = projectedFields(fileSchema, projection);

            PrintWriter out = openOutput();
            String effectiveNullString = nullString == null ? "" : nullString;
            try (RowReader rowReader = RowLimits.buildRowReader(reader, projection, rowLimit)) {
                switch (format) {
                    case CSV -> writeCsv(out, fields, rowReader, projection, effectiveNullString);
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
        catch (IOException | ParquetReadException e) {
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

    private static void writeCsv(PrintWriter out, List<SchemaNode> fields, RowReader rowReader,
                                 ColumnProjection projection, String nullString) throws IOException {
        List<String> flatHeaders = new ArrayList<>();
        for (SchemaNode field : fields) {
            flattenHeaders(field, field.name(), projection, flatHeaders);
        }
        out.println(csvRow(flatHeaders.toArray(new String[0])));

        while (rowReader.hasNext()) {
            rowReader.next();
            List<String> flatValues = new ArrayList<>();
            for (int i = 0; i < fields.size(); i++) {
                SchemaNode field = fields.get(i);
                flattenValues(rowReader.getValue(i), field, field.name(), projection, flatValues, nullString);
            }
            out.println(csvRow(flatValues.toArray(new String[0])));
        }
    }

    private static void flattenHeaders(SchemaNode node, String path, ColumnProjection projection,
                                       List<String> headers) {
        if (node instanceof SchemaNode.GroupNode group && isStruct(group)) {
            for (SchemaNode child : group.children()) {
                String childPath = path + "." + child.name();
                if (isProjected(projection, childPath)) {
                    flattenHeaders(child, childPath, projection, headers);
                }
            }
        }
        else {
            headers.add(path);
        }
    }

    // package visibility for tests
    static void flattenValues(Object value, SchemaNode schema, String path, ColumnProjection projection,
                              List<String> values, String nullString) {
        if (schema instanceof SchemaNode.GroupNode group && isStruct(group)) {
            if (value == null) {
                flattenNulls(group, path, projection, values, nullString);
            }
            else if (value instanceof PqStruct struct) {
                for (SchemaNode child : group.children()) {
                    String childPath = path + "." + child.name();
                    if (isProjected(projection, childPath)) {
                        flattenValues(struct.getValue(child.name()), child, childPath, projection, values, nullString);
                    }
                }
            }
            else {
                throw new IllegalStateException("Field '" + group.name() + "' is a struct in the schema, but the"
                        + " reader returned a " + value.getClass().getName());
            }
        }
        else if (value == null) {
            values.add(nullString);
        }
        else {
            values.add(ValueFormatter.formatValue(value, schema, BinaryValues.NO_LIMIT));
        }
    }

    private static void flattenNulls(SchemaNode schema, String path, ColumnProjection projection,
                                     List<String> values, String nullString) {
        if (schema instanceof SchemaNode.GroupNode group && isStruct(group)) {
            for (SchemaNode child : group.children()) {
                String childPath = path + "." + child.name();
                if (isProjected(projection, childPath)) {
                    flattenNulls(child, childPath, projection, values, nullString);
                }
            }
        }
        else {
            values.add(nullString);
        }
    }

    /// A group is flattened into one CSV column per leaf only when it is a plain
    /// struct; lists, maps and Variants render into a single cell.
    private static boolean isStruct(SchemaNode.GroupNode group) {
        return !group.isList() && !group.isMap() && !group.isVariant();
    }

    /// A dotted field path is exported when the projection selects it, an ancestor
    /// of it, or a descendant of it.
    private static boolean isProjected(ColumnProjection projection, String path) {
        if (projection.projectsAll()) {
            return true;
        }
        for (String name : projection.getProjectedColumnNames()) {
            if (name.equals(path) || name.startsWith(path + ".") || path.startsWith(name + ".")) {
                return true;
            }
        }
        return false;
    }

    // ==================== JSON ====================

    private static void writeJson(PrintWriter out, List<SchemaNode> fields, RowReader rowReader) throws IOException {
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
                out.print("\"" + JsonStrings.escape(headers[i]) + "\":");
                if (rowReader.isNull(i)) {
                    out.print("null");
                } else if (fieldSchema instanceof SchemaNode.GroupNode group && group.isVariant()) {
                    PqVariant variant = rowReader.getVariant(fieldSchema.name());
                    out.print(ValueFormatter.variantJson(variant));
                } else if (!writeIfJsonScalar(out, rowReader, i, fieldSchema)) {
                    String val = ValueFormatter.formatReader(rowReader, i, fieldSchema, true,
                            ValueFormatter.NestedStyle.COMPACT, BinaryValues.NO_LIMIT);
                    out.print("\"" + JsonStrings.escape(val) + "\"");
                }
            }
            out.print("}");
        }
        out.println("\n]");
    }

    private static boolean writeIfJsonScalar(PrintWriter out, RowReader rowReader, int fieldIndex,
                                             SchemaNode fieldSchema) {
        if (fieldSchema instanceof SchemaNode.PrimitiveNode primitive
                && primitive.repetitionType() != RepetitionType.REPEATED) {
            LogicalType logicalType = primitive.logicalType();
            return (logicalType == null || logicalType instanceof LogicalType.IntType) &&
                    switch (primitive.type()) {
                        case BOOLEAN -> {
                            out.print(rowReader.getBoolean(fieldIndex));
                            yield true;
                        }
                        case INT32 -> writeJsonInt32(out, logicalType, rowReader.getInt(fieldIndex));
                        case INT64 -> writeJsonInt64(out, logicalType, rowReader.getLong(fieldIndex));
                        case FLOAT -> writeJsonFloat(out, rowReader.getFloat(fieldIndex));
                        case DOUBLE -> writeJsonDouble(out, rowReader.getDouble(fieldIndex));
                        case INT96, BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> false;
                    };
        }

        return false;
    }

    private static boolean writeJsonInt32(PrintWriter out, LogicalType logicalType, int n) {
        if (logicalType instanceof LogicalType.IntType intType && !intType.isSigned()) {
            out.print(Integer.toUnsignedLong(n));
        } else {
            out.print(n);
        }
        return true;
    }

    private static boolean writeJsonInt64(PrintWriter out, LogicalType logicalType, long n) {
        if (logicalType instanceof LogicalType.IntType intType && !intType.isSigned()) {
            out.print(Long.toUnsignedString(n));
        } else {
            out.print(n);
        }
        return true;
    }

    private static boolean writeJsonFloat(PrintWriter out, float f) {
        if (Float.isFinite(f)) {
            out.print(f);
        } else {
            writeJsonString(out, Float.toString(f));
        }
        return true;
    }

    private static boolean writeJsonDouble(PrintWriter out, double d) {
        if (Double.isFinite(d)) {
            out.print(d);
        } else {
            writeJsonString(out, Double.toString(d));
        }
        return true;
    }

    private static void writeJsonString(PrintWriter out, String value) {
        out.print("\"" + JsonStrings.escape(value) + "\"");
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

}
