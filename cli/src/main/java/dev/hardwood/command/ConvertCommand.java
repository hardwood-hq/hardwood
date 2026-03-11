/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.command;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.Callable;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.FileSchema;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

@CommandLine.Command(name = "convert", description = "Convert Parquet file to CSV or JSON.")
public class ConvertCommand implements Callable<Integer> {

    enum Format {
        CSV,
        JSON
    }

    @CommandLine.Mixin
    HelpMixin help;

    @CommandLine.Mixin
    FileMixin fileMixin;

    @Spec
    CommandSpec spec;

    @CommandLine.Option(names = "--to", required = true, description = "Output format: csv, json.")
    Format format;

    @CommandLine.Option(names = "-o", description = "Output file path (default: stdout).")
    String outputFile;

    @CommandLine.Option(names = "--columns", description = "Comma-separated list of columns to include.")
    String columns;

    @Override
    public Integer call() {
        InputFile inputFile = fileMixin.toInputFile();
        if (inputFile == null) {
            return CommandLine.ExitCode.SOFTWARE;
        }

        try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
            FileSchema fileSchema = reader.getFileSchema();
            String[] allHeaders = RowTable.topLevelFieldNames(fileSchema);
            int[] columnIndices = resolveColumnIndices(allHeaders);
            String[] headers = projectHeaders(allHeaders, columnIndices);

            PrintWriter out = openOutput();
            try (RowReader rowReader = reader.createRowReader()) {
                switch (format) {
                    case CSV -> writeCsv(out, headers, columnIndices, rowReader);
                    case JSON -> writeJson(out, headers, columnIndices, rowReader);
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
            spec.commandLine().getErr().println(e.getMessage());
            return CommandLine.ExitCode.SOFTWARE;
        }
        catch (IOException e) {
            spec.commandLine().getErr().println("Error reading file: " + e.getMessage());
            return CommandLine.ExitCode.SOFTWARE;
        }

        return CommandLine.ExitCode.OK;
    }

    private PrintWriter openOutput() throws IOException {
        if (outputFile != null) {
            return new PrintWriter(new FileWriter(outputFile));
        }
        return spec.commandLine().getOut();
    }

    private int[] resolveColumnIndices(String[] allHeaders) {
        if (columns == null) {
            int[] indices = new int[allHeaders.length];
            for (int i = 0; i < allHeaders.length; i++) {
                indices[i] = i;
            }
            return indices;
        }
        String[] requested = columns.split(",");
        int[] indices = new int[requested.length];
        for (int i = 0; i < requested.length; i++) {
            String col = requested[i].trim();
            indices[i] = indexOfHeader(allHeaders, col);
            if (indices[i] < 0) {
                throw new IllegalArgumentException("Unknown column: " + col);
            }
        }
        return indices;
    }

    private static int indexOfHeader(String[] headers, String name) {
        for (int i = 0; i < headers.length; i++) {
            if (headers[i].equals(name))
                return i;
        }
        return -1;
    }

    private static String[] projectHeaders(String[] all, int[] indices) {
        String[] result = new String[indices.length];
        for (int i = 0; i < indices.length; i++) {
            result[i] = all[indices[i]];
        }
        return result;
    }

    private static void writeCsv(PrintWriter out, String[] headers, int[] columnIndices, RowReader rowReader) {
        out.println(csvRow(headers));
        while (rowReader.hasNext()) {
            rowReader.next();
            String[] values = new String[columnIndices.length];
            for (int i = 0; i < columnIndices.length; i++) {
                values[i] = RowTable.renderValue(rowReader.getValue(columnIndices[i]));
            }
            out.println(csvRow(values));
        }
    }

    private static void writeJson(PrintWriter out, String[] headers, int[] columnIndices, RowReader rowReader) {
        out.print("[");
        boolean first = true;
        while (rowReader.hasNext()) {
            rowReader.next();
            if (!first) {
                out.print(",");
            }
            first = false;
            out.print("\n  {");
            for (int i = 0; i < columnIndices.length; i++) {
                if (i > 0)
                    out.print(",");
                String val = RowTable.renderValue(rowReader.getValue(columnIndices[i]));
                out.print("\"" + jsonEscape(headers[i]) + "\":\"" + jsonEscape(val) + "\"");
            }
            out.print("}");
        }
        out.println("\n]");
    }

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
