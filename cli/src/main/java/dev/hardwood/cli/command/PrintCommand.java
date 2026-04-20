/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.github.freva.asciitable.AsciiTable;

import dev.hardwood.InputFile;
import dev.hardwood.cli.internal.table.RowTable;
import dev.hardwood.cli.internal.table.StreamedTable;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

@CommandLine.Command(name = "print", description = "Print all rows as an ASCII table.")
public class PrintCommand implements Callable<Integer> {

    private static final String ALL = "ALL";

    @CommandLine.Mixin
    HelpMixin help;

    @CommandLine.Mixin
    FileMixin fileMixin;

    @Spec
    CommandSpec spec;

    @CommandLine.Option(names = {"-ss", "--sample-size"}, defaultValue = "10", description = "Max number of lines used to auto-adjust the column width.")
    int sampleSize;

    @CommandLine.Option(names = {"-mw", "--max-width"}, defaultValue = "50", description = "Max width in characters of a column.")
    int maxWidth;

    @CommandLine.Option(names = {"-t", "--truncate"}, negatable = true, fallbackValue = "true", defaultValue = "true", description = "Should rows be truncated instead of wrapping on next line when too long.")
    boolean truncate;

    @CommandLine.Option(names = {"-tp", "--transpose"}, defaultValue = "false", description = "When true, the rows are printed with two columns, the headers and values.")
    boolean transpose;

    @CommandLine.Option(names = {"-ri", "--row-index"}, defaultValue = "false", description = "When true, a virtual column is added containing the row index.")
    boolean addRowIndex;

    @CommandLine.Option(names = {"-rd", "--row-delimiter"}, description = "Should a line separate rows, it is lighter without but less readable when it overlaps a single terminal line.")
    boolean rowDelimiter;

    @CommandLine.Option(names = {"-n", "--rows"}, defaultValue = ALL, description = "Number of rows to display. Positive values show the first N rows (head), negative values show the last N rows (tail), 'ALL' shows every row.")
    String n;

    @CommandLine.Option(names = {"-c", "--columns"}, description = "Comma-separated list of columns to include. Supports nested fields via dot notation (e.g. 'account.id').")
    String columns;

    @Override
    public Integer call() {
        InputFile inputFile = fileMixin.toInputFile();
        if (inputFile == null) {
            return CommandLine.ExitCode.SOFTWARE;
        }

        int rowLimit = parseRowLimit();
        ColumnProjection projection = parseColumnProjection();

        try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
            FileSchema fileSchema = reader.getFileSchema();
            try (RowReader rowReader = rowLimit != 0
                    ? reader.createRowReader(projection, null, rowLimit)
                    : reader.createRowReader(projection)) {
                String[] headers = RowTable.topLevelFieldNames(fileSchema, projection);
                List<SchemaNode> fields = projectedFields(fileSchema, projection);
                AtomicLong rowIndex = addRowIndex ? new AtomicLong() : null;
                Stream<Object[]> stream = stream(rowReader).map(r -> toData(r, headers.length));
                if (transpose) {
                    printTransposed(stream, headers, fields, rowIndex);
                } else {
                    printTable(stream, headers, fields, rowIndex);
                }
            }
        }
        catch (IOException e) {
            spec.commandLine().getErr().println("Error reading file: " + e.getMessage());
            return CommandLine.ExitCode.SOFTWARE;
        }

        return CommandLine.ExitCode.OK;
    }

    private void printTransposed(Stream<Object[]> stream, String[] headers, List<SchemaNode> fields, AtomicLong rowIndex) {
        stream.forEach(r -> {
            Stream<Object[]> data = IntStream.range(0, headers.length)
                    .mapToObj(i -> new Object[]{headers[i], RowTable.renderValue(r[i], fields.get(i))});
            spec.commandLine().getOut().println(
                    AsciiTable.builder()
                            .data((rowIndex != null ?
                                    Stream.concat(
                                            Stream.of(new Object[][]{new Object[]{"rowIndex", Long.toString(rowIndex.getAndIncrement())}}), data) : data)
                                    .toArray(Object[][]::new))
                            .asString());
        });
    }

    private void printTable(Stream<Object[]> stream, String[] headers, List<SchemaNode> fields, AtomicLong rowIndex) {
        new StreamedTable().print(
                spec.commandLine().getOut(),
                addRowIndex ? Stream.concat(Stream.of("rowIndex"), Stream.of(headers)).toArray(String[]::new) : headers,
                stream
                        .map(r -> rowIndex == null ?
                                (IntFunction<String>) i -> RowTable.renderValue(r[i], fields.get(i)) :
                                ((IntFunction<String>) i -> i == 0 ? Long.toString(rowIndex.getAndIncrement()) : RowTable.renderValue(r[i - 1], fields.get(i - 1))))
                        .iterator(),
                sampleSize,
                maxWidth,
                truncate,
                rowDelimiter);
    }

    private int parseRowLimit() {
        if (ALL.equalsIgnoreCase(n)) {
            return 0;
        }
        try {
            return Integer.parseInt(n);
        }
        catch (NumberFormatException e) {
            throw new CommandLine.ParameterException(spec.commandLine(),
                    "Invalid value for option '-n': expected an integer or 'ALL', got '" + n + "'");
        }
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
        // ColumnProjection.columns("a.b") projects "a" at top level — so we filter root children
        // by checking which top-level fields have any projected column underneath them.
        // For simplicity, we match top-level names against the projection prefixes.
        return allChildren.stream()
                .filter(child -> projection.getProjectedColumnNames().stream()
                        .anyMatch(name -> name.equals(child.name()) || name.startsWith(child.name() + ".")))
                .toList();
    }

    private Object[] toData(RowReader rowReader, int fieldCount) {
        return IntStream.range(0, fieldCount)
                .mapToObj(rowReader::getValue)
                .toArray(Object[]::new);
    }

    private Stream<RowReader> stream(RowReader rowReader) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new Iterator<>() {
            @Override
            public boolean hasNext() {
                return rowReader.hasNext();
            }

            @Override
            public RowReader next() {
                rowReader.next();
                return rowReader;
            }
        }, Spliterator.IMMUTABLE), false);
    }
}
