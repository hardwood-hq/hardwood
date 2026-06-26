/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.invocation.CommandInvocation;
import org.aesh.command.option.Option;

import com.github.freva.asciitable.AsciiTable;

import dev.hardwood.InputFile;
import dev.hardwood.cli.internal.table.RowTable;
import dev.hardwood.cli.internal.table.StreamedTable;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;

@CommandDefinition(name = "print", description = "Print all rows as an ASCII table.")
public class PrintCommand extends FileCommandBase implements Command<CommandInvocation> {

    @Option(name = "sample-size", shortName = 's', defaultValue = "10", description = "Max number of lines used to auto-adjust the column width.")
    int sampleSize;

    @Option(name = "max-width", shortName = 'w', defaultValue = "50", description = "Max width in characters of a column.")
    int maxWidth;

    @Option(name = "truncate", shortName = 't', defaultValue = "true", description = "Should rows be truncated instead of wrapping on next line when too long.")
    boolean truncate;

    @Option(name = "no-truncate", hasValue = false, description = "Should rows not be truncated.")
    boolean noTruncate;

    @Option(name = "transpose", defaultValue = "false", hasValue = false, description = "When true, the rows are printed with two columns, the headers and values.")
    boolean transpose;

    @Option(name = "row-index", shortName = 'i', defaultValue = "false", hasValue = false, description = "When true, a virtual column is added containing the row index.")
    boolean addRowIndex;

    @Option(name = "row-delimiter", shortName = 'd', defaultValue = "false", hasValue = false, description = "Should a line separate rows, it is lighter without but less readable when it overlaps a single terminal line.")
    boolean rowDelimiter;

    @Option(name = "rows", shortName = 'n', defaultValue = RowLimits.ALL, description = "Number of rows to display. Positive values show the first N rows (head), negative values show the last N rows (tail), 'ALL' shows every row.")
    String n;

    @Option(name = "columns", shortName = 'c', description = "Comma-separated list of columns to include. Supports nested fields via dot notation (e.g. 'account.id').")
    String columns;

    @Override
    public CommandResult execute(CommandInvocation invocation) {
        InputFile inputFile = toInputFile(invocation);
        if (inputFile == null) {
            return CommandResult.FAILURE;
        }

        int rowLimit;
        try {
            rowLimit = RowLimits.parse(n);
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            return CommandResult.FAILURE;
        }
        ColumnProjection projection = parseColumnProjection();

        PrintWriter printWriter = new PrintWriter(new Writer() {
            @Override
            public void write(char[] cbuf, int off, int len) {
                invocation.print(new String(cbuf, off, len));
            }
            @Override
            public void flush() {}
            @Override
            public void close() {}
        }, true);

        try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
            FileSchema fileSchema = reader.getFileSchema();
            try (RowReader rowReader = RowLimits.buildRowReader(reader, projection, rowLimit)) {
                String[] headers = RowTable.topLevelFieldNames(fileSchema, projection);
                List<SchemaNode> fields = projectedFields(fileSchema, projection);
                AtomicLong rowIndex = addRowIndex ? new AtomicLong() : null;
                Stream<Object[]> stream = stream(rowReader).map(r -> toData(r, headers.length));
                if (transpose) {
                    printTransposed(printWriter, stream, headers, fields, rowIndex);
                } else {
                    printTable(printWriter, stream, headers, fields, rowIndex);
                }
            }
        }
        catch (IOException e) {
            System.err.println("Error reading file: " + e.getMessage());
            return CommandResult.FAILURE;
        }

        return CommandResult.SUCCESS;
    }

    private void printTransposed(PrintWriter printWriter, Stream<Object[]> stream, String[] headers, List<SchemaNode> fields, AtomicLong rowIndex) {
        stream.forEach(r -> {
            Stream<Object[]> data = IntStream.range(0, headers.length)
                    .mapToObj(i -> new Object[]{headers[i], RowTable.renderValue(r[i], fields.get(i))});
            printWriter.println(
                    AsciiTable.builder()
                            .data((rowIndex != null ?
                                    Stream.concat(
                                            Stream.of(new Object[][]{new Object[]{"rowIndex", Long.toString(rowIndex.getAndIncrement())}}), data) : data)
                                    .toArray(Object[][]::new))
                            .asString());
        });
    }

    private void printTable(PrintWriter printWriter, Stream<Object[]> stream, String[] headers, List<SchemaNode> fields, AtomicLong rowIndex) {
        new StreamedTable().print(
                printWriter,
                addRowIndex ? Stream.concat(Stream.of("rowIndex"), Stream.of(headers)).toArray(String[]::new) : headers,
                stream
                        .map(r -> rowIndex == null ?
                                (IntFunction<String>) i -> RowTable.renderValue(r[i], fields.get(i)) :
                                ((IntFunction<String>) i -> i == 0 ? Long.toString(rowIndex.getAndIncrement()) : RowTable.renderValue(r[i - 1], fields.get(i - 1))))
                        .iterator(),
                sampleSize,
                maxWidth,
                truncate && !noTruncate,
                rowDelimiter);
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
