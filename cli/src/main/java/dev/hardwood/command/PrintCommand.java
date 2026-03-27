/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.command;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Iterator;
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
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.table.StreamedTable;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

@CommandLine.Command(name = "print", description = "Print the all rows as an ASCII table.")
public class PrintCommand implements Callable<Integer> {
    @CommandLine.Mixin
    FileMixin fileMixin;
    @Spec
    CommandSpec spec;
    @CommandLine.Option(names = {"-ss", "--sample-size"}, defaultValue = "10", description = "Max number of line used to autoadjust the column width.")
    int sampleSize;
    @CommandLine.Option(names = {"-mw", "--max-width"}, defaultValue = "50", description = "Max width in characters of a column.")
    int maxWidth;
    @CommandLine.Option(names = {"-t", "--truncate"}, negatable = true, fallbackValue = "true", defaultValue = "true", description = "Should rows be truncated instead of wraping on next line when too long.")
    boolean truncate;
    @CommandLine.Option(names = {"-tp", "--transpose"}, defaultValue = "false", description = "When true, the rows are printed with two columns, the headers and values.")
    boolean transpose;
    @CommandLine.Option(names = {"-ri", "--row-index"}, defaultValue = "false", description = "When true, a virtual column is added containing the row index.")
    boolean addRowIndex;
    @CommandLine.Option(names = {"-rd", "--row-delimiter"}, description = "Should a line separate rows, it is lighter without but less readable when it overlaps a single terminal line.")
    boolean rowDelimiter;
    @CommandLine.Option(names = "-n", defaultValue = "-2147483648", description = "If >0 the number of head lines to show and if <0 and not Integer.MIN_VALUE the tail number else it shows the full file. Note that tail (negative values) is loading the rows in memory.")
    int n;

    @Override
    public Integer call() {
        InputFile inputFile = fileMixin.toInputFile();
        if (inputFile == null) {
            return CommandLine.ExitCode.SOFTWARE;
        }

        try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
            FileSchema fileSchema = reader.getFileSchema();
            String[] headers = RowTable.topLevelFieldNames(fileSchema);
            AtomicLong rowIndex = addRowIndex ? new AtomicLong() : null;
            try (RowReader rowReader = reader.createRowReader()) {
                Stream<Object[]> stream = prepareSampling(rowReader, headers);
                if (transpose) {
                    stream.forEach(r -> {
                        Stream<Object[]> data = IntStream.range(0, headers.length)
                                .mapToObj(it -> new Object[]{headers[it], RowTable.renderValue(r[it], fileSchema.getRootNode().children().get(it))});
                        spec.commandLine().getOut().println(
                                AsciiTable.builder()
                                        .data((rowIndex != null ?
                                                Stream.concat(
                                                        Stream.of(new Object[][]{new Object[]{"rowIndex", Long.toString(rowIndex.getAndIncrement())}}), data) : data)
                                                .toArray(Object[][]::new))
                                        .asString());
                    });
                } else {
                    new StreamedTable().print(
                            spec.commandLine().getOut(),
                            addRowIndex ? Stream.concat(Stream.of("rowIndex"), Stream.of(headers)).toArray(String[]::new) : headers,
                            stream
                                    .map(it -> rowIndex == null ?
                                            (IntFunction<String>) i -> RowTable.renderValue(it[i], fileSchema.getRootNode().children().get(i)) :
                                            ((IntFunction<String>) i -> i == 0 ? Long.toString(rowIndex.getAndIncrement()) : RowTable.renderValue(it[i - 1], fileSchema.getRootNode().children().get(i - 1))))
                                    .iterator(),
                            sampleSize,
                            maxWidth,
                            truncate,
                            rowDelimiter);
                }
            }
        } catch (IOException e) {
            spec.commandLine().getErr().println("Error reading file: " + e.getMessage());
            return CommandLine.ExitCode.SOFTWARE;
        }

        return CommandLine.ExitCode.OK;
    }

    private Stream<Object[]> prepareSampling(RowReader rowReader, String[] headers) {
        if (n == 0) {
            return Stream.empty();
        }
        if (n > 0) {
            return stream(rowReader).limit(n).map(it -> toData(rowReader, headers));
        }
        if (n != Integer.MIN_VALUE) {
            int tailSize = -n;
            ArrayDeque<Object[]> buffer = new ArrayDeque<>(tailSize);
            stream(rowReader).forEach(row -> {
                if (buffer.size() == tailSize) {
                    buffer.removeFirst();
                }
                buffer.addLast(toData(rowReader, headers));
            });
            return buffer.stream();
        }
        return stream(rowReader).map(it -> toData(rowReader, headers));
    }

    private Object[] toData(RowReader rowReader, String[] headers) {
        return IntStream.range(0, headers.length)
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
