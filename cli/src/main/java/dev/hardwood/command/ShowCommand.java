/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.command;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.github.freva.asciitable.AsciiTable;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.table.StreamedTable;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

@CommandLine.Command(name = "show", description = "Print the all rows as an ASCII table.")
public class ShowCommand implements Callable<Integer> {
    @CommandLine.Mixin
    FileMixin fileMixin;
    @Spec
    CommandSpec spec;
    @CommandLine.Option(names = {"-s", "--bytes-as-string"}, defaultValue = "false", description = "Render binaries as string.")
    boolean bytesAsString;
    @CommandLine.Option(names = {"-ss", "--sample-size"}, defaultValue = "10", description = "Max number of line used to autoadjust the column width.")
    int sampleSize;
    @CommandLine.Option(names = {"-mw", "--max-width"}, defaultValue = "50", description = "Max width in characters of a column.")
    int maxWidth;
    @CommandLine.Option(names = {"-t", "--truncate"}, defaultValue = "true", description = "Should rows be truncated instead of wraping on next line when too long.")
    boolean truncate;
    @CommandLine.Option(names = {"-tp", "--transpose"}, defaultValue = "false", description = "When true, the rows are printed with two columns, the headers and values.")
    boolean transpose;
    @CommandLine.Option(names = {"-ri", "--row-index"}, defaultValue = "false", description = "When true, a virtual column is added containing the row index.")
    boolean addRowIndex;

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
                if (transpose) {
                    rowReader.toStream().forEach(r -> {
                        Stream<Object[]> data = IntStream.range(0, headers.length)
                                .mapToObj(it -> new Object[]{headers[it], RowTable.renderValue(r.getValue(it), bytesAsString)});
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
                            rowReader
                                    .toStream()
                                    .map(it -> rowIndex == null ?
                                            (IntFunction<String>) i -> RowTable.renderValue(rowReader.getValue(i), bytesAsString) :
                                            ((IntFunction<String>) i -> i == 0 ? Long.toString(rowIndex.getAndIncrement()) : RowTable.renderValue(rowReader.getValue(i - 1), bytesAsString)))
                                    .iterator(),
                            sampleSize,
                            maxWidth,
                            truncate);
                }
            }
        } catch (IOException e) {
            spec.commandLine().getErr().println("Error reading file: " + e.getMessage());
            return CommandLine.ExitCode.SOFTWARE;
        }

        return CommandLine.ExitCode.OK;
    }
}
