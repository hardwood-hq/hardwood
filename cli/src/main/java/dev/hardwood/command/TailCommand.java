/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.command;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.FileSchema;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

@CommandLine.Command(name = "tail", description = "Print the last N rows as an ASCII table.")
public class TailCommand implements Callable<Integer> {

    @CommandLine.Mixin
    HelpMixin help;

    @CommandLine.Mixin
    FileMixin fileMixin;
    @Spec
    CommandSpec spec;
    @CommandLine.Option(names = "-n", defaultValue = "10", description = "Number of rows to display (default: 10).")
    int count;

    @Override
    public Integer call() {
        InputFile inputFile = fileMixin.toInputFile();
        if (inputFile == null) {
            return CommandLine.ExitCode.SOFTWARE;
        }

        try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
            FileSchema fileSchema = reader.getFileSchema();
            String[] headers = RowTable.topLevelFieldNames(fileSchema);
            List<String[]> rows = readLastRows(reader, headers.length);
            RowTable.print(spec, headers, rows);
        }
        catch (IOException e) {
            spec.commandLine().getErr().println("Error reading file: " + e.getMessage());
            return CommandLine.ExitCode.SOFTWARE;
        }

        return CommandLine.ExitCode.OK;
    }

    private List<String[]> readLastRows(ParquetFileReader reader, int fieldCount) throws IOException {
        long totalRows = reader.getFileMetaData().numRows();
        long skip = Math.max(0, totalRows - count);

        List<String[]> rows = new ArrayList<>();
        try (RowReader rowReader = reader.createRowReader()) {
            for (long i = 0; i < skip; i++) {
                rowReader.next();
            }
            RowTable.rowToTableRow(fieldCount, rows, rowReader, count);
        }
        return rows;
    }
}
