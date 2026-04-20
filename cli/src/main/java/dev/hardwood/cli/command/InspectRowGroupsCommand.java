/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import dev.hardwood.InputFile;
import dev.hardwood.cli.internal.Sizes;
import dev.hardwood.cli.internal.table.RowTable;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ParquetFileReader;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

@CommandLine.Command(name = "rowgroups", description = "Display per-row-group column chunk metadata (sizes, codec).")
public class InspectRowGroupsCommand implements Callable<Integer> {

    @CommandLine.Mixin
    HelpMixin help;

    @CommandLine.Mixin
    FileMixin fileMixin;
    @Spec
    CommandSpec spec;

    @Override
    public Integer call() {
        InputFile inputFile = fileMixin.toInputFile();
        if (inputFile == null) {
            return CommandLine.ExitCode.SOFTWARE;
        }

        try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
            FileMetaData metadata = reader.getFileMetaData();
            List<RowGroup> rowGroups = metadata.rowGroups();

            for (int i = 0; i < rowGroups.size(); i++) {
                if (i > 0) {
                    spec.commandLine().getOut().println();
                }
                printRowGroup(i, rowGroups.get(i));
            }
        }
        catch (IOException e) {
            spec.commandLine().getErr().println("Error reading file: " + e.getMessage());
            return CommandLine.ExitCode.SOFTWARE;
        }

        return CommandLine.ExitCode.OK;
    }

    private void printRowGroup(int index, RowGroup rg) {
        spec.commandLine().getOut().printf("Row Group %d  (%d rows, %s uncompressed)%n",
                index, rg.numRows(), Sizes.format(rg.totalByteSize()));

        String[] headers = {"Column", "Type", "Codec", "Compressed", "Uncompressed"};
        List<String[]> rows = new ArrayList<>();
        for (ColumnChunk cc : rg.columns()) {
            ColumnMetaData cmd = cc.metaData();
            rows.add(new String[]{
                    Sizes.columnPath(cmd),
                    cmd.type().toString(),
                    cmd.codec().toString(),
                    Sizes.format(cmd.totalCompressedSize()),
                    Sizes.format(cmd.totalUncompressedSize())
            });
        }
        spec.commandLine().getOut().println(RowTable.renderTable(headers, rows));
    }
}
