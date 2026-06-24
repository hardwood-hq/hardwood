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

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.invocation.CommandInvocation;
import org.aesh.command.option.Mixin;

import dev.hardwood.InputFile;
import dev.hardwood.cli.internal.Sizes;
import dev.hardwood.cli.internal.table.RowTable;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ParquetFileReader;

@CommandDefinition(name = "rowgroups", description = "Display per-row-group column chunk metadata (sizes, codec).", generateHelp = true)
public class InspectRowGroupsCommand implements Command<CommandInvocation> {

    @Mixin
    FileMixin fileMixin;

    @Override
    public CommandResult execute(CommandInvocation ci) {
        InputFile inputFile = fileMixin.toInputFile();
        if (inputFile == null) {
            return CommandResult.FAILURE;
        }

        try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
            FileMetaData metadata = reader.getFileMetaData();
            List<RowGroup> rowGroups = metadata.rowGroups();

            for (int i = 0; i < rowGroups.size(); i++) {
                if (i > 0) {
                    System.out.println();
                }
                printRowGroup(i, rowGroups.get(i));
            }
        }
        catch (IOException e) {
            System.err.println("Error reading file: " + e.getMessage());
            return CommandResult.FAILURE;
        }

        return CommandResult.SUCCESS;
    }

    private void printRowGroup(int index, RowGroup rg) {
        System.out.printf("Row Group %d  (%d rows, %s uncompressed)%n",
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
        System.out.println(RowTable.renderTable(headers, rows));
    }
}
