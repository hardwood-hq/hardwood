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

import dev.hardwood.InputFile;
import dev.hardwood.cli.internal.Sizes;
import dev.hardwood.cli.internal.table.RowTable;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ParquetFileReader;

@CommandDefinition(name = "rowgroups", description = "Display per-row-group column chunk metadata (sizes, codec).")
public class InspectRowGroupsCommand extends FileCommandBase implements Command<CommandInvocation> {

    @Override
    public CommandResult execute(CommandInvocation invocation) {
        InputFile inputFile = toInputFile(invocation);
        if (inputFile == null) {
            return CommandResult.FAILURE;
        }

        try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
            FileMetaData metadata = reader.getFileMetaData();
            List<RowGroup> rowGroups = metadata.rowGroups();

            for (int i = 0; i < rowGroups.size(); i++) {
                if (i > 0) {
                    invocation.println("");
                }
                printRowGroup(invocation, i, rowGroups.get(i));
            }
        }
        catch (IOException e) {
            System.err.println("Error reading file: " + e.getMessage());
            return CommandResult.FAILURE;
        }

        return CommandResult.SUCCESS;
    }

    private void printRowGroup(CommandInvocation invocation, int index, RowGroup rg) {
        invocation.println(String.format("Row Group %d  (%d rows, %s uncompressed)",
                index, rg.numRows(), Sizes.format(rg.totalByteSize())));

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
        invocation.println(RowTable.renderTable(headers, rows));
    }
}
