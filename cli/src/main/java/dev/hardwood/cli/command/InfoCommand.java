/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import java.io.IOException;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.invocation.CommandInvocation;

import dev.hardwood.InputFile;
import dev.hardwood.cli.internal.Sizes;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ParquetFileReader;

@CommandDefinition(name = "info", description = "Display high-level file information.")
public class InfoCommand extends FileCommandBase implements Command<CommandInvocation> {

    @Override
    public CommandResult execute(CommandInvocation invocation) {
        InputFile inputFile = toInputFile(invocation);
        if (inputFile == null) {
            return CommandResult.FAILURE;
        }

        try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
            FileMetaData metadata = reader.getFileMetaData();

            long totalCompressed = 0;
            long totalUncompressed = 0;
            for (RowGroup rg : metadata.rowGroups()) {
                for (ColumnChunk cc : rg.columns()) {
                    totalCompressed += cc.metaData().totalCompressedSize();
                    totalUncompressed += cc.metaData().totalUncompressedSize();
                }
            }

            invocation.println("Format Version:    " + metadata.version());
            invocation.println("Created By:        " + (metadata.createdBy() != null ? metadata.createdBy() : "unknown"));
            invocation.println("Row Groups:        " + metadata.rowGroups().size());
            invocation.println("Total Rows:        " + metadata.numRows());
            invocation.println("Uncompressed Size: " + Sizes.format(totalUncompressed));
            invocation.println("Compressed Size:   " + Sizes.format(totalCompressed));
        }
        catch (IOException e) {
            System.err.println("Error reading file: " + e.getMessage());
            return CommandResult.FAILURE;
        }

        return CommandResult.SUCCESS;
    }
}
