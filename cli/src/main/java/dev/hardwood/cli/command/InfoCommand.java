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
import org.aesh.command.option.Mixin;

import dev.hardwood.InputFile;
import dev.hardwood.cli.internal.Sizes;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ParquetFileReader;

@CommandDefinition(name = "info", description = "Display high-level file information.", generateHelp = true)
public class InfoCommand implements Command<CommandInvocation> {

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

            long totalCompressed = 0;
            long totalUncompressed = 0;
            for (RowGroup rg : metadata.rowGroups()) {
                for (ColumnChunk cc : rg.columns()) {
                    totalCompressed += cc.metaData().totalCompressedSize();
                    totalUncompressed += cc.metaData().totalUncompressedSize();
                }
            }

            System.out.println("Format Version:    " + metadata.version());
            System.out.println("Created By:        " + (metadata.createdBy() != null ? metadata.createdBy() : "unknown"));
            System.out.println("Row Groups:        " + metadata.rowGroups().size());
            System.out.println("Total Rows:        " + metadata.numRows());
            System.out.println("Uncompressed Size: " + Sizes.format(totalUncompressed));
            System.out.println("Compressed Size:   " + Sizes.format(totalCompressed));
        }
        catch (IOException e) {
            System.err.println("Error reading file: " + e.getMessage());
            return CommandResult.FAILURE;
        }

        return CommandResult.SUCCESS;
    }
}
