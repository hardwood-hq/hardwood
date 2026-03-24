/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.command;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;

import dev.hardwood.InputFile;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ParquetFileReader;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

@CommandLine.Command(name = "metadata", description = "Display full file metadata including row groups and column chunks.")
public class MetadataCommand implements Callable<Integer> {

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

            spec.commandLine().getOut().println("Format Version: " + metadata.version());
            spec.commandLine().getOut().println("Created By:     " + (metadata.createdBy() != null ? metadata.createdBy() : "unknown"));
            spec.commandLine().getOut().println("Row Groups:     " + rowGroups.size());
            spec.commandLine().getOut().println("Total Rows:     " + metadata.numRows());
            spec.commandLine().getOut().println();

            for (int i = 0; i < rowGroups.size(); i++) {
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

        for (ColumnChunk cc : rg.columns()) {
            printColumnChunk(cc.metaData());
        }
        spec.commandLine().getOut().println();
    }

    private void printColumnChunk(ColumnMetaData cmd) {
        String path = Sizes.columnPath(cmd);
        spec.commandLine().getOut().printf("  %-30s  type: %-25s  codec: %-14s  compressed: %-10s  uncompressed: %s%n",
                path,
                cmd.type(),
                cmd.codec(),
                Sizes.format(cmd.totalCompressedSize()),
                Sizes.format(cmd.totalUncompressedSize()));
    }
}
