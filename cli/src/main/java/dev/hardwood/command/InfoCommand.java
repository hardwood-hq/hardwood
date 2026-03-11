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

import dev.hardwood.InputFile;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ParquetFileReader;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

@CommandLine.Command(name = "info", description = "Display high-level file information.")
public class InfoCommand implements Callable<Integer> {

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

            long totalCompressed = 0;
            long totalUncompressed = 0;
            for (RowGroup rg : metadata.rowGroups()) {
                for (ColumnChunk cc : rg.columns()) {
                    totalCompressed += cc.metaData().totalCompressedSize();
                    totalUncompressed += cc.metaData().totalUncompressedSize();
                }
            }

            spec.commandLine().getOut().println("Format Version:    " + metadata.version());
            spec.commandLine().getOut().println("Created By:        " + (metadata.createdBy() != null ? metadata.createdBy() : "unknown"));
            spec.commandLine().getOut().println("Row Groups:        " + metadata.rowGroups().size());
            spec.commandLine().getOut().println("Total Rows:        " + metadata.numRows());
            spec.commandLine().getOut().println("Uncompressed Size: " + Sizes.format(totalUncompressed));
            spec.commandLine().getOut().println("Compressed Size:   " + Sizes.format(totalCompressed));
        }
        catch (IOException e) {
            spec.commandLine().getErr().println("Error reading file: " + e.getMessage());
            return CommandLine.ExitCode.SOFTWARE;
        }

        return CommandLine.ExitCode.OK;
    }
}
