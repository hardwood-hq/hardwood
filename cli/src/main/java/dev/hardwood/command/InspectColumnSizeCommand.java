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
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

@CommandLine.Command(name = "column-size", description = "Show compressed and uncompressed byte sizes per column, ranked.")
public class InspectColumnSizeCommand implements Callable<Integer> {

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
            List<ColumnSize> sizes = aggregateSizes(metadata);
            sizes.sort(Comparator.comparingLong(ColumnSize::compressed).reversed());
            printRanked(sizes);
        }
        catch (IOException e) {
            spec.commandLine().getErr().println("Error reading file: " + e.getMessage());
            return CommandLine.ExitCode.SOFTWARE;
        }

        return CommandLine.ExitCode.OK;
    }

    private static List<ColumnSize> aggregateSizes(FileMetaData metadata) {
        Map<String, ColumnSize> byColumn = new LinkedHashMap<>();

        for (RowGroup rg : metadata.rowGroups()) {
            for (ColumnChunk cc : rg.columns()) {
                ColumnMetaData cmd = cc.metaData();
                String path = Sizes.columnPath(cmd);
                ColumnSize existing = byColumn.get(path);
                if (existing == null) {
                    byColumn.put(path, new ColumnSize(path, cmd.type().name(), cmd.codec().name(),
                            cmd.totalCompressedSize(), cmd.totalUncompressedSize()));
                }
                else {
                    byColumn.put(path, new ColumnSize(path, existing.type(), existing.codec(),
                            existing.compressed() + cmd.totalCompressedSize(),
                            existing.uncompressed() + cmd.totalUncompressedSize()));
                }
            }
        }

        return new ArrayList<>(byColumn.values());
    }

    private void printRanked(List<ColumnSize> sizes) {
        spec.commandLine().getOut().printf("%-4s  %-30s  %-12s  %-14s  %-14s  %-6s%n",
                "Rank", "Column", "Type", "Compressed", "Uncompressed", "Ratio");
        spec.commandLine().getOut().println("-".repeat(88));

        for (int i = 0; i < sizes.size(); i++) {
            ColumnSize s = sizes.get(i);
            double ratio = s.uncompressed() > 0 ? (100.0 * s.compressed() / s.uncompressed()) : 100.0;
            spec.commandLine().getOut().printf("%-4d  %-30s  %-12s  %-14s  %-14s  %.1f%%%n",
                    i + 1,
                    s.path(),
                    s.type(),
                    Sizes.format(s.compressed()),
                    Sizes.format(s.uncompressed()),
                    ratio);
        }
    }

    private record ColumnSize(String path, String type, String codec, long compressed, long uncompressed) {
    }
}
