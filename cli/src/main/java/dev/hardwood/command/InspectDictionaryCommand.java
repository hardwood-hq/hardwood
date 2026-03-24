/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.command;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.Callable;

import dev.hardwood.InputFile;
import dev.hardwood.internal.reader.Dictionary;
import dev.hardwood.internal.reader.HardwoodContextImpl;
import dev.hardwood.internal.reader.PageInfo;
import dev.hardwood.internal.reader.PageScanner;
import dev.hardwood.internal.reader.RowGroupIndexBuffers;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

@CommandLine.Command(name = "dictionary", description = "Print dictionary entries for a column.")
public class InspectDictionaryCommand implements Callable<Integer> {

    @CommandLine.Mixin
    HelpMixin help;

    @CommandLine.Mixin
    FileMixin fileMixin;
    @Spec
     CommandSpec spec;
    @CommandLine.Option(names = "--column", required = true, paramLabel = "COLUMN", description = "Column name to inspect.")
    String column;

    @Override
    public Integer call() {
        if (fileMixin.toInputFile() == null) {
            return CommandLine.ExitCode.SOFTWARE;
        }

        FileMetaData metadata;
        FileSchema schema;
        try (ParquetFileReader reader = ParquetFileReader.open(fileMixin.toInputFile())) {
            metadata = reader.getFileMetaData();
            schema = reader.getFileSchema();
        }
        catch (IOException e) {
            spec.commandLine().getErr().println("Error reading file: " + e.getMessage());
            return CommandLine.ExitCode.SOFTWARE;
        }

        ColumnSchema columnSchema;
        try {
            columnSchema = schema.getColumn(column);
        }
        catch (IllegalArgumentException e) {
            spec.commandLine().getErr().println("Unknown column: " + column);
            return CommandLine.ExitCode.SOFTWARE;
        }

        InputFile inputFile = fileMixin.toInputFile();
        try (HardwoodContextImpl context = HardwoodContextImpl.create(1)) {
            inputFile.open();
            printDictionaries(metadata, columnSchema, context, inputFile);
        }
        catch (IOException e) {
            spec.commandLine().getErr().println("Error reading dictionary: " + e.getMessage());
            return CommandLine.ExitCode.SOFTWARE;
        }
        finally {
            try {
                inputFile.close();
            }
            catch (IOException e) {
                spec.commandLine().getErr().println("Error closing file: " + e.getMessage());
            }
        }

        return CommandLine.ExitCode.OK;
    }

    private void printDictionaries(FileMetaData metadata, ColumnSchema columnSchema,
                                   HardwoodContextImpl context, InputFile inputFile)
            throws IOException {
        List<RowGroup> rowGroups = metadata.rowGroups();

        for (int rgIdx = 0; rgIdx < rowGroups.size(); rgIdx++) {
            RowGroup rg = rowGroups.get(rgIdx);
            ColumnChunk chunk = rg.columns().get(columnSchema.columnIndex());

            RowGroupIndexBuffers indexBuffers = RowGroupIndexBuffers.fetch(inputFile, rg);
            Long dictOffset = chunk.metaData().dictionaryPageOffset();
            long chunkStart = (dictOffset != null && dictOffset > 0)
                    ? dictOffset
                    : chunk.metaData().dataPageOffset();
            int chunkLen = Math.toIntExact(chunk.metaData().totalCompressedSize());
            ByteBuffer chunkData = inputFile.readRange(chunkStart, chunkLen);

            PageScanner scanner = new PageScanner(columnSchema, chunk, context,
                    chunkData, chunkStart, indexBuffers.forColumn(columnSchema.columnIndex()),
                    rgIdx, fileMixin.file);
            List<PageInfo> pages = scanner.scanPages();

            Dictionary dictionary = pages.isEmpty() ? null : pages.get(0).dictionary();

            spec.commandLine().getOut().printf("Row Group %d / %s%n", rgIdx, Sizes.columnPath(chunk.metaData()));

            if (dictionary == null) {
                spec.commandLine().getOut().println("  No dictionary (column is not dictionary-encoded)");
            }
            else {
                printDictionary(dictionary);
            }
            spec.commandLine().getOut().println();
        }
    }

    private void printDictionary(Dictionary dictionary) {
        spec.commandLine().getOut().printf("  Dictionary size: %d entries%n", dictionary.size());
        switch (dictionary) {
            case Dictionary.IntDictionary d -> printInts(d.values());
            case Dictionary.LongDictionary d -> printLongs(d.values());
            case Dictionary.FloatDictionary d -> printFloats(d.values());
            case Dictionary.DoubleDictionary d -> printDoubles(d.values());
            case Dictionary.ByteArrayDictionary d -> printByteArrays(d.values());
        }
    }

    private void printInts(int[] values) {
        for (int i = 0; i < values.length; i++) {
            spec.commandLine().getOut().printf("  [%4d] %d%n", i, values[i]);
        }
    }

    private void printLongs(long[] values) {
        for (int i = 0; i < values.length; i++) {
            spec.commandLine().getOut().printf("  [%4d] %d%n", i, values[i]);
        }
    }

    private void printFloats(float[] values) {
        for (int i = 0; i < values.length; i++) {
            spec.commandLine().getOut().printf("  [%4d] %f%n", i, values[i]);
        }
    }

    private void printDoubles(double[] values) {
        for (int i = 0; i < values.length; i++) {
            spec.commandLine().getOut().printf("  [%4d] %f%n", i, values[i]);
        }
    }

    private void printByteArrays(byte[][] values) {
        for (int i = 0; i < values.length; i++) {
            spec.commandLine().getOut().printf("  [%4d] %s%n", i, formatBytes(values[i]));
        }
    }

    private static String formatBytes(byte[] bytes) {
        if (bytes == null) {
            return "null";
        }
        String text = new String(bytes, StandardCharsets.UTF_8);
        if (text.length() > 60) {
            return text.substring(0, 60) + "...";
        }
        return text;
    }
}
