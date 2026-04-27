/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import dev.hardwood.InputFile;
import dev.hardwood.cli.internal.IndexValueFormatter;
import dev.hardwood.cli.internal.Sizes;
import dev.hardwood.cli.internal.table.RowTable;
import dev.hardwood.internal.reader.Dictionary;
import dev.hardwood.internal.reader.DictionaryParser;
import dev.hardwood.internal.reader.HardwoodContextImpl;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.PhysicalType;
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
    @CommandLine.Option(names = {"-c", "--column"}, required = true, paramLabel = "COLUMN", description = "Column name to inspect.")
    String column;
    @CommandLine.Option(names = "--limit", defaultValue = "50", paramLabel = "N",
            description = "Maximum dictionary entries per row group to print (0 = unlimited).")
    int limit;

    @Override
    public Integer call() {
        if (fileMixin.toInputFile() == null) {
            return CommandLine.ExitCode.SOFTWARE;
        }
        if (limit < 0) {
            spec.commandLine().getErr().println("--limit must be greater than or equal to 0");
            return CommandLine.ExitCode.USAGE;
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
        List<String[]> rows = new ArrayList<>();
        List<Integer> separatorsBefore = new ArrayList<>();
        List<String> messages = new ArrayList<>();
        String columnLabel = rowGroups.isEmpty()
                ? columnSchema.name()
                : Sizes.columnPath(rowGroups.getFirst().columns().get(columnSchema.columnIndex()).metaData());
        boolean includeLength = hasVariableWidthDictionaryValues(columnSchema);

        for (int rgIdx = 0; rgIdx < rowGroups.size(); rgIdx++) {
            RowGroup rg = rowGroups.get(rgIdx);
            ColumnChunk chunk = rg.columns().get(columnSchema.columnIndex());

            // Read just the dictionary prefix of the column chunk
            Long dictOffset = chunk.metaData().dictionaryPageOffset();
            long chunkStart = (dictOffset != null && dictOffset > 0)
                    ? dictOffset
                    : chunk.metaData().dataPageOffset();
            // Read enough for the dictionary page (typically a few KB)
            int dictReadSize = Math.toIntExact(Math.min(
                    chunk.metaData().totalCompressedSize(), 4 * 1024 * 1024));
            ByteBuffer dictRegion = inputFile.readRange(chunkStart, dictReadSize);

            Dictionary dictionary = DictionaryParser.parse(
                    dictRegion, columnSchema, chunk.metaData(), context);

            if (dictionary == null) {
                messages.add("Row Group " + rgIdx + ": no dictionary (column is not dictionary-encoded)");
            }
            else {
                if (!rows.isEmpty()) {
                    separatorsBefore.add(rows.size());
                }
                int displayed = displayedEntryCount(dictionary);
                if (displayed < dictionary.size()) {
                    messages.add("Row Group " + rgIdx + " - dictionary has " + dictionary.size()
                            + " entries (showing first " + displayed + ")");
                }
                addDictionaryRows(rows, rgIdx, dictionary, columnSchema, displayed, includeLength);
            }
        }

        spec.commandLine().getOut().println(columnLabel);
        for (String message : messages) {
            spec.commandLine().getOut().println(message);
        }
        if (!rows.isEmpty()) {
            String[] headers = includeLength
                    ? new String[]{"RG", "Index", "Length", "Value"}
                    : new String[]{"RG", "Index", "Value"};
            spec.commandLine().getOut().println(RowTable.renderTable(headers, rows, separatorsBefore, List.of()));
        }
    }

    private static void addDictionaryRows(List<String[]> rows, int rgIdx, Dictionary dictionary,
                                          ColumnSchema columnSchema, int displayed, boolean includeLength) {
        switch (dictionary) {
            case Dictionary.IntDictionary d -> {
                int[] vs = d.values();
                for (int i = 0; i < displayed; i++) {
                    addRow(rows, rgIdx, i, IndexValueFormatter.formatDecoded(vs[i], columnSchema));
                }
            }
            case Dictionary.LongDictionary d -> {
                long[] vs = d.values();
                for (int i = 0; i < displayed; i++) {
                    addRow(rows, rgIdx, i, IndexValueFormatter.formatDecoded(vs[i], columnSchema));
                }
            }
            case Dictionary.FloatDictionary d -> {
                float[] vs = d.values();
                for (int i = 0; i < displayed; i++) {
                    addRow(rows, rgIdx, i, IndexValueFormatter.formatDecoded(vs[i]));
                }
            }
            case Dictionary.DoubleDictionary d -> {
                double[] vs = d.values();
                for (int i = 0; i < displayed; i++) {
                    addRow(rows, rgIdx, i, IndexValueFormatter.formatDecoded(vs[i]));
                }
            }
            case Dictionary.ByteArrayDictionary d -> addByteArrayRows(
                    rows, rgIdx, d.values(), columnSchema, displayed, includeLength);
        }
    }

    private static void addByteArrayRows(List<String[]> rows, int rgIdx, byte[][] values,
                                         ColumnSchema columnSchema, int displayed, boolean includeLength) {
        for (int i = 0; i < displayed; i++) {
            byte[] value = values[i];
            String formatted = IndexValueFormatter.formatDecoded(value, columnSchema);
            if (includeLength) {
                rows.add(new String[]{
                        rgCell(i, rgIdx),
                        String.valueOf(i),
                        value != null ? String.valueOf(value.length) : "-",
                        formatted
                });
            }
            else {
                // INT96 dictionaries also flow through ByteArrayDictionary but
                // store a fixed 12-byte payload, so the Length column is
                // suppressed (the header omits it for non-BYTE_ARRAY columns).
                addRow(rows, rgIdx, i, formatted);
            }
        }
    }

    private static void addRow(List<String[]> rows, int rgIdx, int entryIndex, String value) {
        rows.add(new String[]{rgCell(entryIndex, rgIdx), String.valueOf(entryIndex), value});
    }

    private static boolean hasVariableWidthDictionaryValues(ColumnSchema columnSchema) {
        return columnSchema.type() == PhysicalType.BYTE_ARRAY
                || columnSchema.type() == PhysicalType.FIXED_LEN_BYTE_ARRAY;
    }

    private int displayedEntryCount(Dictionary dictionary) {
        return limit == 0 ? dictionary.size() : Math.min(limit, dictionary.size());
    }

    private static String rgCell(int entryIndex, int rgIdx) {
        return entryIndex == 0 ? String.valueOf(rgIdx) : "";
    }
}
