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

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.invocation.CommandInvocation;
import org.aesh.command.option.Mixin;
import org.aesh.command.option.Option;

import dev.hardwood.InputFile;
import dev.hardwood.cli.internal.BinaryValues;
import dev.hardwood.cli.internal.Sizes;
import dev.hardwood.cli.internal.Strings;
import dev.hardwood.cli.internal.ValueFormatter;
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

@CommandDefinition(name = "dictionary", description = "Print dictionary entries for a column.", generateHelp = true)
public class InspectDictionaryCommand implements Command<CommandInvocation> {

    @Mixin
    FileMixin fileMixin;

    @Option(shortName = 'c', name = "column", required = true, description = "Column name to inspect.")
    String column;

    @Option(name = "limit", defaultValue = "50", description = "Maximum dictionary entries per row group to print (0 = unlimited).")
    int limit;

    @Option(shortName = 'w', name = "max-width", defaultValue = "50", description = "Max width of a column.")
    int maxWidth;

    @Option(shortName = 't', name = "truncate", hasValue = false, negatable = true, defaultValue = "true", description = "Should values wider than the column cap be truncated.")
    Boolean truncate;

    @Override
    public CommandResult execute(CommandInvocation ci) {
        if (fileMixin.toInputFile() == null) {
            return CommandResult.FAILURE;
        }
        if (limit < 0) {
            System.err.println("--limit must be greater than or equal to 0");
            return CommandResult.FAILURE;
        }
        // A column has to be at least one cell wide to render anything at all,
        // so a smaller value has no faithful rendering rather than an ugly one.
        if (maxWidth < 1) {
            System.err.println("--max-width must be greater than or equal to 1");
            return CommandResult.FAILURE;
        }

        FileMetaData metadata;
        FileSchema schema;
        try (ParquetFileReader reader = ParquetFileReader.open(fileMixin.toInputFile())) {
            metadata = reader.getFileMetaData();
            schema = reader.getFileSchema();
        }
        catch (IOException e) {
            System.err.println("Error reading file: " + e.getMessage());
            return CommandResult.FAILURE;
        }

        ColumnSchema columnSchema;
        try {
            columnSchema = schema.getColumn(column);
        }
        catch (IllegalArgumentException e) {
            System.err.println("Unknown column: " + column);
            return CommandResult.FAILURE;
        }

        InputFile inputFile = fileMixin.toInputFile();
        try (HardwoodContextImpl context = HardwoodContextImpl.create(1)) {
            inputFile.open();
            printDictionaries(metadata, columnSchema, context, inputFile);
        }
        catch (IOException e) {
            System.err.println("Error reading dictionary: " + e.getMessage());
            return CommandResult.FAILURE;
        }
        finally {
            try {
                inputFile.close();
            }
            catch (IOException e) {
                System.err.println("Error closing file: " + e.getMessage());
            }
        }

        return CommandResult.SUCCESS;
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
            chunk.requireSameFile();
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
                addDictionaryRows(rows, rgIdx, dictionary, columnSchema, displayed, includeLength, cellBudget());
            }
        }

        System.out.println(columnLabel);
        for (String message : messages) {
            System.out.println(message);
        }
        if (!rows.isEmpty()) {
            String[] headers = includeLength
                    ? new String[]{"RG", "Index", "Length", "Value"}
                    : new String[]{"RG", "Index", "Value"};
            System.out.println(RowTable.renderTable(headers, rows, separatorsBefore, List.of()));
        }
    }

    /// No cell can be wider than the column cap, so hexing a binary payload
    /// beyond it is waste. An untruncated table shows the value whole, and has
    /// no cap.
    private int cellBudget() {
        return truncate ? maxWidth : BinaryValues.NO_LIMIT;
    }

    /// Bounds a rendered entry to the column cap and marks the cut, so a value
    /// the table had to shorten does not read as complete.
    private static String cell(String value, int budget) {
        return budget == BinaryValues.NO_LIMIT ? value : Strings.truncateRight(value, budget);
    }

    private static void addDictionaryRows(List<String[]> rows, int rgIdx, Dictionary dictionary,
                                          ColumnSchema columnSchema, int displayed, boolean includeLength,
                                          int budget) {
        switch (dictionary) {
            case Dictionary.IntDictionary d -> {
                int[] vs = d.values();
                for (int i = 0; i < displayed; i++) {
                    addRow(rows, rgIdx, i, cell(ValueFormatter.formatDecoded(vs[i], columnSchema), budget));
                }
            }
            case Dictionary.LongDictionary d -> {
                long[] vs = d.values();
                for (int i = 0; i < displayed; i++) {
                    addRow(rows, rgIdx, i, cell(ValueFormatter.formatDecoded(vs[i], columnSchema), budget));
                }
            }
            case Dictionary.FloatDictionary d -> {
                float[] vs = d.values();
                for (int i = 0; i < displayed; i++) {
                    addRow(rows, rgIdx, i, cell(ValueFormatter.formatDecoded(vs[i]), budget));
                }
            }
            case Dictionary.DoubleDictionary d -> {
                double[] vs = d.values();
                for (int i = 0; i < displayed; i++) {
                    addRow(rows, rgIdx, i, cell(ValueFormatter.formatDecoded(vs[i]), budget));
                }
            }
            case Dictionary.ByteArrayDictionary d -> addByteArrayRows(
                    rows, rgIdx, d.values(), columnSchema, displayed, includeLength, budget);
        }
    }

    private static void addByteArrayRows(List<String[]> rows, int rgIdx, byte[][] values,
                                         ColumnSchema columnSchema, int displayed, boolean includeLength,
                                         int budget) {
        for (int i = 0; i < displayed; i++) {
            byte[] value = values[i];
            // The budget bounds the hex build too, so a large payload costs a
            // cell rather than twice its own size to render into one.
            String formatted = cell(ValueFormatter.formatBytes(value, columnSchema, true, budget), budget);
            if (includeLength) {
                rows.add(new String[]{
                        rgCell(i, rgIdx),
                        String.valueOf(i),
                        value != null ? String.valueOf(value.length) : Strings.ABSENT_VALUE,
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
