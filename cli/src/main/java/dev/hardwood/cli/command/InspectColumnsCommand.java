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
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.invocation.CommandInvocation;
import org.aesh.command.option.Mixin;
import org.aesh.command.option.Option;

import dev.hardwood.InputFile;
import dev.hardwood.cli.internal.Encodings;
import dev.hardwood.cli.internal.Fmt;
import dev.hardwood.cli.internal.LevelSummary;
import dev.hardwood.cli.internal.Sizes;
import dev.hardwood.cli.internal.Strings;
import dev.hardwood.cli.internal.table.RowTable;
import dev.hardwood.internal.thrift.OffsetIndexReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.OffsetIndex;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

@CommandDefinition(name = "columns", description = "Rank columns by size, with each column's share of the file, its compression and its unencoded size.", generateHelp = true)
public class InspectColumnsCommand implements Command<CommandInvocation> {

    /// Fixed render width for the level blocks. The output is meant to be
    /// diffed and pasted, so it must not vary with the terminal it ran in.
    private static final int DETAIL_LEVEL_WIDTH = 60;

    @Mixin
    FileMixin fileMixin;

    @Option(shortName = 'c', name = "column", description = "Print per-row-group detail and level histograms for a single column.")
    String column;

    @Option(name = "row-group", description = "Restrict --column output to a single row group.")
    Integer rowGroup;

    @Override
    public CommandResult execute(CommandInvocation ci) {
        if (rowGroup != null && column == null) {
            System.err.println("--row-group requires --column");
            return CommandResult.FAILURE;
        }
        InputFile inputFile = fileMixin.toInputFile();
        if (inputFile == null) {
            return CommandResult.FAILURE;
        }

        try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
            FileMetaData metadata = reader.getFileMetaData();
            try {
                inputFile.open();
                if (column != null) {
                    return printColumnDetail(metadata, reader.getFileSchema(), inputFile);
                }
                List<ColumnSize> sizes = aggregateSizes(metadata, reader.getFileSchema(), inputFile);
                sizes.sort(Comparator.comparingLong(ColumnSize::compressed).reversed());
                printRanked(sizes);
            }
            finally {
                inputFile.close();
            }
        }
        catch (IOException e) {
            System.err.println("Error reading file: " + e.getMessage());
            return CommandResult.FAILURE;
        }

        return CommandResult.SUCCESS;
    }

    /// Prints one row per row group for a single column, then the level
    /// histograms. Level histograms combine by addition, so the default block
    /// covers the whole file exactly; `--row-group` narrows both.
    private CommandResult printColumnDetail(FileMetaData metadata, FileSchema schema, InputFile inputFile) {
        ColumnSchema columnSchema = findColumn(schema);
        if (columnSchema == null) {
            System.err.println("No such column: " + column);
            return CommandResult.FAILURE;
        }
        if (rowGroup != null && (rowGroup < 0 || rowGroup >= metadata.rowGroups().size())) {
            System.err.println("No such row group: " + rowGroup
                    + " (file has " + metadata.rowGroups().size() + ")");
            return CommandResult.FAILURE;
        }

        System.out.println(header(columnSchema));
        System.out.println();

        List<String[]> rows = new ArrayList<>();
        List<String> mismatches = new ArrayList<>();
        List<List<LevelSummary.LevelRow>> definitionLevels = new ArrayList<>();
        List<List<LevelSummary.LevelRow>> repetitionLevels = new ArrayList<>();
        for (int rg = 0; rg < metadata.rowGroups().size(); rg++) {
            if (rowGroup != null && rowGroup != rg) {
                continue;
            }
            ColumnChunk chunk = chunkOf(metadata.rowGroups().get(rg), columnSchema);
            if (chunk == null) {
                continue;
            }
            LevelSummary summary = LevelSummary.of(schema, columnSchema, chunk.metaData());
            rows.add(detailRow(rg, chunk, summary, inputFile));
            if (summary.mismatch() != null) {
                mismatches.add(Fmt.fmt("RG #%d  declared vs actual: %s", rg, summary.mismatch()));
            }
            definitionLevels.add(summary.definitionLevels());
            repetitionLevels.add(summary.repetitionLevels());
        }
        System.out.println(RowTable.renderTable(
                new String[]{"RG", "Values", "Nulls", "Records", "Present", "Fan-out",
                        "Codec", "Compressed", "Compression", "Encoding", "Unencoded"},
                rows));

        // The consistency check runs on both surfaces. `dive` paints it red;
        // here it is the ⚠ and the wording alone, under the rows it refers to.
        for (String mismatch : mismatches) {
            System.out.println("⚠ " + mismatch);
        }

        String scope = rowGroup != null ? "RG #" + rowGroup : "all row groups";
        printLevelBlock("Definition levels", scope, columnSchema.maxDefinitionLevel(),
                LevelSummary.combineLevels(definitionLevels));
        printLevelBlock("Repetition levels", scope, columnSchema.maxRepetitionLevel(),
                LevelSummary.combineLevels(repetitionLevels));
        return CommandResult.SUCCESS;
    }

    private static void printLevelBlock(String title, String scope, int maxLevel,
                                        List<LevelSummary.LevelRow> levels) {
        if (levels.isEmpty()) {
            return;
        }
        System.out.println();
        System.out.println(Fmt.fmt("%s (%s, max %d)", title, scope, maxLevel));
        // A fixed width rather than the terminal's: the output is meant to be
        // diffed and pasted, so it must not vary with the window it ran in.
        for (String line : LevelSummary.renderLevels(levels, DETAIL_LEVEL_WIDTH)) {
            System.out.println(line);
        }
    }

    private String[] detailRow(int rg, ColumnChunk chunk, LevelSummary summary, InputFile inputFile) {
        ColumnMetaData cmd = chunk.metaData();
        long nulls = summary.nullCount(cmd.statistics());
        return new String[]{
                String.valueOf(rg),
                Fmt.fmt("%,d", cmd.numValues()),
                nulls >= 0 ? Fmt.fmt("%,d", nulls) : Strings.ABSENT_VALUE,
                // Printed wherever the value is known, including where it
                // follows from the column's shape rather than a histogram. The
                // column occupies its width either way, and a reader — or a
                // parser — should not have to know that a non-repeated column
                // has one value per record to reconstruct the number.
                summary.hasRecords() ? Fmt.fmt("%,d", summary.records()) : Strings.ABSENT_VALUE,
                summary.hasPresentValues() ? Fmt.fmt("%,d", summary.presentValues()) : Strings.ABSENT_VALUE,
                summary.hasAvgFanOut() ? Fmt.fmt("%.2f", summary.avgFanOut()) : Strings.ABSENT_VALUE,
                cmd.codec().name(),
                Sizes.format(cmd.totalCompressedSize()),
                Sizes.compression(cmd.totalCompressedSize(), cmd.totalUncompressedSize()),
                Encodings.label(Encodings.dataPages(cmd),
                        Encodings.dictionaryEntries(chunk, inputFile),
                        dictionaryDenominator(summary, cmd)),
                summary.hasUnencoded()
                        ? Sizes.format(summary.unencodedBytes())
                        : Strings.ABSENT_VALUE
        };
    }

    /// How much of the chunk the file describes: the chunk-level statistics
    /// always, and the per-page copies when the page index carries them. Both
    /// indexes count — the histograms live in the column index and the
    /// unencoded sizes in the offset index.
    ///
    private static OffsetIndex readOffsetIndex(ColumnChunk chunk, InputFile inputFile) throws IOException {
        Long offset = chunk.offsetIndexOffset();
        Integer length = chunk.offsetIndexLength();
        if (offset == null || length == null || length <= 0) {
            return null;
        }
        return OffsetIndexReader.read(new ThriftCompactReader(inputFile.readRange(offset, length)));
    }

    private String header(ColumnSchema columnSchema) {
        StringBuilder header = new StringBuilder(columnSchema.fieldPath().toString());
        header.append("  ").append(columnSchema.type().name());
        if (columnSchema.logicalType() != null) {
            header.append(" / ").append(columnSchema.logicalType());
        }
        header.append("  max def ").append(columnSchema.maxDefinitionLevel());
        header.append("  max rep ").append(columnSchema.maxRepetitionLevel());
        return header.toString();
    }

    private ColumnSchema findColumn(FileSchema schema) {
        for (ColumnSchema candidate : schema.getColumns()) {
            if (candidate.fieldPath().matchesDottedName(column)) {
                return candidate;
            }
        }
        return null;
    }

    private static ColumnChunk chunkOf(RowGroup rowGroup, ColumnSchema columnSchema) {
        for (ColumnChunk chunk : rowGroup.columns()) {
            if (chunk.metaData().pathInSchema().equals(columnSchema.fieldPath())) {
                return chunk;
            }
        }
        return null;
    }

    private static List<ColumnSize> aggregateSizes(FileMetaData metadata, FileSchema schema,
                                                   InputFile inputFile) {
        Map<String, ColumnSize> byColumn = new LinkedHashMap<>();

        for (RowGroup rg : metadata.rowGroups()) {
            for (ColumnChunk cc : rg.columns()) {
                ColumnMetaData cmd = cc.metaData();
                String path = Sizes.columnPath(cmd);
                int pageCount = countPages(cc, inputFile);
                // One summary per chunk serves both the unencoded size and the
                // dictionary's denominator. A column is reported as a whole
                // only if every one of its chunks yields a figure: a partial
                // sum reads as a real total and understates it.
                LevelSummary summary = LevelSummary.of(schema, schema.getColumn(cmd.pathInSchema()), cmd);
                long unencoded = summary.hasUnencoded() ? summary.unencodedBytes() : -1;
                long entries = Encodings.dictionaryEntries(cc, inputFile);
                long denominator = dictionaryDenominator(summary, cmd);
                ColumnSize existing = byColumn.get(path);
                if (existing == null) {
                    byColumn.put(path, new ColumnSize(path, cmd.type().name(), cmd.codec().name(),
                            cmd.totalCompressedSize(), cmd.totalUncompressedSize(), pageCount, pageCount >= 0,
                            Math.max(unencoded, 0), unencoded >= 0,
                            new TreeSet<>(Encodings.dataPages(cmd)),
                            Math.max(entries, 0), entries >= 0, denominator));
                }
                else {
                    int combinedPages = (existing.pageCountAvailable() && pageCount >= 0)
                            ? existing.pageCount() + pageCount
                            : -1;
                    // The union across row groups: a dictionary the writer
                    // abandoned in one of them is a property of the column as
                    // the file stores it, and this is the table a reader scans
                    // to decide which column to look at more closely.
                    existing.encodings().addAll(Encodings.dataPages(cmd));
                    byColumn.put(path, new ColumnSize(path, existing.type(), existing.codec(),
                            existing.compressed() + cmd.totalCompressedSize(),
                            existing.uncompressed() + cmd.totalUncompressedSize(),
                            combinedPages,
                            existing.pageCountAvailable() && pageCount >= 0,
                            existing.unencoded() + Math.max(unencoded, 0),
                            existing.unencodedAvailable() && unencoded >= 0,
                            existing.encodings(),
                            // Each row group holds its own dictionary, so the
                            // file-wide reading is the entries all of them hold
                            // against all the values they cover — not an average
                            // of the per-chunk shares, which would weight a small
                            // row group like a large one.
                            existing.dictionaryEntries() + Math.max(entries, 0),
                            existing.dictionaryEntriesAvailable() && entries >= 0,
                            existing.dictionaryDenominator() + denominator));
                }
            }
        }

        return new ArrayList<>(byColumn.values());
    }

    /// What the dictionary could hold an entry for. Nulls never reach a
    /// dictionary, so the present-value count is the honest denominator where
    /// it is known; a nullable column measured against `num_values` would
    /// understate its cardinality and hide a dictionary that is a verbatim
    /// copy of every value it holds.
    private static long dictionaryDenominator(LevelSummary summary, ColumnMetaData cmd) {
        return summary.hasPresentValues() ? summary.presentValues() : cmd.numValues();
    }

    private static int countPages(ColumnChunk cc, InputFile inputFile) {
        try {
            OffsetIndex oi = readOffsetIndex(cc, inputFile);
            return oi != null ? oi.pageLocations().size() : -1;
        }
        catch (IOException e) {
            return -1;
        }
    }

    /// The ranked table answers "which column is this file, and what is the
    /// lever". `Share` is that first question stated rather than left as
    /// arithmetic over the `Compressed` column, and `Compression` names what
    /// its percentage divides — `Uncompressed` is dropped because it was only
    /// present as that percentage's other operand and follows from the two.
    /// `Encoding` is here rather than only under `--column` because the reason
    /// to scan this table is to pick the column worth drilling into, and a
    /// dictionary the writer abandoned is one of the few things that decides it.
    private void printRanked(List<ColumnSize> sizes) {
        String[] headers = {"Rank", "Column", "Type", "Codec", "Compressed", "Share", "Compression",
                "Encoding", "Unencoded", "# Pages"};
        long totalCompressed = 0;
        for (ColumnSize s : sizes) {
            totalCompressed += s.compressed();
        }
        List<String[]> rows = new ArrayList<>();
        for (int i = 0; i < sizes.size(); i++) {
            ColumnSize s = sizes.get(i);
            rows.add(new String[]{
                    String.valueOf(i + 1),
                    s.path(),
                    s.type(),
                    s.codec(),
                    Sizes.format(s.compressed()),
                    totalCompressed > 0 ? Fmt.fmt("%.1f%%", 100.0 * s.compressed() / totalCompressed) : Strings.ABSENT_VALUE,
                    Sizes.compression(s.compressed(), s.uncompressed()),
                    Encodings.label(s.encodings(),
                            s.dictionaryEntriesAvailable() ? s.dictionaryEntries() : -1,
                            s.dictionaryDenominator()),
                    s.unencodedAvailable() ? Sizes.format(s.unencoded()) : Strings.ABSENT_VALUE,
                    s.pageCountAvailable() ? String.valueOf(s.pageCount()) : Strings.ABSENT_VALUE
            });
        }
        System.out.println(RowTable.renderTable(headers, rows));
    }

    private record ColumnSize(String path, String type, String codec, long compressed, long uncompressed,
                              int pageCount, boolean pageCountAvailable, long unencoded,
                              boolean unencodedAvailable, Set<Encoding> encodings,
                              long dictionaryEntries, boolean dictionaryEntriesAvailable,
                              long dictionaryDenominator) {
    }
}
