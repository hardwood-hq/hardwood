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
import dev.hardwood.internal.metadata.DataPageHeader;
import dev.hardwood.internal.metadata.DataPageHeaderV2;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.thrift.ColumnIndexReader;
import dev.hardwood.internal.thrift.OffsetIndexReader;
import dev.hardwood.internal.thrift.PageHeaderReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnIndex;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.OffsetIndex;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

@CommandLine.Command(name = "pages", description = "List data and dictionary pages per column chunk.")
public class InspectPagesCommand implements Callable<Integer> {

    @CommandLine.Mixin
    HelpMixin help;

    @CommandLine.Mixin
    FileMixin fileMixin;
    @Spec
     CommandSpec spec;
    @CommandLine.Option(names = {"-c", "--column"}, paramLabel = "COLUMN", description = "Restrict output to a single column.")
    String column;

    @CommandLine.Option(names = "--no-stats", description = "Omit page-index derived columns (First Row, Min, Max, Nulls) even when available.")
    boolean noStats;

    @Override
    public Integer call() {
        InputFile inputFile = fileMixin.toInputFile();
        if (inputFile == null) {
            return CommandLine.ExitCode.SOFTWARE;
        }

        FileMetaData metadata;
        FileSchema schema;
        try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
            metadata = reader.getFileMetaData();
            schema = reader.getFileSchema();
        }
        catch (IOException e) {
            spec.commandLine().getErr().println("Error reading file: " + e.getMessage());
            return CommandLine.ExitCode.SOFTWARE;
        }

        int filterColumnIndex = -1;
        if (column != null) {
            try {
                filterColumnIndex = schema.getColumn(column).columnIndex();
            }
            catch (IllegalArgumentException e) {
                spec.commandLine().getErr().println("Unknown column: " + column);
                return CommandLine.ExitCode.SOFTWARE;
            }
        }

        InputFile pageInputFile = fileMixin.toInputFile();
        try {
            pageInputFile.open();
            printPages(metadata, schema, pageInputFile, filterColumnIndex);
        }
        catch (IOException e) {
            spec.commandLine().getErr().println("Error reading pages: " + e.getMessage());
            return CommandLine.ExitCode.SOFTWARE;
        }
        finally {
            try {
                pageInputFile.close();
            }
            catch (IOException e) {
                spec.commandLine().getErr().println("Error closing file: " + e.getMessage());
            }
        }

        return CommandLine.ExitCode.OK;
    }

    private static final String[] HEADERS_PLAIN = {"RG", "Page", "Type", "Encoding", "Compressed", "Values"};
    private static final String[] HEADERS_WITH_INDEX = {"RG", "Page", "Type", "Encoding", "First Row", "Compressed", "Values", "Min", "Max", "Nulls"};

    private void printPages(FileMetaData metadata, FileSchema schema, InputFile inputFile, int filterColumnIndex) throws IOException {
        List<RowGroup> rowGroups = metadata.rowGroups();
        List<ColumnSchema> columns = schema.getColumns();

        boolean firstColumn = true;
        for (ColumnSchema col : columns) {
            if (filterColumnIndex >= 0 && col.columnIndex() != filterColumnIndex) {
                continue;
            }
            if (!firstColumn) {
                spec.commandLine().getOut().println();
            }
            firstColumn = false;
            renderColumn(col, rowGroups, inputFile);
        }
    }

    private void renderColumn(ColumnSchema col, List<RowGroup> rowGroups, InputFile inputFile) throws IOException {
        List<RowGroupData> rgData = new ArrayList<>();
        boolean anyIndex = false;
        String columnLabel = col.name();
        boolean anyColumnIndex = false;
        boolean anyInline = false;
        boolean anyNoStats = false;

        for (int rgIdx = 0; rgIdx < rowGroups.size(); rgIdx++) {
            RowGroup rg = rowGroups.get(rgIdx);
            ColumnChunk chunk = rg.columns().get(col.columnIndex());
            columnLabel = Sizes.columnPath(chunk.metaData());

            ColumnIndex columnIndex = null;
            OffsetIndex offsetIndex = null;
            if (!noStats) {
                columnIndex = tryLoadColumnIndex(chunk, inputFile);
                offsetIndex = tryLoadOffsetIndex(chunk, inputFile);
            }

            boolean trackRowIndex = chunk.metaData().numValues() == rg.numRows();
            List<PageInfo> pages = collectPageHeaders(chunk.metaData(), inputFile, trackRowIndex);

            boolean rgHasIndex = columnIndex != null && offsetIndex != null;
            boolean rgHasInline = !noStats && hasInlineStats(pages);
            if (rgHasIndex || rgHasInline) {
                anyIndex = true;
            }
            if (rgHasIndex) {
                anyColumnIndex = true;
            }
            else if (rgHasInline) {
                anyInline = true;
            }
            else if (!noStats) {
                anyNoStats = true;
            }
            rgData.add(new RowGroupData(rgIdx, pages, columnIndex, offsetIndex));
        }

        if (!noStats) {
            String suffix = statsSourceSuffix(anyColumnIndex, anyInline, anyNoStats);
            if (!suffix.isEmpty()) {
                columnLabel = columnLabel + "  " + suffix;
            }
        }

        String[] headers = anyIndex ? HEADERS_WITH_INDEX : HEADERS_PLAIN;
        List<String[]> rows = new ArrayList<>();
        List<Integer> separatorsBefore = new ArrayList<>();
        long totalCompressed = 0;
        long totalDataValues = 0;
        long totalNulls = 0;
        boolean anyNullCount = false;

        for (int i = 0; i < rgData.size(); i++) {
            RowGroupData rd = rgData.get(i);
            if (i > 0 && !rd.pages().isEmpty()) {
                separatorsBefore.add(rows.size());
            }

            int dataPageCounter = 0;
            for (int j = 0; j < rd.pages().size(); j++) {
                PageInfo p = rd.pages().get(j);
                String rgCell = (j == 0) ? String.valueOf(rd.rgIdx()) : "";

                if (anyIndex) {
                    IndexCells idx = indexCellsFor(p, dataPageCounter, rd, col);
                    rows.add(new String[]{
                            rgCell,
                            p.label(),
                            p.type(),
                            p.encoding(),
                            idx.firstRow(),
                            Sizes.format(p.compressedSize()),
                            String.valueOf(p.numValues()),
                            idx.min(),
                            idx.max(),
                            idx.nulls()
                    });
                    if (idx.nullCount() >= 0) {
                        totalNulls += idx.nullCount();
                        anyNullCount = true;
                    }
                }
                else {
                    rows.add(new String[]{
                            rgCell,
                            p.label(),
                            p.type(),
                            p.encoding(),
                            Sizes.format(p.compressedSize()),
                            String.valueOf(p.numValues())
                    });
                }

                totalCompressed += p.compressedSize();
                if (!p.isDictionary()) {
                    totalDataValues += p.numValues();
                    dataPageCounter++;
                }
            }
        }

        int totalRowIdx = rows.size();
        if (anyIndex) {
            rows.add(new String[]{
                    "",
                    "Total",
                    "",
                    "",
                    "",
                    Sizes.format(totalCompressed),
                    String.valueOf(totalDataValues),
                    "",
                    "",
                    anyNullCount ? String.valueOf(totalNulls) : "-"
            });
        }
        else {
            rows.add(new String[]{
                    "",
                    "Total",
                    "",
                    "",
                    Sizes.format(totalCompressed),
                    String.valueOf(totalDataValues)
            });
        }

        spec.commandLine().getOut().println(columnLabel);
        spec.commandLine().getOut().println(RowTable.renderTable(headers, rows, separatorsBefore, List.of(totalRowIdx)));
    }

    /// Formats the column label suffix indicating where the Min / Max / Nulls
    /// cells' values came from for this column. Reports `ColumnIndex` (side-car
    /// Page Index), `inline` (`DataPageHeader.statistics`), `mixed` when row
    /// groups disagree, or `no page-level stats` when neither is present.
    private static String statsSourceSuffix(boolean anyColumnIndex, boolean anyInline, boolean anyNoStats) {
        int kinds = (anyColumnIndex ? 1 : 0) + (anyInline ? 1 : 0) + (anyNoStats ? 1 : 0);
        if (kinds == 0) {
            return "";
        }
        if (kinds > 1) {
            return "(stats: mixed)";
        }
        if (anyColumnIndex) {
            return "(stats: ColumnIndex)";
        }
        if (anyInline) {
            return "(stats: inline)";
        }
        return "(no page-level stats)";
    }

    private static boolean hasInlineStats(List<PageInfo> pages) {
        for (PageInfo p : pages) {
            if (!p.isDictionary() && p.inlineStats() != null) {
                return true;
            }
        }
        return false;
    }

    private static IndexCells indexCellsFor(PageInfo p, int dataPageCounter, RowGroupData rd, ColumnSchema col) {
        if (p.isDictionary()) {
            return IndexCells.DASHES;
        }
        ColumnIndex ci = rd.columnIndex();
        OffsetIndex oi = rd.offsetIndex();
        boolean hasColumnIndex = ci != null && oi != null
                && dataPageCounter < oi.pageLocations().size()
                && dataPageCounter < ci.minValues().size();
        if (hasColumnIndex) {
            return fromColumnIndex(p, dataPageCounter, ci, oi, col);
        }
        return fromInlineStats(p, col);
    }

    private static IndexCells fromColumnIndex(PageInfo p, int dataPageCounter,
            ColumnIndex ci, OffsetIndex oi, ColumnSchema col) {
        String firstRow = String.valueOf(oi.pageLocations().get(dataPageCounter).firstRowIndex());
        String min;
        String max;
        if (ci.nullPages().get(dataPageCounter)) {
            min = "(null page)";
            max = "(null page)";
        }
        else {
            min = IndexValueFormatter.format(ci.minValues().get(dataPageCounter), col);
            max = IndexValueFormatter.format(ci.maxValues().get(dataPageCounter), col);
        }
        long nullCount = -1;
        String nulls = "-";
        if (ci.nullCounts() != null && dataPageCounter < ci.nullCounts().size()) {
            nullCount = ci.nullCounts().get(dataPageCounter);
            nulls = String.valueOf(nullCount);
        }
        return new IndexCells(firstRow, min, max, nulls, nullCount);
    }

    private static IndexCells fromInlineStats(PageInfo p, ColumnSchema col) {
        Statistics stats = p.inlineStats();
        String firstRow = p.firstRowIndex() != null ? String.valueOf(p.firstRowIndex()) : "-";
        if (stats == null) {
            return new IndexCells(firstRow, "-", "-", "-", -1);
        }
        String min;
        String max;
        if (stats.isMinMaxDeprecated()) {
            min = "(deprecated)";
            max = "(deprecated)";
        }
        else {
            min = stats.minValue() != null ? IndexValueFormatter.format(stats.minValue(), col) : "-";
            max = stats.maxValue() != null ? IndexValueFormatter.format(stats.maxValue(), col) : "-";
        }
        long nullCount = stats.nullCount() != null ? stats.nullCount() : -1;
        String nulls = nullCount >= 0 ? String.valueOf(nullCount) : "-";
        return new IndexCells(firstRow, min, max, nulls, nullCount);
    }

    private static ColumnIndex tryLoadColumnIndex(ColumnChunk chunk, InputFile inputFile) {
        Long offset = chunk.columnIndexOffset();
        Integer length = chunk.columnIndexLength();
        if (offset == null || length == null || length <= 0) {
            return null;
        }
        try {
            ByteBuffer buffer = inputFile.readRange(offset, length);
            return ColumnIndexReader.read(new ThriftCompactReader(buffer));
        }
        catch (IOException e) {
            return null;
        }
    }

    private static OffsetIndex tryLoadOffsetIndex(ColumnChunk chunk, InputFile inputFile) {
        Long offset = chunk.offsetIndexOffset();
        Integer length = chunk.offsetIndexLength();
        if (offset == null || length == null || length <= 0) {
            return null;
        }
        try {
            ByteBuffer buffer = inputFile.readRange(offset, length);
            return OffsetIndexReader.read(new ThriftCompactReader(buffer));
        }
        catch (IOException e) {
            return null;
        }
    }

    private record RowGroupData(int rgIdx, List<PageInfo> pages, ColumnIndex columnIndex, OffsetIndex offsetIndex) {
    }

    private record IndexCells(String firstRow, String min, String max, String nulls, long nullCount) {
        static final IndexCells DASHES = new IndexCells("-", "-", "-", "-", -1);
    }

    private record PageInfo(String label, String type, String encoding, long compressedSize, long numValues,
            boolean isDictionary, Long firstRowIndex, Statistics inlineStats) {
    }

    private List<PageInfo> collectPageHeaders(ColumnMetaData cmd, InputFile inputFile, boolean trackRowIndex) throws IOException {
        Long dictOffset = cmd.dictionaryPageOffset();
        long chunkStart = (dictOffset != null && dictOffset > 0) ? dictOffset : cmd.dataPageOffset();
        long chunkSize = cmd.totalCompressedSize();

        ByteBuffer buffer = inputFile.readRange(chunkStart, (int) chunkSize);

        List<PageInfo> rows = new ArrayList<>();
        int pageIndex = 0;
        long valuesRead = 0;
        int position = 0;

        while (position < buffer.limit()) {
            ThriftCompactReader headerReader = new ThriftCompactReader(buffer, position);
            PageHeader header = PageHeaderReader.read(headerReader);
            int headerSize = headerReader.getBytesRead();

            boolean isDictionary = header.type() == PageHeader.PageType.DICTIONARY_PAGE;
            String label = isDictionary ? "dict" : String.valueOf(pageIndex);
            Long firstRowIndex = null;
            Statistics inlineStats = null;
            if (!isDictionary) {
                if (trackRowIndex) {
                    firstRowIndex = valuesRead;
                }
                inlineStats = inlineStatsOf(header);
            }

            rows.add(new PageInfo(
                    label,
                    shortType(header.type()),
                    pageEncoding(header),
                    header.compressedPageSize(),
                    numValues(header),
                    isDictionary,
                    firstRowIndex,
                    inlineStats
            ));

            if (header.type() == PageHeader.PageType.DATA_PAGE || header.type() == PageHeader.PageType.DATA_PAGE_V2) {
                valuesRead += numValues(header);
                pageIndex++;
                if (valuesRead >= cmd.numValues()) {
                    break;
                }
            }

            position += headerSize + header.compressedPageSize();
        }

        return rows;
    }

    private static Statistics inlineStatsOf(PageHeader header) {
        return switch (header.type()) {
            case DATA_PAGE -> {
                DataPageHeader dp = header.dataPageHeader();
                yield dp != null ? dp.statistics() : null;
            }
            case DATA_PAGE_V2 -> {
                DataPageHeaderV2 dp = header.dataPageHeaderV2();
                yield dp != null ? dp.statistics() : null;
            }
            case DICTIONARY_PAGE, INDEX_PAGE -> null;
        };
    }

    private static String shortType(PageHeader.PageType type) {
        return switch (type) {
            case DATA_PAGE -> "DATA";
            case DATA_PAGE_V2 -> "DATA_V2";
            case DICTIONARY_PAGE -> "DICT";
            case INDEX_PAGE -> "INDEX";
        };
    }

    private static String pageEncoding(PageHeader header) {
        return switch (header.type()) {
            case DATA_PAGE -> shortEncoding(header.dataPageHeader().encoding().name());
            case DATA_PAGE_V2 -> shortEncoding(header.dataPageHeaderV2().encoding().name());
            case DICTIONARY_PAGE -> shortEncoding(header.dictionaryPageHeader().encoding().name());
            case INDEX_PAGE -> "N/A";
        };
    }

    private static String shortEncoding(String encoding) {
        return switch (encoding) {
            case "PLAIN_DICTIONARY" -> "PLAIN_DICT";
            case "RLE_DICTIONARY" -> "RLE_DICT";
            default -> encoding;
        };
    }

    private static int numValues(PageHeader header) {
        return switch (header.type()) {
            case DATA_PAGE -> header.dataPageHeader().numValues();
            case DATA_PAGE_V2 -> header.dataPageHeaderV2().numValues();
            case DICTIONARY_PAGE -> header.dictionaryPageHeader().numValues();
            case INDEX_PAGE -> 0;
        };
    }
}
