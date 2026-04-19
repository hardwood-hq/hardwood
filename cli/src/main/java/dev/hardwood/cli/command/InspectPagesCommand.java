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
import dev.hardwood.cli.internal.Sizes;
import dev.hardwood.cli.internal.table.RowTable;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.thrift.PageHeaderReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;
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

        if (column != null) {
            try {
                schema.getColumn(column);
            }
            catch (IllegalArgumentException e) {
                spec.commandLine().getErr().println("Unknown column: " + column);
                return CommandLine.ExitCode.SOFTWARE;
            }
        }

        InputFile pageInputFile = fileMixin.toInputFile();
        try {
            pageInputFile.open();
            printPages(metadata, schema, pageInputFile);
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

    private static final String[] HEADERS = {"RG", "Page", "Type", "Encoding", "Compressed", "Values"};

    private void printPages(FileMetaData metadata, FileSchema schema, InputFile inputFile) throws IOException {
        List<RowGroup> rowGroups = metadata.rowGroups();
        List<ColumnSchema> columns = schema.getColumns();

        boolean firstColumn = true;
        for (ColumnSchema col : columns) {
            if (this.column != null && !col.name().equals(this.column)) {
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
        List<String[]> rows = new ArrayList<>();
        List<Integer> separatorsBefore = new ArrayList<>();
        long totalCompressed = 0;
        long totalDataValues = 0;
        String columnLabel = col.name();

        for (int rgIdx = 0; rgIdx < rowGroups.size(); rgIdx++) {
            RowGroup rg = rowGroups.get(rgIdx);
            ColumnChunk chunk = rg.columns().get(col.columnIndex());
            columnLabel = Sizes.columnPath(chunk.metaData());

            List<PageInfo> pages = collectPageHeaders(chunk.metaData(), inputFile);
            if (rgIdx > 0 && !pages.isEmpty()) {
                separatorsBefore.add(rows.size());
            }
            for (int i = 0; i < pages.size(); i++) {
                PageInfo p = pages.get(i);
                rows.add(new String[]{
                        i == 0 ? String.valueOf(rgIdx) : "",
                        p.label(),
                        p.type(),
                        p.encoding(),
                        Sizes.format(p.compressedSize()),
                        String.valueOf(p.numValues())
                });
                totalCompressed += p.compressedSize();
                if (!p.isDictionary()) {
                    totalDataValues += p.numValues();
                }
            }
        }

        int totalRowIdx = rows.size();
        rows.add(new String[]{
                "",
                "Total",
                "",
                "",
                Sizes.format(totalCompressed),
                String.valueOf(totalDataValues)
        });

        spec.commandLine().getOut().println(columnLabel);
        spec.commandLine().getOut().println(RowTable.renderTable(HEADERS, rows, separatorsBefore, List.of(totalRowIdx)));
    }

    private record PageInfo(String label, String type, String encoding, long compressedSize, long numValues, boolean isDictionary) {
    }

    private List<PageInfo> collectPageHeaders(ColumnMetaData cmd, InputFile inputFile) throws IOException {
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
            rows.add(new PageInfo(
                    label,
                    header.type().toString(),
                    pageEncoding(header),
                    header.compressedPageSize(),
                    numValues(header),
                    isDictionary
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

    private static String pageEncoding(PageHeader header) {
        return switch (header.type()) {
            case DATA_PAGE -> header.dataPageHeader().encoding().name();
            case DATA_PAGE_V2 -> header.dataPageHeaderV2().encoding().name();
            case DICTIONARY_PAGE -> header.dictionaryPageHeader().encoding().name();
            case INDEX_PAGE -> "N/A";
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
