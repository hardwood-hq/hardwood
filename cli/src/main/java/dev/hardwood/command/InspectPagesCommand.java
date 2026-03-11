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
import java.util.List;
import java.util.concurrent.Callable;

import dev.hardwood.InputFile;
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
    @CommandLine.Option(names = "--column", paramLabel = "COLUMN", description = "Restrict output to a single column.")
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

        if (column != null) {
            try {
                schema.getColumn(column);
            }
            catch (IllegalArgumentException e) {
                spec.commandLine().getErr().println("Unknown column: " + column);
                return CommandLine.ExitCode.SOFTWARE;
            }
        }

        InputFile inputFile = fileMixin.toInputFile();
        try {
            inputFile.open();
            printPages(metadata, schema, inputFile);
        }
        catch (IOException e) {
            spec.commandLine().getErr().println("Error reading pages: " + e.getMessage());
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

    private void printPages(FileMetaData metadata, FileSchema schema, InputFile inputFile) throws IOException {
        List<RowGroup> rowGroups = metadata.rowGroups();
        List<ColumnSchema> columns = schema.getColumns();

        for (int rgIdx = 0; rgIdx < rowGroups.size(); rgIdx++) {
            RowGroup rg = rowGroups.get(rgIdx);
            for (ColumnSchema col : columns) {
                if (column != null && !col.name().equals(column)) {
                    continue;
                }
                ColumnChunk chunk = rg.columns().get(col.columnIndex());
                spec.commandLine().getOut().printf("Row Group %d / %s%n", rgIdx, Sizes.columnPath(chunk.metaData()));
                scanPageHeaders(chunk.metaData(), inputFile);
                spec.commandLine().getOut().println();
            }
        }
    }

    private void scanPageHeaders(ColumnMetaData cmd, InputFile inputFile) throws IOException {
        Long dictOffset = cmd.dictionaryPageOffset();
        long chunkStart = (dictOffset != null && dictOffset > 0) ? dictOffset : cmd.dataPageOffset();
        long chunkSize = cmd.totalCompressedSize();

        ByteBuffer buffer = inputFile.readRange(chunkStart, (int) chunkSize);

        int pageIndex = 0;
        long valuesRead = 0;
        int position = 0;

        while (position < buffer.limit()) {
            ThriftCompactReader headerReader = new ThriftCompactReader(buffer, position);
            PageHeader header = PageHeaderReader.read(headerReader);
            int headerSize = headerReader.getBytesRead();

            printPageHeader(pageIndex, header);

            if (header.type() == PageHeader.PageType.DATA_PAGE || header.type() == PageHeader.PageType.DATA_PAGE_V2) {
                valuesRead += numValues(header);
                pageIndex++;
                if (valuesRead >= cmd.numValues()) {
                    break;
                }
            }

            position += headerSize + header.compressedPageSize();
        }
    }

    private void printPageHeader(int index, PageHeader header) {
        String label = header.type() == PageHeader.PageType.DICTIONARY_PAGE ? " dict" : String.format("[%3d]", index);
        String encoding = pageEncoding(header);
        int numValues = numValues(header);
        spec.commandLine().getOut().printf("  %s  %-18s  encoding: %-22s  compressed: %-10s  values: %d%n",
                label,
                header.type(),
                encoding,
                Sizes.format(header.compressedPageSize()),
                numValues);
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
