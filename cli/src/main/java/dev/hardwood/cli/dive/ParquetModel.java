/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.dive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import dev.hardwood.InputFile;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;

/// Snapshot of a Parquet file exposed to the dive screens.
///
/// Opens the file eagerly at construction, reading the footer and schema so any I/O
/// error surfaces before the TUI enters raw mode. The underlying [ParquetFileReader]
/// is held open for the session and closed via [AutoCloseable]. Aggregate facts
/// (compressed/uncompressed totals, ratio) are computed once and cached in [Facts].
public final class ParquetModel implements AutoCloseable {

    private final String displayPath;
    private final long fileSizeBytes;
    private final ParquetFileReader reader;
    private final FileMetaData metadata;
    private final FileSchema schema;
    private final Facts facts;

    private ParquetModel(String displayPath, long fileSizeBytes, ParquetFileReader reader) {
        this.displayPath = displayPath;
        this.fileSizeBytes = fileSizeBytes;
        this.reader = reader;
        this.metadata = reader.getFileMetaData();
        this.schema = reader.getFileSchema();
        this.facts = computeFacts();
    }

    public static ParquetModel open(InputFile inputFile, String displayPath) throws IOException {
        ParquetFileReader reader = ParquetFileReader.open(inputFile);
        try {
            return new ParquetModel(displayPath, inputFile.length(), reader);
        }
        catch (RuntimeException e) {
            reader.close();
            throw e;
        }
    }

    public String displayPath() {
        return displayPath;
    }

    public long fileSizeBytes() {
        return fileSizeBytes;
    }

    public FileMetaData metadata() {
        return metadata;
    }

    public FileSchema schema() {
        return schema;
    }

    public Facts facts() {
        return facts;
    }

    public int rowGroupCount() {
        return metadata.rowGroups().size();
    }

    public int columnCount() {
        return schema.getColumnCount();
    }

    public RowGroup rowGroup(int index) {
        return metadata.rowGroups().get(index);
    }

    public ColumnChunk chunk(int rowGroupIndex, int columnIndex) {
        return rowGroup(rowGroupIndex).columns().get(columnIndex);
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    private Facts computeFacts() {
        long compressed = 0;
        long uncompressed = 0;
        for (RowGroup rg : metadata.rowGroups()) {
            for (ColumnChunk cc : rg.columns()) {
                ColumnMetaData cmd = cc.metaData();
                compressed += cmd.totalCompressedSize();
                uncompressed += cmd.totalUncompressedSize();
            }
        }
        double ratio = compressed == 0 ? 0.0 : (double) uncompressed / compressed;
        Map<String, String> kv = metadata.keyValueMetadata();
        List<Map.Entry<String, String>> kvList = kv == null ? List.of() : new ArrayList<>(kv.entrySet());
        return new Facts(
                metadata.version(),
                metadata.createdBy(),
                metadata.numRows(),
                metadata.rowGroups().size(),
                schema.getColumnCount(),
                compressed,
                uncompressed,
                ratio,
                List.copyOf(kvList));
    }

    /// Pre-aggregated file-level facts. Everything here is cheap to display; derived
    /// from the metadata at model construction time so screens don't recompute.
    public record Facts(
            int formatVersion,
            String createdBy,
            long totalRows,
            int rowGroupCount,
            int columnCount,
            long compressedBytes,
            long uncompressedBytes,
            double compressionRatio,
            List<Map.Entry<String, String>> keyValueMetadata) {
    }
}
