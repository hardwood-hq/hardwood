/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.plan;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;

import org.junit.jupiter.api.Test;

import dev.hardwood.InMemoryFiles;
import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.internal.thrift.FooterRewriter;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A column chunk the footer places outside the file is rejected as such by whichever stage of a
/// read reaches it first: here the dictionary a filter prunes with, which planning reads before
/// it builds the fetch plans.
class ChunkOutsideFileTest {

    @Test
    void aChunkPastTheFileIsRejectedBeforeItsDictionaryIsRead() throws Exception {
        byte[] bytes = dictionaryEncodedFile();
        long fileLength;
        byte[] moved;
        ColumnChunk chunk;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(bytes)))) {
            chunk = reader.getFileMetaData().rowGroups().get(0).columns().get(0);
        }
        moved = FooterRewriter.rewrite(bytes, shifted(bytes.length));
        fileLength = moved.length;
        long start = chunk.chunkStartOffset() + bytes.length;
        long end = start + chunk.metaData().totalCompressedSize();

        assertThatThrownBy(() -> {
            try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(moved)));
                    RowReader rows = reader.buildRowReader().filter(FilterPredicate.eq("s", "v3")).build()) {
                while (rows.hasNext()) {
                    rows.next();
                }
            }
        }).isExactlyInstanceOf(ParquetReadException.class)
                .hasMessage("[<memory>: row group 0, column 's'] The column chunk [" + start + ", " + end
                        + ") lies outside the file of " + fileLength + " bytes");
    }

    /// A chunk stored in another file has offsets into that file, which may lie past the end of
    /// this one: it is rejected as stored elsewhere, not as lying outside this file.
    @Test
    void aChunkInAnotherFileIsRejectedAsSuchBeforeItsExtentIsChecked() throws Exception {
        byte[] bytes = dictionaryEncodedFile();
        byte[] moved = FooterRewriter.rewrite(bytes, inAnotherFile(bytes.length, "other.parquet"));

        assertThatThrownBy(() -> {
            try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(moved)));
                    RowReader rows = reader.rowReader()) {
                while (rows.hasNext()) {
                    rows.next();
                }
            }
        }).isExactlyInstanceOf(UnsupportedOperationException.class)
                .hasMessage("[<memory>] Cannot read column 0 in row group 0: Column chunk stores its data in a"
                        + " separate file ('other.parquet'); the split-file layout is not supported");
    }

    /// One row group whose one column is dictionary encoded and holds `v0` to `v9`, so its
    /// statistics cannot decide `s = 'v3'`.
    private static byte[] dictionaryEncodedFile() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("s", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED, new LogicalType.StringType())
                .build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            for (int i = 0; i < 1000; i++) {
                String value = "v" + (i % 10);
                writer.rowWriter().writeRow(row -> row.setString("s", value));
            }
        }
        return InMemoryFiles.toByteArray(out);
    }

    /// A footer patch moving the first column chunk of every row group `by` bytes later.
    private static UnaryOperator<FileMetaData> shifted(long by) {
        return inAnotherFile(by, "");
    }

    /// As [#shifted], the chunk also stored in the file `filePath`, or in this file where it is
    /// empty.
    private static UnaryOperator<FileMetaData> inAnotherFile(long by, String filePath) {
        return metaData -> new FileMetaData(metaData.version(), metaData.schema(), metaData.numRows(),
                metaData.rowGroups().stream().map(rowGroup -> {
                    List<ColumnChunk> columns = new ArrayList<>(rowGroup.columns());
                    ColumnChunk chunk = columns.get(0);
                    ColumnMetaData m = chunk.metaData();
                    columns.set(0, new ColumnChunk(new ColumnMetaData(m.type(), m.encodings(), m.pathInSchema(),
                            m.codec(), m.numValues(), m.totalUncompressedSize(), m.totalCompressedSize(),
                            m.keyValueMetadata(), m.dataPageOffset() + by,
                            m.dictionaryPageOffset() == null ? null : m.dictionaryPageOffset() + by, m.statistics(),
                            m.geospatialStatistics(), m.bloomFilterOffset(), m.bloomFilterLength(),
                            m.encodingStats(), m.sizeStatistics()),
                            chunk.offsetIndexOffset(), chunk.offsetIndexLength(), chunk.columnIndexOffset(),
                            chunk.columnIndexLength(), filePath));
                    return new RowGroup(columns, rowGroup.totalByteSize(), rowGroup.numRows());
                }).toList(),
                metaData.keyValueMetadata(), metaData.createdBy(), metaData.columnOrders());
    }
}
