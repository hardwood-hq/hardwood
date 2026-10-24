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
import java.util.function.BiFunction;
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
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A region the footer places outside the file (a column chunk, a page index or a bloom filter) is
/// rejected as such before any read of it: a chunk by whichever stage of a read reaches it first,
/// here the dictionary a filter prunes with; a page index or bloom filter as the read plans the
/// window that fetches it.
class OutsideFileTest {

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

        assertThatThrownBy(() -> readAll(moved, FilterPredicate.eq("s", "v3"))).isExactlyInstanceOf(ParquetReadException.class)
                .hasMessage("[<memory>: row group 0, column 's'] The column chunk [" + start + ", " + end
                        + ") lies outside the file of " + fileLength + " bytes");
    }

    /// A chunk stored in another file has offsets into that file, which may lie past the end of
    /// this one: it is rejected as stored elsewhere, not as lying outside this file.
    @Test
    void aChunkInAnotherFileIsRejectedAsSuchBeforeItsExtentIsChecked() throws Exception {
        byte[] bytes = dictionaryEncodedFile();
        byte[] moved = FooterRewriter.rewrite(bytes, inAnotherFile(bytes.length, "other.parquet"));

        assertThatThrownBy(() -> readAll(moved, null)).isExactlyInstanceOf(UnsupportedOperationException.class)
                .hasMessage("[<memory>] Cannot read column 0 in row group 0: Column chunk stores its data in a"
                        + " separate file ('other.parquet'); the split-file layout is not supported");
    }

    @Test
    void aBloomFilterPastTheFileIsRejectedBeforeItIsRead() throws Exception {
        byte[] bytes = dictionaryEncodedFile(WriterConfig.builder().bloomFilter("s").build());
        long offset = bytes.length + 10L;
        byte[] moved = FooterRewriter.rewrite(bytes, firstChunk((chunk, m) -> new ColumnChunk(
                metaData(m, 0, offset, m.bloomFilterLength()), chunk.offsetIndexOffset(),
                chunk.offsetIndexLength(), chunk.columnIndexOffset(), chunk.columnIndexLength(), chunk.filePath())));

        assertThatThrownBy(() -> readAll(moved, FilterPredicate.eq("s", "v3")))
                .isExactlyInstanceOf(ParquetReadException.class)
                .hasMessage("[<memory>: row group 0, column 's'] The bloom filter [" + offset + ", "
                        + (offset + bloomFilterLength(moved)) + ") lies outside the file of " + moved.length + " bytes");
    }

    /// A filter whose length the footer omits is not fetched with the window but read where a probe
    /// reaches it, so the probe rejects it.
    @Test
    void aBloomFilterOfUnknownLengthPastTheFileIsRejectedBeforeItIsRead() throws Exception {
        byte[] bytes = dictionaryEncodedFile(WriterConfig.builder().bloomFilter("s").build());
        long offset = bytes.length + 10L;
        byte[] moved = FooterRewriter.rewrite(bytes, firstChunk((chunk, m) -> new ColumnChunk(
                metaData(m, 0, offset, null), chunk.offsetIndexOffset(), chunk.offsetIndexLength(),
                chunk.columnIndexOffset(), chunk.columnIndexLength(), chunk.filePath())));

        assertThatThrownBy(() -> readAll(moved, FilterPredicate.eq("s", "v3")))
                .isExactlyInstanceOf(ParquetReadException.class)
                .hasMessage("[<memory>: column 's'] The bloom filter at offset " + offset
                        + " lies outside the file of " + moved.length + " bytes");
    }

    @Test
    void anOffsetIndexPastTheFileIsRejectedBeforeItIsRead() throws Exception {
        byte[] bytes = dictionaryEncodedFile(WriterConfig.builder().build());
        long offset = bytes.length + 10L;
        byte[] moved = FooterRewriter.rewrite(bytes, firstChunk((chunk, m) -> new ColumnChunk(m, offset, 20,
                chunk.columnIndexOffset(), chunk.columnIndexLength(), chunk.filePath())));

        assertThatThrownBy(() -> readAll(moved, null))
                .isExactlyInstanceOf(ParquetReadException.class)
                .hasMessage("[<memory>: row group 0, column 's'] The offset index [" + offset + ", " + (offset + 20)
                        + ") lies outside the file of " + moved.length + " bytes");
    }

    private static void readAll(byte[] bytes, FilterPredicate filter) throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(bytes)));
                RowReader rows = filter == null ? reader.rowReader() : reader.buildRowReader().filter(filter).build()) {
            while (rows.hasNext()) {
                rows.next();
            }
        }
    }

    private static int bloomFilterLength(byte[] bytes) throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(bytes)))) {
            return reader.getFileMetaData().rowGroups().get(0).columns().get(0).metaData().bloomFilterLength();
        }
    }

    /// One row group whose one column is dictionary encoded and holds `v0` to `v9`, so its
    /// statistics cannot decide `s = 'v3'`.
    private static byte[] dictionaryEncodedFile() throws Exception {
        return dictionaryEncodedFile(WriterConfig.builder().build());
    }

    private static byte[] dictionaryEncodedFile(WriterConfig config) throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("s", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED, new LogicalType.StringType())
                .build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, config)) {
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
        return firstChunk((chunk, m) -> new ColumnChunk(metaData(m, by, m.bloomFilterOffset(), m.bloomFilterLength()),
                chunk.offsetIndexOffset(), chunk.offsetIndexLength(), chunk.columnIndexOffset(),
                chunk.columnIndexLength(), filePath));
    }

    /// A footer patch replacing the first column chunk of every row group by what `patch` makes of
    /// it and its metadata.
    private static UnaryOperator<FileMetaData> firstChunk(BiFunction<ColumnChunk, ColumnMetaData, ColumnChunk> patch) {
        return metaData -> new FileMetaData(metaData.version(), metaData.schema(), metaData.numRows(),
                metaData.rowGroups().stream().map(rowGroup -> {
                    List<ColumnChunk> columns = new ArrayList<>(rowGroup.columns());
                    ColumnChunk chunk = columns.get(0);
                    columns.set(0, patch.apply(chunk, chunk.metaData()));
                    return new RowGroup(columns, rowGroup.totalByteSize(), rowGroup.numRows());
                }).toList(),
                metaData.keyValueMetadata(), metaData.createdBy(), metaData.columnOrders());
    }

    /// `m` with its pages moved `by` bytes later and its bloom filter at `bloomFilterOffset`.
    private static ColumnMetaData metaData(ColumnMetaData m, long by, Long bloomFilterOffset,
            Integer bloomFilterLength) {
        return new ColumnMetaData(m.type(), m.encodings(), m.pathInSchema(), m.codec(), m.numValues(),
                m.totalUncompressedSize(), m.totalCompressedSize(), m.keyValueMetadata(), m.dataPageOffset() + by,
                m.dictionaryPageOffset() == null ? null : m.dictionaryPageOffset() + by, m.statistics(),
                m.geospatialStatistics(), bloomFilterOffset, bloomFilterLength, m.encodingStats(),
                m.sizeStatistics());
    }
}
