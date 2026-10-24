/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.fetch;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

import dev.hardwood.InMemoryFiles;
import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.thrift.PageHeaderReader;
import dev.hardwood.internal.thrift.PageHeaderWriter;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.internal.thrift.ThriftCompactWriter;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// An indexed read takes a chunk's dictionary page from the bytes between the chunk's start and
/// its first data page as the OffsetIndex lists it. A dictionary page header claiming more bytes
/// than that region holds is a malformed file, and fails as one.
class IndexedDictionaryOverrunTest {

    @Test
    void aDictionaryPageClaimingMoreThanItsRegionFails() throws Exception {
        byte[] file = dictionaryEncodedFile();
        ColumnChunk chunk;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)))) {
            chunk = reader.getFileMetaData().rowGroups().get(0).columns().get(0);
        }
        assertThat(chunk.offsetIndexOffset()).isNotNull();
        int dictionaryStart = Math.toIntExact(chunk.metaData().dictionaryPageOffset());
        int region = Math.toIntExact(chunk.metaData().dataPageOffset() - dictionaryStart);
        claimOneMoreBodyByte(file, dictionaryStart);

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)));
                ColumnReader column = reader.columnReader("s")) {
            assertThatThrownBy(column::nextBatch)
                    .isExactlyInstanceOf(ParquetReadException.class)
                    .hasMessage("[<memory>: row group 0, column 's', dictionary page] Malformed Parquet"
                            + " metadata: the dictionary page header declares " + (region + 1)
                            + " bytes but only " + region + " bytes lie before the first data page");
        }
    }

    /// One row group whose one column is dictionary encoded.
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

    /// Rewrites the dictionary page header at `offset` in place to claim one more body byte than
    /// the page has.
    private static void claimOneMoreBodyByte(byte[] file, int offset) {
        ThriftCompactReader reader = new ThriftCompactReader(ByteBuffer.wrap(file, offset, file.length - offset).slice());
        PageHeader header = PageHeaderReader.read(reader);
        int headerLength = reader.getBytesRead();
        ThriftCompactWriter writer = new ThriftCompactWriter();
        PageHeaderWriter.writeDictionaryPageV1(writer, header.dictionaryPageHeader().numValues(),
                header.uncompressedPageSize(), header.compressedPageSize() + 1, header.crc(), Encoding.PLAIN);
        byte[] rewritten = writer.toByteArray();
        assertThat(rewritten).hasSize(headerLength);
        System.arraycopy(rewritten, 0, file, offset, headerLength);
    }
}
