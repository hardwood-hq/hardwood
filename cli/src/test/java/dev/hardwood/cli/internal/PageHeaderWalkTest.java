/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.internal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.reader.PageFormatProbe;
import dev.hardwood.internal.thrift.ThriftCompactConstants;
import dev.hardwood.internal.thrift.ThriftCompactWriter;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetReadException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PageHeaderWalkTest {

    /// One row group, 10 000 rows; column 1 (`category`) is dictionary-encoded, with several
    /// data pages.
    private static final Path FIXTURE = Path.of(PageHeaderWalkTest.class
            .getResource("/column_index_pushdown_dict.parquet").getPath());

    /// A window smaller than a page makes headers straddle window ends and bodies span several
    /// windows; the walk must find the same headers as one read of the whole chunk.
    @Test
    void walkingInWindowsFindsTheSameHeadersAsOneRead() throws IOException {
        try (InputFile file = InputFile.of(FIXTURE)) {
            file.open();
            ColumnMetaData metaData = metaData(file);
            int wholeChunk = Math.toIntExact(metaData.totalCompressedSize());

            List<PageHeader> whole = walk(file, metaData, wholeChunk);
            List<PageHeader> windowed = walk(file, metaData, 16);

            assertThat(whole).hasSizeGreaterThan(2);
            assertThat(shapes(windowed)).isEqualTo(shapes(whole));
        }
    }

    @Test
    void theVisitorStopsTheWalk() throws IOException {
        try (InputFile file = InputFile.of(FIXTURE)) {
            file.open();
            ColumnMetaData metaData = metaData(file);
            List<PageHeader> visited = new ArrayList<>();

            PageHeaderWalk.walk(file, metaData.dictionaryPageOffset(), metaData.totalCompressedSize(), 16,
                    header -> {
                        visited.add(header);
                        return visited.size() < 2;
                    });

            assertThat(visited).hasSize(2);
        }
    }

    /// A header too long for its window widens the window, but not without bound: one running
    /// past [PageFormatProbe#MAX_PEEK_SIZE], as a corrupt length makes one, fails as a corrupt
    /// file rather than being read to the end of a chunk that may span gigabytes.
    @Test
    void aHeaderLongerThanTheMaximumPeekSizeFails() throws IOException {
        int rangeSize = 3 * PageFormatProbe.MAX_PEEK_SIZE;
        ByteBuffer bytes = ByteBuffer.allocate(rangeSize);
        bytes.put(oversizedPageHeader(2 * PageFormatProbe.MAX_PEEK_SIZE)).rewind();
        try (InputFile file = InputFile.of(bytes)) {
            file.open();

            assertThatThrownBy(() -> PageHeaderWalk.walk(file, 0, rangeSize, 16, header -> true))
                    .isExactlyInstanceOf(ParquetReadException.class)
                    .hasMessage("Page header at offset 0 exceeds maximum peek size ("
                            + PageFormatProbe.MAX_PEEK_SIZE + " bytes)");
        }
    }

    /// A data page header carrying an unknown binary field of `padding` bytes, which a reader
    /// skips but has to have in its buffer to skip.
    private static byte[] oversizedPageHeader(int padding) {
        ThriftCompactWriter writer = new ThriftCompactWriter();
        writer.writeFieldBegin(1, ThriftCompactConstants.FieldType.I32);
        writer.writeI32(0); // Thrift PageType.DATA_PAGE
        writer.writeFieldBegin(2, ThriftCompactConstants.FieldType.I32);
        writer.writeI32(0);
        writer.writeFieldBegin(3, ThriftCompactConstants.FieldType.I32);
        writer.writeI32(0);
        writer.writeFieldBegin(99, ThriftCompactConstants.FieldType.BINARY);
        writer.writeBinary(new byte[padding]);
        writer.writeFieldStop();
        return writer.toByteArray();
    }

    private static List<PageHeader> walk(InputFile file, ColumnMetaData metaData, int windowBytes)
            throws IOException {
        List<PageHeader> headers = new ArrayList<>();
        PageHeaderWalk.walk(file, metaData.dictionaryPageOffset(), metaData.totalCompressedSize(), windowBytes,
                headers::add);
        return headers;
    }

    private static ColumnMetaData metaData(InputFile file) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE))) {
            return reader.getFileMetaData().rowGroups().getFirst().columns().get(1).metaData();
        }
    }

    /// Headers carry statistics as `byte[]`, which records compare by identity.
    private static List<String> shapes(List<PageHeader> headers) {
        return headers.stream()
                .map(h -> h.type() + " " + h.compressedPageSize() + "/" + h.uncompressedPageSize())
                .toList();
    }
}
