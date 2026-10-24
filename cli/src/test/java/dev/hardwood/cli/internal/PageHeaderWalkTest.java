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
import dev.hardwood.cli.TestResources;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.reader.decode.PageFormatProbe;
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
    private static final Path FIXTURE = TestResources.path("/column_index_pushdown_dict.parquet");

    /// Larger than a header's first peek ([PageFormatProbe#INITIAL_PEEK_SIZE]), smaller than the
    /// fixture's chunk.
    private static final int WINDOW = 4 * PageFormatProbe.INITIAL_PEEK_SIZE;

    /// A window larger than a header's peek but smaller than the chunk serves several headers from
    /// one read and starts a new window at a header it does not hold; the walk finds the same
    /// headers as one read of the whole chunk, in reads no longer than a window.
    @Test
    void walkingInWindowsFindsTheSameHeadersAsOneRead() throws IOException {
        try (InputFile file = InputFile.of(FIXTURE)) {
            file.open();
            ColumnMetaData metaData = metaData(file);
            int wholeChunk = Math.toIntExact(metaData.totalCompressedSize());
            List<Integer> reads = new ArrayList<>();
            InputFile recording = new RecordingInputFile(file, reads);

            List<PageHeader> whole = walk(file, metaData, wholeChunk);
            List<PageHeader> windowed = walk(recording, metaData, WINDOW);

            assertThat(whole).hasSizeGreaterThan(2);
            assertThat(shapes(windowed)).isEqualTo(shapes(whole));
            assertThat(reads).as("read lengths").hasSizeBetween(2, whole.size() - 1)
                    .allSatisfy(length -> assertThat(length).isLessThanOrEqualTo(WINDOW));
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

    /// A header too long for its peek widens the peek, but not without bound: one running
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

    /// A page whose body runs past the range is a malformed chunk; the walk fails rather than
    /// handing on a page the chunk does not hold.
    @Test
    void aPageRunningPastTheRangeFails() throws IOException {
        byte[] header = pageHeader(100, 0);
        int rangeSize = header.length + 10;
        try (InputFile file = InputFile.of(ByteBuffer.allocate(rangeSize).put(header).rewind())) {
            file.open();
            List<PageHeader> visited = new ArrayList<>();

            assertThatThrownBy(() -> PageHeaderWalk.walk(file, 0, rangeSize, 16, visited::add))
                    .isExactlyInstanceOf(ParquetReadException.class)
                    .hasMessage("Page at offset 0 ends at offset " + (header.length + 100)
                            + ", past the end of the column chunk at offset " + rangeSize);
            assertThat(visited).isEmpty();
        }
    }

    /// A header cut off by the range's end is a truncated chunk, reported as the reader reports it.
    @Test
    void aHeaderCutOffByTheRangeEndFails() throws IOException {
        byte[] header = pageHeader(0, 40);
        int rangeSize = header.length - 2;
        try (InputFile file = InputFile.of(ByteBuffer.wrap(header, 0, rangeSize).slice())) {
            file.open();

            assertThatThrownBy(() -> PageHeaderWalk.walk(file, 0, rangeSize, 16, visited -> true))
                    .isExactlyInstanceOf(ParquetReadException.class)
                    .hasMessage("Page header at offset 0 does not fit in the " + rangeSize
                            + " bytes left in its column chunk — the file is likely corrupt");
        }
    }

    /// A data page header carrying an unknown binary field of `padding` bytes, which a reader
    /// skips but has to have in its buffer to skip.
    private static byte[] oversizedPageHeader(int padding) {
        return pageHeader(0, padding);
    }

    /// A data page header declaring a body of `compressedSize` bytes and carrying an unknown
    /// binary field of `padding` bytes.
    private static byte[] pageHeader(int compressedSize, int padding) {
        ThriftCompactWriter writer = new ThriftCompactWriter();
        writer.writeFieldBegin(1, ThriftCompactConstants.FieldType.I32);
        writer.writeI32(0); // Thrift PageType.DATA_PAGE
        writer.writeFieldBegin(2, ThriftCompactConstants.FieldType.I32);
        writer.writeI32(compressedSize);
        writer.writeFieldBegin(3, ThriftCompactConstants.FieldType.I32);
        writer.writeI32(compressedSize);
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

    /// Records the length of each read it hands on to `delegate`.
    private record RecordingInputFile(InputFile delegate, List<Integer> reads) implements InputFile {

        @Override
        public void open() throws IOException {
            delegate.open();
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            reads.add(length);
            return delegate.readRange(offset, length);
        }

        @Override
        public long length() throws IOException {
            return delegate.length();
        }

        @Override
        public String name() {
            return delegate.name();
        }

        @Override
        public void close() {
        }
    }

    /// Headers carry statistics as `byte[]`, which records compare by identity.
    private static List<String> shapes(List<PageHeader> headers) {
        return headers.stream()
                .map(h -> h.type() + " " + h.compressedPageSize() + "/" + h.uncompressedPageSize())
                .toList();
    }
}
