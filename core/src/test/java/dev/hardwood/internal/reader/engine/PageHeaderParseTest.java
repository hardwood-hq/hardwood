/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.UnaryOperator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import dev.hardwood.InMemoryFiles;
import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.internal.io.CountingInputFile;
import dev.hardwood.internal.reader.decode.PageFormatProbe;
import dev.hardwood.internal.reader.fetch.TailedInputFile;
import dev.hardwood.internal.reader.footer.ParquetMetadataReader;
import dev.hardwood.internal.thrift.FooterRewriter;
import dev.hardwood.internal.thrift.OffsetIndexReader;
import dev.hardwood.internal.thrift.PlainInt64File;
import dev.hardwood.internal.thrift.ThriftCompactConstants;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.internal.thrift.ThriftCompactWriter;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.PageLocation;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ColumnEncoding;
import dev.hardwood.writer.Compression;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.RowWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A page's header wherever its bytes come from (VALUE_DECODE.md, Page headers): a header that
/// does not parse fails the read at its page, as its decode reports it, and a header that crosses
/// the start of the file's tail is read across it.
class PageHeaderParseTest {

    private static final int ROWS = 100_000;
    private static final int PAGE_ROWS = 10_000;
    private static final int CORRUPT_PAGE = 3;

    /// Where the read takes a page's bytes from, and how it finds the page.
    enum Source {
        /// a file in memory, whose pages its OffsetIndex lists
        HELD,
        /// a file read by request, whose pages its OffsetIndex lists
        FETCHED,
        /// a file without an OffsetIndex, whose pages a scan finds
        SCANNED
    }

    /// the tail size the test JVM reads with, which the tests that set their own restore
    private String tailBytes;

    @BeforeEach
    void saveTailBytes() {
        tailBytes = System.getProperty(TailedInputFile.TAIL_BYTES_PROPERTY);
    }

    @AfterEach
    void restoreTailBytes() {
        if (tailBytes == null) {
            System.clearProperty(TailedInputFile.TAIL_BYTES_PROPERTY);
        }
        else {
            System.setProperty(TailedInputFile.TAIL_BYTES_PROPERTY, tailBytes);
        }
    }

    @ParameterizedTest
    @EnumSource(Source.class)
    void aHeaderThatDoesNotParseFailsTheReadAtItsPage(Source source) throws Exception {
        byte[] bytes = write();
        PageLocation corrupt = pages(bytes).get(CORRUPT_PAGE);
        // the page type, the header's first field, becomes a varint no page type reads as
        bytes[Math.toIntExact(corrupt.offset() + 1)] = (byte) 0xFF;
        InputFile file = switch (source) {
            case HELD -> InputFile.of(ByteBuffer.wrap(bytes));
            case FETCHED -> new CountingInputFile(ByteBuffer.wrap(bytes));
            case SCANNED -> new CountingInputFile(ByteBuffer.wrap(FooterRewriter.withoutPageIndex(bytes)));
        };
        long[] read = new long[1];

        assertThatThrownBy(() -> {
            try (ParquetFileReader reader = ParquetFileReader.open(file);
                    ColumnReader column = reader.columnReader("a")) {
                while (column.nextBatch()) {
                    read[0] += column.getRecordCount();
                }
            }
        }).isInstanceOf(ParquetReadException.class)
                .hasMessage("[<memory>: row group 0, column 'a', page " + CORRUPT_PAGE
                        + "] PageHeader has unknown page type: -1408");
        assertThat(read[0]).as("no row of the page or after it is delivered")
                .isLessThanOrEqualTo(corrupt.firstRowIndex());
    }

    /// A header that runs past the end of its page, as its OffsetIndex sizes it, fails the read at
    /// the page in the words every reader uses, naming the page as its extent: an unknown binary
    /// field at the header's start declares more bytes than the page holds.
    @ParameterizedTest
    @EnumSource(value = Source.class, names = { "HELD", "FETCHED" })
    void aPlannedHeaderPastItsPageFailsTheReadAtItsPage(Source source) throws Exception {
        byte[] bytes = write();
        PageLocation corrupt = pages(bytes).get(CORRUPT_PAGE);
        int at = Math.toIntExact(corrupt.offset());
        // field 15, binary, declaring 256 KiB
        bytes[at] = (byte) 0xF8;
        bytes[at + 1] = (byte) 0x80;
        bytes[at + 2] = (byte) 0x80;
        bytes[at + 3] = (byte) 0x10;
        InputFile file = source == Source.HELD ? InputFile.of(ByteBuffer.wrap(bytes))
                : new CountingInputFile(ByteBuffer.wrap(bytes));

        assertThatThrownBy(() -> {
            try (ParquetFileReader reader = ParquetFileReader.open(file);
                    ColumnReader column = reader.columnReader("a")) {
                while (column.nextBatch()) {
                    // reads up to the failure
                }
            }
        }).isInstanceOf(ParquetReadException.class)
                .hasMessage("[<memory>: row group 0, column 'a', page " + CORRUPT_PAGE + "] Page header at offset "
                        + corrupt.offset() + " does not fit in the " + corrupt.compressedPageSize()
                        + " bytes left in its page — the file is likely corrupt");
    }

    /// A page its OffsetIndex lists whose header is padded past a mebibyte, as long inline
    /// statistics make one, is read whole, from a file in memory and by request alike: the header is
    /// bounded by its page alone.
    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void aPlannedHeaderOfAnySizeWithinItsPageIsRead(boolean held) throws Exception {
        byte[] bytes = PlainInt64File.build(List.of(new PlainInt64File.Page(0, PAGE_ROWS),
                new PlainInt64File.Page(PAGE_ROWS, PAGE_ROWS, null, 2 * 1024 * 1024),
                new PlainInt64File.Page(2L * PAGE_ROWS, PAGE_ROWS)), UnaryOperator.identity());
        InputFile file = held ? InputFile.of(ByteBuffer.wrap(bytes)) : new CountingInputFile(ByteBuffer.wrap(bytes));

        long sum = 0;
        long rows = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(file);
                ColumnReader column = reader.columnReader("v")) {
            while (column.nextBatch()) {
                long[] values = column.getLongs();
                for (int i = 0; i < column.getRecordCount(); i++) {
                    sum += values[i];
                }
                rows += column.getRecordCount();
            }
        }

        long expected = 3L * PAGE_ROWS;
        assertThat(rows).isEqualTo(expected);
        assertThat(sum).isEqualTo(expected * (expected - 1) / 2);
    }

    /// A scanned chunk whose page header is padded past a mebibyte, as long inline statistics
    /// make one, is read whole: the header is bounded by its chunk alone, from a file in memory and
    /// by request alike.
    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void aScannedHeaderOfAnySizeWithinItsChunkIsRead(boolean held) throws Exception {
        byte[] bytes = withPaddedHeader(write(), CORRUPT_PAGE, 2 * 1024 * 1024);
        InputFile file = held ? InputFile.of(ByteBuffer.wrap(bytes)) : new CountingInputFile(ByteBuffer.wrap(bytes));

        long sum = 0;
        long rows = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(file);
                ColumnReader column = reader.columnReader("a")) {
            while (column.nextBatch()) {
                long[] values = column.getLongs();
                for (int i = 0; i < column.getRecordCount(); i++) {
                    sum += values[i];
                }
                rows += column.getRecordCount();
            }
        }

        assertThat(rows).isEqualTo(ROWS);
        assertThat(sum).isEqualTo((long) ROWS * (ROWS - 1) / 2);
    }

    /// The tail starts two bytes into the last page's header: the page is fetched up to there and
    /// sliced from the tail after it, and the read returns every row.
    @Test
    void aPageWhoseHeaderCrossesTheTailsStartIsRead() throws Exception {
        byte[] bytes = write();
        PageLocation last = pages(bytes).getLast();
        long tailStart = last.offset() + 2;
        System.setProperty(TailedInputFile.TAIL_BYTES_PROPERTY, Long.toString(bytes.length - tailStart));
        CountingInputFile file = new CountingInputFile(ByteBuffer.wrap(bytes));

        long sum = 0;
        long rows = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(file);
                ColumnReader column = reader.columnReader("a")) {
            while (column.nextBatch()) {
                long[] values = column.getLongs();
                for (int i = 0; i < column.getRecordCount(); i++) {
                    sum += values[i];
                }
                rows += column.getRecordCount();
            }
        }

        assertThat(rows).isEqualTo(ROWS);
        assertThat(sum).isEqualTo((long) ROWS * (ROWS - 1) / 2);
        assertThat(file.reads()).as("the page is fetched up to the tail's start")
                .anySatisfy(r -> assertThat(r.end()).isEqualTo(tailStart));
    }

    /// `file` without its page index, the header of its page `page` carrying an unknown binary
    /// field of `padding` bytes, which a reader skips, and its chunk grown by as much.
    private static byte[] withPaddedHeader(byte[] file, int page, int padding) throws Exception {
        PageLocation location = pages(file).get(page);
        int offset = Math.toIntExact(location.offset());
        int headerLength = PageFormatProbe.readHeader((o, length) -> ByteBuffer.wrap(file, Math.toIntExact(o), length).slice(),
                location.offset(), location.compressedPageSize(), PageFormatProbe.Extent.PAGE,
                PageFormatProbe.INITIAL_PEEK_SIZE, "").value().length();
        ThriftCompactWriter field = new ThriftCompactWriter();
        field.writeFieldBegin(99, ThriftCompactConstants.FieldType.BINARY);
        field.writeBinary(new byte[padding]);
        byte[] added = field.toByteArray();
        // the field goes in before the header's closing stop byte
        int insertAt = offset + headerLength - 1;
        byte[] spliced = new byte[file.length + added.length];
        System.arraycopy(file, 0, spliced, 0, insertAt);
        System.arraycopy(added, 0, spliced, insertAt, added.length);
        System.arraycopy(file, insertAt, spliced, insertAt + added.length, file.length - insertAt);
        return FooterRewriter.rewrite(FooterRewriter.withoutPageIndex(spliced), metaData -> new FileMetaData(
                metaData.version(), metaData.schema(), metaData.numRows(), metaData.rowGroups().stream().map(rowGroup -> {
                    ColumnChunk chunk = rowGroup.columns().getFirst();
                    ColumnMetaData m = chunk.metaData();
                    ColumnChunk grown = new ColumnChunk(new ColumnMetaData(m.type(), m.encodings(), m.pathInSchema(),
                            m.codec(), m.numValues(), m.totalUncompressedSize() + added.length,
                            m.totalCompressedSize() + added.length, m.keyValueMetadata(), m.dataPageOffset(),
                            m.dictionaryPageOffset(), m.statistics(), m.geospatialStatistics(), m.bloomFilterOffset(),
                            m.bloomFilterLength(), m.encodingStats(), m.sizeStatistics()),
                            chunk.offsetIndexOffset(), chunk.offsetIndexLength(), chunk.columnIndexOffset(),
                            chunk.columnIndexLength(), chunk.filePath());
                    return new RowGroup(List.of(grown), rowGroup.totalByteSize(), rowGroup.numRows());
                }).toList(), metaData.keyValueMetadata(), metaData.createdBy(), metaData.columnOrders()));
    }

    /// The pages of column `a`, from its OffsetIndex.
    private static List<PageLocation> pages(byte[] bytes) throws Exception {
        ColumnChunk chunk = ParquetMetadataReader.readMetadata(InputFile.of(ByteBuffer.wrap(bytes)))
                .rowGroups().getFirst().columns().getFirst();
        List<PageLocation> pages = OffsetIndexReader.read(new ThriftCompactReader(
                ByteBuffer.wrap(bytes, Math.toIntExact(chunk.offsetIndexOffset()), chunk.offsetIndexLength()).slice()))
                .pageLocations();
        assertThat(pages).hasSize(ROWS / PAGE_ROWS);
        return pages;
    }

    /// One row group of a required, plain, uncompressed `INT64` column `a` holding each row's index,
    /// in pages of [#PAGE_ROWS] rows.
    private static byte[] write() throws Exception {
        FileSchema schema = FileSchema.builder("sequence").primitive("a", PhysicalType.INT64, RepetitionType.REQUIRED)
                .build();
        WriterConfig config = WriterConfig.builder()
                .compression(Compression.uncompressed())
                .encoding(ColumnEncoding.PLAIN)
                .pageTargetRows(PAGE_ROWS)
                .build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, config)) {
            RowWriter rowWriter = writer.rowWriter();
            for (int i = 0; i < ROWS; i++) {
                long value = i;
                rowWriter.writeRow(row -> row.setLong("a", value));
            }
        }
        return InMemoryFiles.toByteArray(out);
    }
}
