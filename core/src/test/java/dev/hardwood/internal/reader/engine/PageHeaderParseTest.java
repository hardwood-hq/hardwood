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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import dev.hardwood.InMemoryFiles;
import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.internal.io.CountingInputFile;
import dev.hardwood.internal.reader.decode.PageFormatProbe;
import dev.hardwood.internal.reader.fetch.FileRange;
import dev.hardwood.internal.reader.fetch.TailedInputFile;
import dev.hardwood.internal.reader.footer.ParquetMetadataReader;
import dev.hardwood.internal.thrift.OffsetIndexReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.PageLocation;
import dev.hardwood.metadata.PageType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ColumnEncoding;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.RowWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A page's header, parsed outside the read's lock wherever its bytes come from (READ_PIPELINE.md,
/// Decoded columns): a header that does not parse fails the read at its page, as its decode
/// reports it, and a header that crosses the start of the file's tail is read across it.
class PageHeaderParseTest {

    private static final int ROWS = 100_000;
    private static final int PAGE_ROWS = 10_000;
    private static final int CORRUPT_PAGE = 3;

    /// Where the read takes a page's bytes, and so where its header is parsed.
    enum Source {
        /// a file in memory, whose planned pages' headers the plan job parses
        HELD,
        /// a file read by request, whose planned pages' headers are parsed as each request arrives
        FETCHED
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

    /// The header of a page that starts in a fetched range and runs into the tail is read from
    /// both: the range's bytes before the tail's start, the tail's after it.
    @Test
    void aHeaderPastAFetchedRangeIsReadFromTheTail() throws Exception {
        byte[] bytes = write();
        PageLocation last = pages(bytes).getLast();
        long tailStart = last.offset() + 2;
        System.setProperty(TailedInputFile.TAIL_BYTES_PROPERTY, Long.toString(bytes.length - tailStart));
        CountingInputFile delegate = new CountingInputFile(ByteBuffer.wrap(bytes));
        delegate.open();
        InputFile file = TailedInputFile.take(delegate);
        FileRange range = new FileRange(last.offset(), 2, "test");
        ByteBuffer fetched = ByteBuffer.wrap(bytes, Math.toIntExact(last.offset()), 2).slice();

        PageFormatProbe.HeaderAt header = TrackedPage.readHeader(
                (offset, length) -> RangeFetch.read(file, range, fetched, offset, length), last.offset(),
                last.compressedPageSize());

        assertThat(header).isNotNull();
        assertThat(header.header().type()).isEqualTo(PageType.DATA_PAGE);
        assertThat(header.length() + header.header().compressedPageSize()).isEqualTo(last.compressedPageSize());
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
        FileSchema schema = FileSchema.builder("sequence").addColumn("a", PhysicalType.INT64, RepetitionType.REQUIRED)
                .build();
        WriterConfig config = WriterConfig.builder()
                .codec(CompressionCodec.UNCOMPRESSED)
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
