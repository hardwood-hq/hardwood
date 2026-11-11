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
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import dev.hardwood.InMemoryFiles;
import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.internal.io.CountingInputFile;
import dev.hardwood.internal.reader.fetch.CoalescingPolicy;
import dev.hardwood.internal.reader.footer.ParquetMetadataReader;
import dev.hardwood.internal.thrift.FooterRewriter;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ColumnEncoding;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.RowWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// The requests a read makes (FETCH_PLANNING.md, Requests): what a walk inserts, what
/// planning rejects and how requests are named.
class ReadRequestsTest {

    private static final int ROW_GROUPS = 4;
    private static final int ROWS_PER_ROW_GROUP = 1_000;

    @AfterEach
    void clearProperties() {
        System.clearProperty(CoalescingPolicy.MAX_SPAN_BYTES_PROPERTY);
    }

    /// A row-limited read of a column without an OffsetIndex fetches its chunk's first part, sized
    /// from the limit, and nothing of the rest it does not reach.
    @Test
    void aRowLimitReadsTheFirstPartOfAChunkWithoutAnOffsetIndex() throws Exception {
        // About 4.8 MB of plain longs, above the size a row limit fetches whole
        int rows = 600_000;
        int maxSpan = 1024 * 1024;
        System.setProperty(CoalescingPolicy.MAX_SPAN_BYTES_PROPERTY, String.valueOf(maxSpan));
        byte[] bytes = FooterRewriter.withoutPageIndex(write(rows, rows, "a"));
        ColumnChunk chunk = chunks(bytes)[0];

        List<CountingInputFile.Read> data = dataReads(bytes, chunk, 1_000);

        assertThat(data).hasSize(1);
        assertThat(data.getFirst().offset()).isEqualTo(chunk.chunkStartOffset());
        assertThat(data.getFirst().length()).isLessThanOrEqualTo(maxSpan);
    }

    /// A row-limited read that reaches past its chunk's first part fetches the rest as its scan
    /// reaches it, a piece of at most S at a time, each starting where the one before ends, and no
    /// byte twice.
    @Test
    void theRestOfAChunkIsFetchedAsTheScanReachesIt() throws Exception {
        int rows = 600_000;
        int maxSpan = 1024 * 1024;
        System.setProperty(CoalescingPolicy.MAX_SPAN_BYTES_PROPERTY, String.valueOf(maxSpan));
        byte[] bytes = FooterRewriter.withoutPageIndex(write(rows, rows, "a"));
        ColumnChunk chunk = chunks(bytes)[0];

        List<CountingInputFile.Read> data = dataReads(bytes, chunk, rows - 1);

        long chunkLength = chunk.metaData().totalCompressedSize();
        assertThat(data).hasSize(Math.toIntExact(Math.ceilDiv(chunkLength, maxSpan)));
        assertThat(data).allSatisfy(read -> assertThat(read.length()).isLessThanOrEqualTo(maxSpan));
        for (int i = 1; i < data.size(); i++) {
            assertThat(data.get(i).offset()).as("each piece starts where the one before ends")
                    .isEqualTo(data.get(i - 1).end());
        }
        assertThat(data.getLast().end()).isEqualTo(chunk.chunkStartOffset() + chunkLength);
    }

    /// Pages larger than S in chunks without an OffsetIndex, each spanning several requests: a
    /// request is keyed by where it lies in its chunk until the scan finds the page it holds, and
    /// then by that page's row, so the requests of a page the consumer's next batch needs are
    /// within its need however far into the chunk they lie.
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aPageSpanningSeveralRequestsOfAChunkWithoutAnOffsetIndexIsRead() throws Exception {
        System.setProperty(CoalescingPolicy.MAX_SPAN_BYTES_PROPERTY, String.valueOf(128 * 1024));
        byte[] bytes = FooterRewriter.withoutPageIndex(write(20 * 50_000, 50_000, "c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7"));
        CountingInputFile file = new CountingInputFile(ByteBuffer.wrap(bytes));
        file.open();
        assertThat(readAll(file)).isEqualTo(20 * 50_000);
    }

    @Test
    void overlappingColumnChunksAreRejected() throws Exception {
        byte[] bytes = write(ROWS_PER_ROW_GROUP, ROWS_PER_ROW_GROUP, "a", "b");
        ColumnChunk[] chunks = chunks(bytes);
        long bStart = chunks[1].chunkStartOffset();
        long aStart = chunks[0].chunkStartOffset();
        byte[] overlapping = FooterRewriter.rewrite(bytes, resized(0, bStart + 8 - aStart));

        assertThatThrownBy(() -> readAll(overlapping))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("[<memory>: row group 0, column 'b'] The column chunk [" + bStart + ", "
                        + (bStart + chunks[1].metaData().totalCompressedSize()) + ") overlaps the column chunk of 'a' ["
                        + aStart + ", " + (bStart + 8) + ")");
    }

    /// A column chunk that runs past the end of the file fails the read with a
    /// `ParquetReadException` naming the column.
    @Test
    void aColumnChunkPastTheEndOfTheFileIsRejected() throws Exception {
        byte[] bytes = write(ROWS_PER_ROW_GROUP, ROWS_PER_ROW_GROUP, "a", "b");
        long bStart = chunks(bytes)[1].chunkStartOffset();
        byte[] beyond = FooterRewriter.rewrite(bytes, resized(1, bytes.length));
        long fileLength = beyond.length;

        assertThatThrownBy(() -> readAll(beyond))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("[<memory>: row group 0, column 'b'] The column chunk [" + bStart + ", "
                        + (bStart + bytes.length) + ") lies outside the file of " + fileLength + " bytes");
    }

    /// A request that serves several columns names each of them, in file order.
    @Test
    void aRequestNamesEveryColumnItServes() throws Exception {
        CountingInputFile file = new CountingInputFile(ByteBuffer.wrap(write(ROWS_PER_ROW_GROUP, ROWS_PER_ROW_GROUP,
                "a", "b")));
        file.open();
        readAll(file);

        assertThat(file.reads()).filteredOn(read -> read.reason().contains(" col="))
                .extracting(CountingInputFile.Read::reason)
                .containsExactly("rg=0 col=a col=b");
    }

    /// Page groups of different columns share a request across a gap of at most G: each column's
    /// first and last pages are needed and lie more than G apart within the column, while the last
    /// page of `a` and the first of `b` are adjacent, so three requests serve four page groups.
    @Test
    void pageGroupsOfDifferentColumnsShareARequest() throws Exception {
        int rows = 400_000;
        CountingInputFile file = new CountingInputFile(ByteBuffer.wrap(write(rows, rows, "a", "b")));
        file.open();
        FilterPredicate firstAndLastRows = FilterPredicate.or(FilterPredicate.lt("a", 1_000L),
                FilterPredicate.gtEq("a", rows - 1_000L));
        long matched = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(file);
                RowReader rowReader = reader.buildRowReader().filter(firstAndLastRows).build()) {
            while (rowReader.hasNext()) {
                rowReader.next();
                matched++;
            }
        }

        assertThat(matched).isEqualTo(2_000);
        assertThat(file.reads()).filteredOn(read -> read.reason().contains(" col="))
                .extracting(CountingInputFile.Read::reason)
                .containsExactlyInAnyOrder("rg=0 col=a", "rg=0 col=a col=b", "rg=0 col=b");
    }

    private static long readAll(byte[] bytes) throws Exception {
        CountingInputFile file = new CountingInputFile(ByteBuffer.wrap(bytes));
        file.open();
        return readAll(file);
    }

    private static long readAll(InputFile file) throws Exception {
        long rows = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(file);
                RowReader rowReader = reader.rowReader()) {
            while (rowReader.hasNext()) {
                rowReader.next();
                rows++;
            }
        }
        return rows;
    }

    /// The reads of `chunk`'s bytes a read of the first `limit` rows of `bytes` makes, by offset.
    private static List<CountingInputFile.Read> dataReads(byte[] bytes, ColumnChunk chunk, long limit)
            throws Exception {
        CountingInputFile file = new CountingInputFile(ByteBuffer.wrap(bytes));
        file.open();
        long read = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(file);
                RowReader rows = reader.buildRowReader().head(limit).build()) {
            while (rows.hasNext()) {
                rows.next();
                read++;
            }
        }
        assertThat(read).isEqualTo(limit);
        long start = chunk.chunkStartOffset();
        long end = start + chunk.metaData().totalCompressedSize();
        return file.reads().stream().filter(r -> r.offset() >= start && r.offset() < end)
                .sorted(Comparator.comparingLong(CountingInputFile.Read::offset)).toList();
    }

    private static ColumnChunk[] chunks(byte[] bytes) throws Exception {
        return ParquetMetadataReader.readMetadata(InputFile.of(ByteBuffer.wrap(bytes)))
                .rowGroups().getFirst().columns().toArray(new ColumnChunk[0]);
    }

    /// A footer patch giving column `column` of every row group a compressed size of `size`.
    private static UnaryOperator<FileMetaData> resized(int column, long size) {
        return metaData -> new FileMetaData(metaData.version(), metaData.schema(), metaData.numRows(),
                metaData.rowGroups().stream().map(rowGroup -> {
                    List<ColumnChunk> columns = new ArrayList<>(rowGroup.columns());
                    ColumnChunk chunk = columns.get(column);
                    ColumnMetaData m = chunk.metaData();
                    columns.set(column, new ColumnChunk(new ColumnMetaData(m.type(), m.encodings(), m.pathInSchema(),
                            m.codec(), m.numValues(), m.totalUncompressedSize(), size, m.keyValueMetadata(),
                            m.dataPageOffset(), m.dictionaryPageOffset(), m.statistics(), m.geospatialStatistics(),
                            m.bloomFilterOffset(), m.bloomFilterLength(), m.encodingStats(), m.sizeStatistics()),
                            chunk.offsetIndexOffset(), chunk.offsetIndexLength(), chunk.columnIndexOffset(),
                            chunk.columnIndexLength(), chunk.filePath()));
                    return new RowGroup(columns, rowGroup.totalByteSize(), rowGroup.numRows());
                }).toList(),
                metaData.keyValueMetadata(), metaData.createdBy(), metaData.columnOrders());
    }

    /// `rows` rows of required, plain, uncompressed `INT64` columns of the given names, each holding
    /// the row's index, in row groups of `rowsPerRowGroup` and pages of 10,000 rows.
    private static byte[] write(int rows, int rowsPerRowGroup, String... columns) throws Exception {
        FileSchema.Builder schemaBuilder = FileSchema.builder("sequence");
        for (String column : columns) {
            schemaBuilder.addColumn(column, PhysicalType.INT64, RepetitionType.REQUIRED);
        }
        WriterConfig config = WriterConfig.builder()
                .codec(CompressionCodec.UNCOMPRESSED)
                .encoding(ColumnEncoding.PLAIN)
                .rowGroupTargetRows(rowsPerRowGroup)
                .pageTargetRows(10_000)
                .build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schemaBuilder.build(), config)) {
            RowWriter rowWriter = writer.rowWriter();
            for (int i = 0; i < rows; i++) {
                long value = i;
                rowWriter.writeRow(row -> {
                    for (String column : columns) {
                        row.setLong(column, value);
                    }
                });
            }
        }
        return InMemoryFiles.toByteArray(out);
    }
}
