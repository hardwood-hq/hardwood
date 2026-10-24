/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import dev.hardwood.InMemoryFiles;
import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.internal.schema.ReadProjection;
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
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ColumnEncoding;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.RowWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// The sequence's rules as a read applies them (FETCH_PLANNING.md, Entries and Planning ahead):
/// what a read starts ahead, how far it plans, what a walk inserts, what planning rejects and how
/// entries are named.
class FetchSequenceReadTest {

    private static final int ROW_GROUPS = 4;
    private static final int ROWS_PER_ROW_GROUP = 1_000;

    @AfterEach
    void clearProperties() {
        System.clearProperty(CoalescingPolicy.MAX_SPAN_BYTES_PROPERTY);
    }

    /// A read with a row limit starts no data past its next unit, however much room F and
    /// R leave.
    @Test
    void aReadWithARowLimitStartsNoDataPastItsNextUnit() throws Exception {
        assertThat(rowGroupsReadAfterEnteringTheFirst(ROW_GROUPS * ROWS_PER_ROW_GROUP)).containsExactly(0, 1);
    }

    /// A read without a row limit plans past its next unit while F has room, each row group
    /// one past the last with a started entry.
    @Test
    void aReadWithoutARowLimitPlansAheadWithinF() throws Exception {
        assertThat(rowGroupsReadAfterEnteringTheFirst(0)).containsExactly(0, 1, 2, 3);
    }

    /// The row groups of a four-row-group file not in memory with data read once the first column
    /// has entered the first row group and nothing walks a plan, under a row limit of `maxRows`.
    private static List<Integer> rowGroupsReadAfterEnteringTheFirst(long maxRows) throws Exception {
        byte[] bytes = write(ROW_GROUPS * ROWS_PER_ROW_GROUP, ROWS_PER_ROW_GROUP, "a");
        CountingInputFile file = new CountingInputFile(ByteBuffer.wrap(bytes));
        file.open();
        QuiescentExecutor speculative = new QuiescentExecutor();
        try (HardwoodContextImpl context = HardwoodContextImpl.withIoExecutor(2, speculative)) {
            RowGroupIterator iterator = iterator(file, context, maxRows);
            try {
                iterator.getColumnPlan(iterator.workItemAt(0), 0);
                // The next unit is planned on the read's executor, and starts its first entry then
                for (int i = 0; i < 100 && !iterator.isPlannedAhead(1); i++) {
                    Thread.sleep(50);
                }
                assertThat(speculative.awaitQuiescence(5, TimeUnit.SECONDS)).isTrue();
            }
            finally {
                iterator.close();
            }
        }
        return file.reads().stream()
                .filter(read -> read.reason().contains(" col="))
                .map(read -> Integer.parseInt(read.reason().substring(3, read.reason().indexOf(' '))))
                .distinct().sorted().toList();
    }

    /// A read whose files are all in memory plans one row group ahead on a pool.
    @Tag("pool")
    @Test
    void aReadOfFilesInMemoryPlansOneRowGroupAheadOnAPool() throws Exception {
        try (HardwoodContextImpl context = HardwoodContextImpl.create(2)) {
            RowGroupIterator iterator = inMemoryIterator(context);
            try {
                iterator.getColumnPlan(iterator.workItemAt(0), 0);
                for (int i = 0; i < 100 && !iterator.isPlannedAhead(1); i++) {
                    Thread.sleep(50);
                }
                assertThat(iterator.isPlannedAhead(1)).as("the next row group planned").isTrue();
                Thread.sleep(200);
                assertThat(iterator.isPlannedAhead(2)).as("the row group after it planned").isFalse();
            }
            finally {
                iterator.close();
            }
        }
    }

    /// A read whose files are all in memory plans nothing ahead on a context of zero
    /// threads.
    @Test
    void aReadOfFilesInMemoryPlansNothingAheadOnZeroThreads() throws Exception {
        try (HardwoodContextImpl context = HardwoodContextImpl.create(0)) {
            RowGroupIterator iterator = inMemoryIterator(context);
            try {
                iterator.getColumnPlan(iterator.workItemAt(0), 0);
                assertThat(iterator.isPlannedAhead(1)).as("the next row group planned").isFalse();
            }
            finally {
                iterator.close();
            }
        }
    }

    private static RowGroupIterator inMemoryIterator(HardwoodContextImpl context) throws Exception {
        InputFile file = InputFile.of(ByteBuffer.wrap(write(ROW_GROUPS * ROWS_PER_ROW_GROUP, ROWS_PER_ROW_GROUP, "a")));
        file.open();
        return iterator(file, context, 0);
    }

    private static RowGroupIterator iterator(InputFile file, HardwoodContextImpl context, long maxRows)
            throws Exception {
        FileMetaData metaData = ParquetMetadataReader.readMetadata(file);
        FileSchema schema = FileSchema.fromSchemaElements(metaData.schema());
        RowGroupIterator iterator = RowGroupIterators.standalone(List.of(file), context, maxRows);
        iterator.setFirstFile(schema, metaData.rowGroups());
        iterator.initialize(ReadProjection.of(ProjectedSchema.create(schema, ColumnProjection.all())), null, true);
        return iterator;
    }

    /// A column without an offset index under a row limit holds its chunk's first part; the
    /// rest is requested as the walk reaches it, a piece of at most S at a time, and no byte twice.
    @Test
    void theRestOfAChunkARowLimitLeftOutIsRequestedAsTheWalkReachesIt() throws Exception {
        // About 4.8 MB of plain longs, above the size a row limit fetches whole
        int rows = 600_000;
        byte[] bytes = FooterRewriter.withoutPageIndex(write(rows, rows, "a"));
        int maxSpan = 1024 * 1024;
        System.setProperty(CoalescingPolicy.MAX_SPAN_BYTES_PROPERTY, String.valueOf(maxSpan));
        CountingInputFile file = new CountingInputFile(ByteBuffer.wrap(bytes));
        file.open();
        try (HardwoodContextImpl context = HardwoodContextImpl.create(1)) {
            FileMetaData metaData = ParquetMetadataReader.readMetadata(file);
            ColumnSchema column = FileSchema.fromSchemaElements(metaData.schema()).getColumn("a");
            ColumnChunk chunk = metaData.rowGroups().getFirst().columns().getFirst();
            int before = file.readCount();
            PageWalks.Pages pages = PageWalks.of(SequentialFetchPlan.build(file, column, chunk, context, 0,
                    file.name(), rows));

            pages.next();
            assertThat(file.readCount() - before).as("requests for the first page").isEqualTo(1);
            while (pages.hasNext()) {
                pages.next();
            }

            List<CountingInputFile.Read> data = file.reads().subList(before, file.readCount()).stream()
                    .sorted(Comparator.comparingLong(CountingInputFile.Read::offset)).toList();
            long chunkLength = chunk.metaData().totalCompressedSize();
            assertThat(data).hasSize(Math.toIntExact(Math.ceilDiv(chunkLength, maxSpan)));
            assertThat(data).allSatisfy(read -> assertThat(read.length()).isLessThanOrEqualTo(maxSpan));
            for (int i = 1; i < data.size(); i++) {
                assertThat(data.get(i).offset()).as("each piece starts where the one before ends")
                        .isEqualTo(data.get(i - 1).end());
            }
            assertThat(data.getLast().end()).isEqualTo(chunk.chunkStartOffset() + chunkLength);
        }
    }

    /// Column chunks that overlap fail the read with a `ParquetReadException` naming the
    /// column, before any data is requested.
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

    /// An entry that serves several columns names each of them, in file order.
    @Test
    void anEntryNamesEveryColumnItServes() throws Exception {
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
