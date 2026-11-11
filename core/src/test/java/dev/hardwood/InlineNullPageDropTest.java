/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.LongStream;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.predicate.BoundsReadability;
import dev.hardwood.internal.predicate.FilterPredicateResolver;
import dev.hardwood.internal.predicate.LogContext;
import dev.hardwood.internal.predicate.PageDropPredicates;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.reader.decode.ChunkPages;
import dev.hardwood.internal.reader.decode.PageInfo;
import dev.hardwood.internal.thrift.PageHeaderReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.PageType;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Pages null on every row, in a file carrying no page index, are dropped from their inline
/// statistics for a predicate a null row fails, and read for `IS NULL`, which a null row
/// satisfies.
///
/// The fixtures hold one row group of 1000 rows in pages of 100. `value` and `address.city` are
/// null on rows `[300, 600)`, three pages of each, while `address` stays present on every row.
/// The null pages record no bounds, so only their null count can drop them. One fixture is
/// written with v1 data pages, the other with v2.
///
/// Each case asserts two things. The rows come from a read through the row reader and a read
/// through the column readers, which must agree with each other and with the predicate. The
/// pages come from deciding each of the column's pages by its inline statistics
/// ([PageDropPredicates#canDropPage(List, dev.hardwood.internal.metadata.PageHeader, ColumnSchema,
/// dev.hardwood.internal.predicate.BoundsReadability, dev.hardwood.internal.predicate.LogContext)])
/// and counting those kept, so a page kept where it could be dropped fails the test even though
/// the per-row filter would still return the right rows.
class InlineNullPageDropTest {

    @ParameterizedTest
    @ValueSource(strings = { "v1", "v2" })
    void aValuePredicateReadsNoPageNullOnEveryRow(String pageVersion) throws Exception {
        Path file = fixture(pageVersion);
        FilterPredicate filter = FilterPredicate.eq("value", 650L);

        assertReads(file, filter, "value", ids(650, 651));

        // Page [600, 700) holds the literal; the other six pages with values drop on their
        // bounds, and the three null pages on their null count.
        assertThat(pagesRead(file, "value", filter)).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(strings = { "v1", "v2" })
    void isNotNullReadsNoPageNullOnEveryRow(String pageVersion) throws Exception {
        Path file = fixture(pageVersion);
        FilterPredicate filter = FilterPredicate.isNotNull("value");

        assertReads(file, filter, "value", nonNullIds());

        assertThat(pagesRead(file, "value", filter)).isEqualTo(7);
    }

    @ParameterizedTest
    @ValueSource(strings = { "v1", "v2" })
    void isNullReadsEveryPage(String pageVersion) throws Exception {
        Path file = fixture(pageVersion);
        FilterPredicate filter = FilterPredicate.isNull("value");

        assertReads(file, filter, "value", ids(300, 600));

        assertThat(pagesRead(file, "value", filter)).isEqualTo(10);
    }

    @ParameterizedTest
    @ValueSource(strings = { "v1", "v2" })
    void aLeafBelowAPresentStructDropsItsNullPages(String pageVersion) throws Exception {
        Path file = fixture(pageVersion);
        FilterPredicate filter = FilterPredicate.isNotNull("address.city");

        assertReads(file, filter, "address.city", nonNullIds());

        assertThat(pagesRead(file, "address.city", filter)).isEqualTo(7);
    }

    @ParameterizedTest
    @ValueSource(strings = { "v1", "v2" })
    void isNullOnALeafBelowAPresentStructReturnsItsNullRows(String pageVersion) throws Exception {
        Path file = fixture(pageVersion);
        FilterPredicate filter = FilterPredicate.isNull("address.city");

        assertReads(file, filter, "address.city", ids(300, 600));

        assertThat(pagesRead(file, "address.city", filter)).isEqualTo(10);
    }

    /// The bodies of the pages the filter's inline statistics drop are overwritten, which no
    /// decode survives: the filtered read returns its rows from the pages it decodes, reading the
    /// dropped ones as nulls, while a read without the filter, which decodes every page, fails.
    @ParameterizedTest
    @ValueSource(strings = { "v1", "v2" })
    void aPageItsInlineStatisticsDropIsNeverDecoded(String pageVersion, @TempDir Path dir) throws Exception {
        FilterPredicate filter = FilterPredicate.eq("value", 650L);
        Path corrupted = dir.resolve("dropped-pages-corrupted.parquet");
        Files.write(corrupted, withDroppedPagesCorrupted(fixture(pageVersion), "value", filter));

        assertReads(corrupted, filter, "value", ids(650, 651));

        assertThatThrownBy(() -> {
            try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(corrupted));
                 RowReader rows = reader.rowReader()) {
                while (rows.hasNext()) {
                    rows.next();
                }
            }
        }).isInstanceOf(ParquetReadException.class);
    }

    // ==================== Fixtures ====================

    /// `file`'s bytes with the body of every page of `column` that `filter`'s inline statistics
    /// drop overwritten with `0xFF`, its header left intact.
    private static byte[] withDroppedPagesCorrupted(Path file, String column, FilterPredicate filter)
            throws IOException {
        byte[] bytes = Files.readAllBytes(file);
        FileSchema schema;
        ColumnChunk chunk;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            schema = reader.getFileSchema();
            chunk = reader.getFileMetaData().rowGroups().get(0).columns().get(schema.getColumn(column).columnIndex());
        }
        ColumnSchema columnSchema = schema.getColumn(column);
        List<ResolvedPredicate> leaves = PageDropPredicates.byColumn(FilterPredicateResolver.resolve(filter, schema))
                .getOrDefault(columnSchema.columnIndex(), List.of());
        int position = Math.toIntExact(chunk.chunkStartOffset());
        int end = Math.toIntExact(chunk.chunkStartOffset() + chunk.metaData().totalCompressedSize());
        for (int page = 0; position < end; ) {
            ThriftCompactReader reader = new ThriftCompactReader(ByteBuffer.wrap(bytes, position, end - position).slice(), 0);
            PageHeader header = PageHeaderReader.read(reader);
            int headerSize = reader.getBytesRead();
            if ((header.type() == PageType.DATA_PAGE || header.type() == PageType.DATA_PAGE_V2)
                    && PageDropPredicates.canDropPage(leaves, header, columnSchema, BoundsReadability.ALL,
                            new LogContext(file.toString(), 0).withColumn(columnSchema.fieldPath()).withPageIndex(page++))) {
                Arrays.fill(bytes, position + headerSize, position + headerSize + header.compressedPageSize(), (byte) 0xFF);
            }
            position += headerSize + header.compressedPageSize();
        }
        return bytes;
    }

    private static Path fixture(String pageVersion) {
        return Paths.get("src/test/resources/inline_null_pages_" + pageVersion + ".parquet");
    }

    /// Reads `file` under `filter` through the row reader and through the column readers, and
    /// asserts that both return exactly the rows `expectedIds` names.
    private static void assertReads(Path file, FilterPredicate filter, String column, List<Long> expectedIds)
            throws Exception {
        List<Long> rowIds = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
             RowReader rows = reader.buildRowReader().filter(filter).build()) {
            while (rows.hasNext()) {
                rows.next();
                assertThat(rows.getStruct("address")).as("address is present on every row").isNotNull();
                rowIds.add(rows.getLong("id"));
            }
        }

        List<Long> columnIds = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
             ColumnReaders cols = reader.buildColumnReaders(ColumnProjection.columns("id", column))
                     .filter(filter)
                     .build()) {
            ColumnReader id = cols.getColumnReader("id");
            while (cols.nextBatch()) {
                long[] values = id.getLongs();
                for (int i = 0; i < id.getRecordCount(); i++) {
                    columnIds.add(values[i]);
                }
            }
        }

        assertThat(rowIds).as("row reader").containsExactlyElementsOf(expectedIds);
        assertThat(columnIds).as("column readers").containsExactlyElementsOf(expectedIds);
    }

    /// The pages of `column` its inline statistics do not drop under `filter`: a page they drop is
    /// read as a null placeholder carrying no data, and is not counted.
    private static int pagesRead(Path file, String column, FilterPredicate filter) throws IOException {
        FileMetaData fileMetaData;
        FileSchema schema;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            fileMetaData = reader.getFileMetaData();
            schema = reader.getFileSchema();
        }

        ColumnSchema columnSchema = schema.getColumn(column);
        int columnIndex = schema.getColumns().indexOf(columnSchema);
        assertThat(columnIndex).as("column '%s' must exist in the fixture", column).isNotNegative();
        ColumnChunk columnChunk = fileMetaData.rowGroups().get(0).columns().get(columnIndex);

        ResolvedPredicate resolved = FilterPredicateResolver.resolve(filter, schema);
        List<ResolvedPredicate> dropLeaves =
                PageDropPredicates.byColumn(resolved).getOrDefault(columnIndex, List.of());

        int pagesRead = 0;
        try (HardwoodContextImpl context = HardwoodContextImpl.create();
             InputFile inputFile = InputFile.of(file)) {
            inputFile.open();
            List<PageInfo> pages = ChunkPages.of(inputFile, columnSchema, columnChunk, context);
            for (int i = 0; i < pages.size(); i++) {
                LogContext page = new LogContext(inputFile.name(), 0).withColumn(columnSchema.fieldPath())
                        .withPageIndex(i);
                if (!PageDropPredicates.canDropPage(dropLeaves, pages.get(i).dataPage().header(), columnSchema,
                        BoundsReadability.ALL, page)) {
                    pagesRead++;
                }
            }
        }
        return pagesRead;
    }

    private static List<Long> ids(long fromInclusive, long toExclusive) {
        return LongStream.range(fromInclusive, toExclusive).boxed().toList();
    }

    private static List<Long> nonNullIds() {
        return LongStream.concat(LongStream.range(0, 300), LongStream.range(600, 1000)).boxed().toList();
    }
}
