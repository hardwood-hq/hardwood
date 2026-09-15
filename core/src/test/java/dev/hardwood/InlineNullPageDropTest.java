/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.LongStream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.hardwood.internal.predicate.FilterPredicateResolver;
import dev.hardwood.internal.predicate.PageDropPredicates;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.reader.HardwoodContextImpl;
import dev.hardwood.internal.reader.PageIterator;
import dev.hardwood.internal.reader.SequentialFetchPlan;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

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
/// pages come from walking the column's [SequentialFetchPlan] directly and counting what it
/// reads, so a page kept where it could be dropped fails the test even though the per-row
/// filter would still return the right rows. Neither fixture carries a page index, so the plan
/// walked here is the one both readers use.
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

    // ==================== Fixtures ====================

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

    /// Walks the fetch plan for `column` under `filter` and returns the number of pages it
    /// reads. A page the inline statistics drop is emitted as a null placeholder carrying no
    /// data, and is not counted.
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
            SequentialFetchPlan plan = SequentialFetchPlan.build(inputFile, columnSchema, columnChunk,
                    context, 0, inputFile.name(), 0L, dropLeaves);
            PageIterator pages = plan.pages();
            while (pages.hasNext()) {
                if (!pages.next().isNullPlaceholder()) {
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
