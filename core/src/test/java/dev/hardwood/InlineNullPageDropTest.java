/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.LongStream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.hardwood.jfr.AbstractJfrRecorderTest;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;

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
/// Every read goes through the row reader and through the column readers, and the decoded pages
/// of the filtered column are counted across both, so a page kept where it could be dropped
/// shows up even though the per-row filter would still return the right rows.
class InlineNullPageDropTest extends AbstractJfrRecorderTest {

    private static final String PAGE_DECODED = "dev.hardwood.PageDecoded";

    /// Each read of the fixture decodes the filtered column's pages once, through the row reader
    /// and through the column readers.
    private static final int READS = 2;

    @ParameterizedTest
    @ValueSource(strings = { "v1", "v2" })
    void aValuePredicateReadsNoPageNullOnEveryRow(String pageVersion) throws Exception {
        Path file = fixture(pageVersion);

        assertReads(file, FilterPredicate.eq("value", 650L), "value", ids(650, 651));

        // Page [600, 700) holds the literal; the other six pages with values drop on their
        // bounds, and the three null pages on their null count.
        assertThat(decodedPages("value")).isEqualTo(READS);
    }

    @ParameterizedTest
    @ValueSource(strings = { "v1", "v2" })
    void isNotNullReadsNoPageNullOnEveryRow(String pageVersion) throws Exception {
        Path file = fixture(pageVersion);

        assertReads(file, FilterPredicate.isNotNull("value"), "value", nonNullIds());

        assertThat(decodedPages("value")).isEqualTo(7 * READS);
    }

    @ParameterizedTest
    @ValueSource(strings = { "v1", "v2" })
    void isNullReadsEveryPage(String pageVersion) throws Exception {
        Path file = fixture(pageVersion);

        assertReads(file, FilterPredicate.isNull("value"), "value", ids(300, 600));

        assertThat(decodedPages("value")).isEqualTo(10 * READS);
    }

    @ParameterizedTest
    @ValueSource(strings = { "v1", "v2" })
    void aLeafBelowAPresentStructDropsItsNullPages(String pageVersion) throws Exception {
        Path file = fixture(pageVersion);

        assertReads(file, FilterPredicate.isNotNull("address.city"), "address.city", nonNullIds());

        assertThat(decodedPages("city")).isEqualTo(7 * READS);
    }

    @ParameterizedTest
    @ValueSource(strings = { "v1", "v2" })
    void isNullOnALeafBelowAPresentStructReturnsItsNullRows(String pageVersion) throws Exception {
        Path file = fixture(pageVersion);

        assertReads(file, FilterPredicate.isNull("address.city"), "address.city", ids(300, 600));

        assertThat(decodedPages("city")).isEqualTo(10 * READS);
    }

    // ==================== Fixtures ====================

    private static Path fixture(String pageVersion) {
        return Paths.get("src/test/resources/inline_null_pages_" + pageVersion + ".parquet");
    }

    /// Reads `file` under `filter` through the row reader and through the column readers, asserts
    /// that both return exactly the rows `expectedIds` names, and stops the recording.
    private void assertReads(Path file, FilterPredicate filter, String column, List<Long> expectedIds)
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
        awaitEvents();

        assertThat(rowIds).as("row reader").containsExactlyElementsOf(expectedIds);
        assertThat(columnIds).as("column readers").containsExactlyElementsOf(expectedIds);
    }

    private long decodedPages(String column) {
        return events(PAGE_DECODED).filter(event -> column.equals(event.getString("column"))).count();
    }

    private static List<Long> ids(long fromInclusive, long toExclusive) {
        return LongStream.range(fromInclusive, toExclusive).boxed().toList();
    }

    private static List<Long> nonNullIds() {
        return LongStream.concat(LongStream.range(0, 300), LongStream.range(600, 1000)).boxed().toList();
    }
}
