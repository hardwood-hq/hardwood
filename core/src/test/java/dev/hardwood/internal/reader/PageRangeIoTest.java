/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.Hardwood;
import dev.hardwood.InputFile;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;

import static org.assertj.core.api.Assertions.assertThat;

/// With page-level Column Index filtering active, each reader fetches only the pages holding
/// matching rows, measured against [IoBudget].
///
/// Uses column_index_pushdown.parquet: 10000 rows, sorted id [0,9999],
/// Parquet v2 with Column Index, ~10 pages of 1024 values each.
class PageRangeIoTest {

    private static final Path TEST_FILE = Path.of("src/test/resources/column_index_pushdown.parquet");

    /// `id` equals the row index, so this matches exactly the rows `[0, MATCHING_ROWS)`, all on the
    /// first page.
    private static final FilterPredicate SELECTIVE_FILTER = FilterPredicate.lt("id", 1000L);
    private static final long MATCHING_ROWS = 1000;

    // ColumnReader path (single-file, single-column)

    @Test
    void columnReaderFetchesOnlyMatchingPages() throws Exception {
        CountingInputFile inputFile = new CountingInputFile(InputFile.of(TEST_FILE));
        inputFile.open();
        long rows = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(inputFile);
                ColumnReader col = reader.buildColumnReader("id").filter(SELECTIVE_FILTER).build()) {
            while (col.nextBatch()) {
                rows += col.getRecordCount();
            }
        }

        assertThat(rows).isEqualTo(MATCHING_ROWS);
        IoBudget.of(TEST_FILE, List.of("id"), 0, MATCHING_ROWS).assertWithin(inputFile);
    }

    // Single-file, multi-column path

    @Test
    void rowReaderFetchesOnlyMatchingPages() throws Exception {
        CountingInputFile inputFile = new CountingInputFile(InputFile.of(TEST_FILE));
        inputFile.open();
        long rows = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(inputFile);
                RowReader rowReader = reader.buildRowReader()
                        .projection(ColumnProjection.columns("id", "value"))
                        .filter(SELECTIVE_FILTER)
                        .build()) {
            while (rowReader.hasNext()) {
                rowReader.next();
                rows++;
            }
        }

        assertThat(rows).isEqualTo(MATCHING_ROWS);
        IoBudget.of(TEST_FILE, List.of("id", "value"), 0, MATCHING_ROWS).assertWithin(inputFile);
    }

    // FileManager path (multi-file, via ParquetFileReader)

    @Test
    void multiFileReaderFetchesOnlyMatchingPages() throws Exception {
        CountingInputFile inputFile = new CountingInputFile(InputFile.of(TEST_FILE));
        inputFile.open();
        long rows = 0;
        try (Hardwood hardwood = Hardwood.create();
                ParquetFileReader parquet = hardwood.openAll(List.of(inputFile));
                ColumnReaders columns = parquet.buildColumnReaders(ColumnProjection.columns("id", "value"))
                        .filter(SELECTIVE_FILTER)
                        .build()) {
            while (columns.nextBatch()) {
                rows += columns.getRecordCount();
            }
        }

        assertThat(rows).isEqualTo(MATCHING_ROWS);
        IoBudget.of(TEST_FILE, List.of("id", "value"), 0, MATCHING_ROWS).assertWithin(inputFile);
    }

    // Dictionary-encoded column path

    private static final Path DICT_TEST_FILE = Path.of("src/test/resources/column_index_pushdown_dict.parquet");

    @Test
    void columnReaderFetchesOnlyMatchingPagesOfDictionaryEncodedColumn() throws Exception {
        // The filter is on `id` while `category` is read, so exact filtering (#624) decodes `id`
        // as well: both columns are in the budget.
        CountingInputFile inputFile = new CountingInputFile(InputFile.of(DICT_TEST_FILE));
        inputFile.open();
        long rows = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(inputFile);
                ColumnReader col = reader.buildColumnReader("category").filter(SELECTIVE_FILTER).build()) {
            while (col.nextBatch()) {
                rows += col.getRecordCount();
                // The values decode only with the dictionary page fetched.
                assertThat(col.getStrings()).isNotEmpty();
            }
        }

        assertThat(rows).isEqualTo(MATCHING_ROWS);
        IoBudget.of(DICT_TEST_FILE, List.of("category", "id"), 0, MATCHING_ROWS).assertWithin(inputFile);
    }
}
