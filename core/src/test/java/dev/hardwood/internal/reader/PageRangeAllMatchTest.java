/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.file.Path;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;

import static org.assertj.core.api.Assertions.assertThat;

/// Regression test: when a filter matches all pages in a row group (Column Index
/// shows every page overlaps the predicate), the reader must still return all rows.
///
/// Previously, [PageRange#forColumn] returned an empty list to signal "no pages
/// skipped, use full chunk path", but [PageSource#scanWithPageRanges] interpreted
/// that as "no data" and returned zero rows.
class PageRangeAllMatchTest {

    private static final Path TEST_FILE = Path.of("src/test/resources/filter_all_pages_match.parquet");

    @Test
    void filterMatchingAllPagesShouldReturnAllRows() throws Exception {
        // Filter that matches every row (id < 1000 on a file with id 0-99).
        // Column Index filtering produces RowRanges covering all pages, but
        // NOT RowRanges.ALL — it's a concrete range set.
        FilterPredicate filter = FilterPredicate.lt("id", 1000L);

        long unfilteredCount;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(TEST_FILE));
             RowReader rows = reader.buildRowReader().projection(ColumnProjection.all()).build()) {
            unfilteredCount = 0;
            while (rows.hasNext()) {
                rows.next();
                unfilteredCount++;
            }
        }

        long filteredCount;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(TEST_FILE));
             RowReader rows = reader.buildRowReader().projection(ColumnProjection.all()).filter(filter).build()) {
            filteredCount = 0;
            while (rows.hasNext()) {
                rows.next();
                filteredCount++;
            }
        }

        assertThat(unfilteredCount).as("Unfiltered row count").isEqualTo(100);
        assertThat(filteredCount)
                .as("Filter matching all pages should return all rows")
                .isEqualTo(unfilteredCount);
    }
}
