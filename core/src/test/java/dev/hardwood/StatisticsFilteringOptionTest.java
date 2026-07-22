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

import org.junit.jupiter.api.Test;

import dev.hardwood.jfr.AbstractJfrRecorderTest;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ReaderConfig;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;

/// The `hardwood.statistics-filtering` reader option (#799): set to `"false"`,
/// no row group or page is skipped from statistics, bloom filters, or page
/// indexes, and predicates fall back to per-row evaluation — the escape hatch
/// for files whose metadata is unreliable. Results must be identical to the
/// default path; only the shortcut is gone.
///
/// `filter_pushdown_int.parquet`: 3 row groups of 100 rows — `id` 1..100,
/// 101..200, 201..300, with statistics that would normally prune.
class StatisticsFilteringOptionTest extends AbstractJfrRecorderTest {

    private static final Path FIXTURE =
            Paths.get("src/test/resources/filter_pushdown_int.parquet");
    private static final String PUSH_DOWN_EVENT = "dev.hardwood.RowGroupFilter";

    private static final ReaderConfig STATS_OFF = ReaderConfig.builder()
            .option("hardwood.statistics-filtering", "false")
            .build();

    @Test
    void rowReaderReturnsExactRowsWithoutPruning() throws Exception {
        // gt(200) would prune RG0 and RG1; with statistics filtering off, all
        // three groups are decoded and per-row evaluation must produce the
        // identical row set.
        try (HardwoodContext context = HardwoodContext.create();
             ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE), context, STATS_OFF);
             RowReader rows = reader.buildRowReader()
                     .filter(FilterPredicate.gt("id", 200L))
                     .build()) {
            long expected = 201;
            while (rows.hasNext()) {
                rows.next();
                assertThat(rows.getLong("id")).isEqualTo(expected);
                expected++;
            }
            assertThat(expected).isEqualTo(301);
        }
        awaitEvents();
        assertThat(events(PUSH_DOWN_EVENT).count())
                .as("statistics filtering disabled: no push-down evaluation ran")
                .isZero();
    }

    @Test
    void fullyMatchingPredicateIsStillEvaluatedPerRow() throws Exception {
        // gtEq(1) matches every row; the always-match fast path would normally
        // drop the filter wholesale. Disabled, the read must still return all
        // rows through per-row evaluation.
        try (HardwoodContext context = HardwoodContext.create();
             ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE), context, STATS_OFF);
             RowReader rows = reader.buildRowReader()
                     .filter(FilterPredicate.gtEq("id", 1L))
                     .build()) {
            long count = 0;
            long sum = 0;
            while (rows.hasNext()) {
                rows.next();
                sum += rows.getLong("id");
                count++;
            }
            assertThat(count).isEqualTo(300);
            assertThat(sum).isEqualTo(300L * 301 / 2);
        }
    }

    @Test
    void filteredColumnReaderReturnsExactRowsWithoutPruning() throws Exception {
        // Exact column filtering on the drain-side path: filter on id, read
        // value, statistics filtering off.
        try (HardwoodContext context = HardwoodContext.create();
             ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE), context, STATS_OFF);
             ColumnReader values = reader.buildColumnReader("value")
                     .filter(FilterPredicate.gt("id", 200L))
                     .build()) {
            int totalRows = 0;
            while (values.nextBatch()) {
                long[] batch = values.getLongs();
                int count = values.getRecordCount();
                totalRows += count;
                for (int i = 0; i < count; i++) {
                    assertThat(batch[i]).isGreaterThan(200L);
                }
            }
            assertThat(totalRows).isEqualTo(100);
        }
        awaitEvents();
        assertThat(events(PUSH_DOWN_EVENT).count())
                .as("statistics filtering disabled: no push-down evaluation ran")
                .isZero();
    }
}
