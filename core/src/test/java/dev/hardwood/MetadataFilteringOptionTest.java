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

/// The `hardwood.metadata-filtering` reader option (#797): set to `"false"`,
/// no row group or page is skipped from statistics, bloom filters, dictionary
/// pages, or page indexes, and predicates fall back to per-row evaluation — the escape hatch
/// for files whose metadata is unreliable. Results must be identical to the
/// default path; only the shortcut is gone.
///
/// `filter_pushdown_int.parquet`: 3 row groups of 100 rows — `id` 1..100,
/// 101..200, 201..300, with honest statistics that would normally prune.
///
/// `filter_pushdown_int_lying_stats.parquet`: same data, but the third row
/// group's `id` statistics are falsified to advertise `[101, 200]`. Trusting
/// that footer prunes the group that really holds 201..300 for `gt(200)`, and
/// skips per-row evaluation of it for `ltEq(200)`; the opt-out must return the
/// exact rows for both.
class MetadataFilteringOptionTest extends AbstractJfrRecorderTest {

    private static final Path FIXTURE =
            Paths.get("src/test/resources/filter_pushdown_int.parquet");
    private static final Path LYING_STATS_FIXTURE =
            Paths.get("src/test/resources/filter_pushdown_int_lying_stats.parquet");
    private static final String PUSH_DOWN_EVENT = "dev.hardwood.RowGroupFilter";

    private static final ReaderConfig STATS_OFF = ReaderConfig.builder()
            .option("hardwood.metadata-filtering", "false")
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
        // The third row group's falsified statistics claim [101, 200], which ltEq(200)
        // satisfies throughout, so trusting them skips per-row evaluation there and
        // returns its rows 201..300 although none of them matches.
        try (HardwoodContext context = HardwoodContext.create();
             ParquetFileReader reader = ParquetFileReader.open(InputFile.of(LYING_STATS_FIXTURE), context);
             RowReader rows = reader.buildRowReader()
                     .filter(FilterPredicate.ltEq("id", 200L))
                     .build()) {
            long count = 0;
            while (rows.hasNext()) {
                rows.next();
                count++;
            }
            assertThat(count)
                    .as("trusting the falsified statistics takes the always-match shortcut")
                    .isEqualTo(300);
        }

        // With statistics filtering off every row is evaluated, so only 1..200 are returned.
        try (HardwoodContext context = HardwoodContext.create();
             ParquetFileReader reader = ParquetFileReader.open(InputFile.of(LYING_STATS_FIXTURE), context, STATS_OFF);
             RowReader rows = reader.buildRowReader()
                     .filter(FilterPredicate.ltEq("id", 200L))
                     .build()) {
            long expected = 1;
            while (rows.hasNext()) {
                rows.next();
                assertThat(rows.getLong("id")).isEqualTo(expected);
                expected++;
            }
            assertThat(expected)
                    .as("ignoring the falsified statistics evaluates every row")
                    .isEqualTo(201);
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

    @Test
    void lyingStatisticsDropMatchingRowsByDefaultButOptOutRecoversThem() throws Exception {
        // The third row group's `id` statistics claim [101, 200] while the data
        // is 201..300. gt(200) prunes that group when the footer is trusted, so
        // the default read returns nothing — the exact silent-data-loss failure
        // the opt-out exists to escape.
        try (HardwoodContext context = HardwoodContext.create();
             ParquetFileReader reader = ParquetFileReader.open(InputFile.of(LYING_STATS_FIXTURE), context);
             RowReader rows = reader.buildRowReader()
                     .filter(FilterPredicate.gt("id", 200L))
                     .build()) {
            assertThat(rows.hasNext())
                    .as("trusting the falsified statistics prunes the matching row group")
                    .isFalse();
        }

        // With statistics filtering off the same predicate is evaluated per row,
        // so the 100 genuinely-matching rows (201..300) are returned.
        try (HardwoodContext context = HardwoodContext.create();
             ParquetFileReader reader = ParquetFileReader.open(InputFile.of(LYING_STATS_FIXTURE), context, STATS_OFF);
             RowReader rows = reader.buildRowReader()
                     .filter(FilterPredicate.gt("id", 200L))
                     .build()) {
            long expected = 201;
            while (rows.hasNext()) {
                rows.next();
                assertThat(rows.getLong("id")).isEqualTo(expected);
                expected++;
            }
            assertThat(expected)
                    .as("ignoring the falsified statistics recovers every matching row")
                    .isEqualTo(301);
        }
    }
}
