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
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;

/// Row-set correctness when statistics prove row groups match a filter in full.
///
/// `filter_pushdown_int.parquet` holds `id`/`value` 1–300 across three row groups
/// (1–100, 101–200, 201–300) with statistics, so threshold sweeps cross every
/// configuration of dropped, partially-matching, and fully-matching row groups —
/// including the all-groups-fully-matching case where filtering is skipped
/// wholesale, and mixed cases where fully-matching groups skip per-row evaluation
/// while partially-matching groups are still evaluated exactly.
class AlwaysMatchingRowGroupTest {

    private static final Path FIXTURE =
            Paths.get("src/test/resources/filter_pushdown_int.parquet");

    private static final List<InputFile> MULTI_FIXTURE = InputFile.ofPaths(
            Paths.get("src/test/resources/multi_file_part0.parquet"),
            Paths.get("src/test/resources/multi_file_part1.parquet")
    );

    @Test
    void thresholdSweepMatchesBruteForce() throws Exception {
        // Thresholds cross row-group boundaries (100, 200, 300) exactly, adjacently,
        // and far outside, exercising every mix of full/partial/dropped groups.
        int[] thresholds = { 0, 1, 2, 50, 100, 101, 102, 150, 200, 201, 250, 300, 301 };
        for (int threshold : thresholds) {
            assertGtEqMatchesBruteForce(threshold);
            assertLtMatchesBruteForce(threshold);
        }
    }

    @Test
    void fullyMatchingRangePredicateKeepsEveryRow() throws Exception {
        // AND of two bounds, both satisfied by all three row groups.
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rows = reader.buildRowReader()
                     .filter(FilterPredicate.and(
                             FilterPredicate.gtEq("id", 1L),
                             FilterPredicate.ltEq("id", 300L)))
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
    void multiFileReadWithFullyMatchingAndDroppedRowGroups() throws Exception {
        // Files hold id 0–149 and 150–249 in row groups of 50: lt(200) fully matches
        // every surviving row group and drops the last one entirely.
        try (ParquetFileReader reader = ParquetFileReader.openAll(MULTI_FIXTURE);
             RowReader rows = reader.buildRowReader()
                     .filter(FilterPredicate.lt("id", 200L))
                     .build()) {
            long expected = 0;
            while (rows.hasNext()) {
                rows.next();
                assertThat(rows.getLong("id")).isEqualTo(expected);
                expected++;
            }
            assertThat(expected).isEqualTo(200);
        }
    }

    private static void assertGtEqMatchesBruteForce(int threshold) throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rows = reader.buildRowReader()
                     .filter(FilterPredicate.gtEq("id", (long) threshold))
                     .build()) {
            long expected = Math.max(threshold, 1);
            long count = 0;
            while (rows.hasNext()) {
                rows.next();
                assertThat(rows.getLong("id"))
                        .as("gtEq(%s), match %s", threshold, count)
                        .isEqualTo(expected);
                expected++;
                count++;
            }
            assertThat(count).isEqualTo(Math.max(0, 300 - Math.max(threshold, 1) + 1));
        }
    }

    private static void assertLtMatchesBruteForce(int threshold) throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rows = reader.buildRowReader()
                     .filter(FilterPredicate.lt("id", (long) threshold))
                     .build()) {
            long expected = 1;
            long count = 0;
            while (rows.hasNext()) {
                rows.next();
                assertThat(rows.getLong("id"))
                        .as("lt(%s), match %s", threshold, count)
                        .isEqualTo(expected);
                expected++;
                count++;
            }
            assertThat(count).isEqualTo(Math.max(0, Math.min(threshold - 1, 300)));
        }
    }
}
