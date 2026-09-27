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

import dev.hardwood.jfr.AbstractJfrRecorderTest;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import jdk.jfr.consumer.RecordedEvent;

import static org.assertj.core.api.Assertions.assertThat;

/// Asserts that a row group dropped by its bloom filter is reported by
/// `dev.hardwood.RowGroupBloomFilter`, while `dev.hardwood.RowGroupFilter`, which reports the
/// statistics decisions taken before the read reaches it, counts it as kept.
class RowGroupBloomFilterEventTest extends AbstractJfrRecorderTest {

    /// Five row groups of 200 rows; `code` cycles 0, 3, 6, 9 within every row group, with a bloom
    /// filter and no dictionary.
    private static final Path FIXTURE = Paths.get("src/test/resources/bloom_filter_multi_rg_test.parquet");

    private static final String BLOOM_EVENT = "dev.hardwood.RowGroupBloomFilter";
    private static final String PUSH_DOWN_EVENT = "dev.hardwood.RowGroupFilter";

    @Test
    void rowGroupsDroppedByTheirBloomFiltersAreReportedOnceEach() throws Exception {
        // Between the statistics' 0 and 9, but in no row group.
        long rows = read(FilterPredicate.eq("code", 5));

        assertThat(rows).isZero();
        List<RecordedEvent> dropped = events(BLOOM_EVENT).toList();
        assertThat(dropped).extracting(event -> event.getInt("rowGroupIndex"))
                .containsExactlyInAnyOrder(0, 1, 2, 3, 4);
        assertThat(dropped).allSatisfy(event ->
                assertThat(event.getString("file")).endsWith("bloom_filter_multi_rg_test.parquet"));

        List<RecordedEvent> pushDown = events(PUSH_DOWN_EVENT).toList();
        assertThat(pushDown).hasSize(1);
        assertThat(pushDown.getFirst().getInt("rowGroupsKept"))
                .as("statistics keep every row group; their bloom filters drop them later")
                .isEqualTo(5);
    }

    @Test
    void rowGroupsTheirBloomFiltersKeepAreNotReported() throws Exception {
        long rows = read(FilterPredicate.eq("code", 3));

        assertThat(rows).isEqualTo(250);
        assertThat(events(BLOOM_EVENT).count()).isZero();
    }

    private long read(FilterPredicate filter) throws Exception {
        long rows = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE));
                RowReader rowReader = reader.buildRowReader().filter(filter).build()) {
            while (rowReader.hasNext()) {
                rowReader.next();
                rows++;
            }
        }
        awaitEvents();
        return rows;
    }
}
