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

/// Asserts that a row group dropped by its dictionaries is reported by
/// `dev.hardwood.RowGroupDictionaryFilter`, while `dev.hardwood.RowGroupFilter`, which reports the
/// statistics and bloom-filter decisions taken before the read reaches it, counts it as kept.
class RowGroupDictionaryFilterEventTest extends AbstractJfrRecorderTest {

    /// One row group, 10 000 rows; `category` cycles ten dictionary-encoded values `"cat_0"`…`"cat_9"`.
    private static final Path FIXTURE = Paths.get("src/test/resources/column_index_pushdown_dict.parquet");

    private static final String DICTIONARY_EVENT = "dev.hardwood.RowGroupDictionaryFilter";
    private static final String PUSH_DOWN_EVENT = "dev.hardwood.RowGroupFilter";

    @Test
    void rowGroupDroppedByItsDictionaryIsReportedOnce() throws Exception {
        // Between the statistics' "cat_0" and "cat_9", but in no dictionary.
        long rows = read(FilterPredicate.eq("category", "cat_5x"));

        assertThat(rows).isZero();
        List<RecordedEvent> dropped = events(DICTIONARY_EVENT).toList();
        assertThat(dropped).hasSize(1);
        assertThat(dropped.getFirst().getString("file")).endsWith("column_index_pushdown_dict.parquet");
        assertThat(dropped.getFirst().getInt("rowGroupIndex")).isZero();

        List<RecordedEvent> pushDown = events(PUSH_DOWN_EVENT).toList();
        assertThat(pushDown).hasSize(1);
        assertThat(pushDown.getFirst().getInt("rowGroupsKept"))
                .as("statistics keep the row group; its dictionary drops it later")
                .isEqualTo(1);
    }

    @Test
    void rowGroupItsDictionaryKeepsIsNotReported() throws Exception {
        long rows = read(FilterPredicate.eq("category", "cat_5"));

        assertThat(rows).isEqualTo(1000);
        assertThat(events(DICTIONARY_EVENT).count()).isZero();
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
