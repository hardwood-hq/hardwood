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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import dev.hardwood.jfr.AbstractJfrRecorderTest;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowGroupPredicate;
import dev.hardwood.schema.ColumnProjection;
import jdk.jfr.consumer.RecordedEvent;

import static org.assertj.core.api.Assertions.assertThat;

/// Asserts the two row-group filtering stages each emit their own JFR event exactly once
/// for a single-file read: `dev.hardwood.RowGroupByteRangeFilter` for byte-range split
/// selection (emitted by [ParquetFileReader]) and `dev.hardwood.RowGroupFilter` for
/// statistics predicate push-down (emitted by the row-group iterator).
///
/// Regression guard for #718: the column-readers path used to evaluate the first file's
/// statistics twice — once eagerly in [ParquetFileReader] and again in the iterator —
/// emitting two `RowGroupFilter` events whose second `totalRowGroups` reported the
/// already-pruned count rather than the file's real total.
class RowGroupFilterEventTest extends AbstractJfrRecorderTest {

    private static final Path FIXTURE = Paths.get("src/test/resources/filter_pushdown_int.parquet");
    private static final String BYTE_RANGE_EVENT = "dev.hardwood.RowGroupByteRangeFilter";
    private static final String PUSH_DOWN_EVENT = "dev.hardwood.RowGroupFilter";

    private static long fileLen;

    @BeforeAll
    static void readFixture() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE))) {
            assertThat(reader.getFileMetaData().rowGroups()).hasSize(3);
        }
        fileLen = FIXTURE.toFile().length();
    }

    @Test
    void predicatePushDownEmitsSinglePushDownEventAndNoByteRangeEvent() throws Exception {
        readIdColumn(reader -> reader.buildColumnReaders(ColumnProjection.columns("id"))
                .filter(FilterPredicate.gt("id", 150L))
                .build());

        List<RecordedEvent> pushDown = events(PUSH_DOWN_EVENT).toList();
        assertThat(pushDown)
                .as("exactly one predicate-push-down event for the single file")
                .hasSize(1);
        assertThat(pushDown.get(0).getInt("totalRowGroups"))
                .as("total must be the file's real row-group count")
                .isEqualTo(3);
        assertThat(pushDown.getFirst().getInt("rowGroupsKept")).isEqualTo(2);
        assertThat(pushDown.getFirst().getInt("rowGroupsSkipped")).isEqualTo(1);

        assertThat(events(BYTE_RANGE_EVENT).count())
                .as("no byte-range event without a RowGroupPredicate")
                .isZero();
    }

    @Test
    void byteRangeEmitsByteRangeEventNotPushDownEvent() throws Exception {
        readIdColumn(reader -> reader.buildColumnReaders(ColumnProjection.columns("id"))
                .filter(RowGroupPredicate.byteRange(0, fileLen))
                .build());

        assertThat(events(BYTE_RANGE_EVENT).count())
                .as("byte-range selection emits exactly one byte-range event")
                .isEqualTo(1);
        assertThat(events(PUSH_DOWN_EVENT).count())
                .as("no push-down event without a FilterPredicate")
                .isZero();
    }

    @Test
    void byteRangeAndPredicateEmitOneEventEach() throws Exception {
        readIdColumn(reader -> reader.buildColumnReaders(ColumnProjection.columns("id"))
                .filter(FilterPredicate.gt("id", 150L))
                .filter(RowGroupPredicate.byteRange(0, fileLen))
                .build());

        assertThat(events(BYTE_RANGE_EVENT).count())
                .as("one byte-range event")
                .isEqualTo(1);
        assertThat(events(PUSH_DOWN_EVENT).count())
                .as("statistics evaluated once: one push-down event")
                .isEqualTo(1);
    }

    /// Builds a [ColumnReaders] over the fixture via `build`, then stops the recording.
    /// Single-file row-group filtering (both the byte-range pre-filter and the iterator's
    /// statistics push-down) runs eagerly during `build`, so no read is needed to emit the
    /// events.
    private void readIdColumn(ColumnReadersFactory build) throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE));
             ColumnReaders cols = build.create(reader)) {
            assertThat(cols.getColumnReader(0)).isNotNull();
        }
        awaitEvents();
    }

    @FunctionalInterface
    private interface ColumnReadersFactory {
        ColumnReaders create(ParquetFileReader reader);
    }
}
