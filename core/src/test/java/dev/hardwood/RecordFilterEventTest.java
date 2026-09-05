/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.hardwood.jfr.AbstractJfrRecorderTest;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;
import jdk.jfr.consumer.RecordedEvent;

import static org.assertj.core.api.Assertions.assertThat;

/// Asserts that record-level filtering emits `dev.hardwood.RecordFilter`, one event per
/// file, across all three paths that evaluate a predicate per record: the drain-side
/// batch matchers, the row-reader wrapper they fall back to, and the column-reader
/// selection engine.
///
/// The fixture is 300 rows in 3 row groups of 100, `id` ascending from 1. With
/// `id > 150` the first row group never reaches the record filter at all, which is what
/// makes `totalRecords` (200) differ from the file's row count (300): the event reports
/// what the predicate decided on after row-group and page pruning, not what the file
/// holds.
class RecordFilterEventTest extends AbstractJfrRecorderTest {

    private static final Path FIXTURE = Paths.get("src/test/resources/filter_pushdown_int.parquet");

    private static final String RECORD_FILTER_EVENT = "dev.hardwood.RecordFilter";

    /// `id > 150`: row group 1 (ids 1-100) is dropped by statistics, row group 2
    /// (101-200) contributes 50 matches, row group 3 (201-300) matches in full.
    private static final FilterPredicate ID_OVER_150 = FilterPredicate.gt("id", 150L);

    private static final long EVALUATED = 200;
    private static final long KEPT = 150;

    @Test
    void drainSideFilterReportsCountsForItsFile() throws Exception {
        // A flat int comparison compiles to the drain-side batch matchers, where the
        // row group proven to match in full is never tested record by record. Those
        // records still count as evaluated and kept: the event reports the filter's
        // selectivity, not which mechanism decided each record.
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rows = reader.buildRowReader().filter(ID_OVER_150).build()) {
            assertThat(countRows(rows)).isEqualTo(KEPT);
        }
        awaitEvents();

        RecordedEvent event = singleEvent();
        assertThat(event.getString("file")).endsWith("filter_pushdown_int.parquet");
        assertThat(event.getString("predicate")).isEqualTo("gt(id, ?)");
        assertThat(event.getLong("totalRecords"))
                .as("only the surviving row groups reach the record filter")
                .isEqualTo(EVALUATED);
        assertThat(event.getLong("recordsKept")).isEqualTo(KEPT);
        assertThat(event.getLong("recordsSkipped")).isEqualTo(EVALUATED - KEPT);
    }

    @Test
    void rowReaderFallbackReportsCountsForItsFile() throws Exception {
        // A string predicate has no drain-side matcher, so this read goes through the
        // per-record wrapper instead — the path where the counts are gathered one
        // evaluation at a time.
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rows = reader.buildRowReader()
                     .filter(FilterPredicate.eq("label", "rg2_150"))
                     .build()) {
            assertThat(countRows(rows)).isEqualTo(1);
        }
        awaitEvents();

        RecordedEvent event = singleEvent();
        assertThat(event.getString("file")).endsWith("filter_pushdown_int.parquet");
        assertThat(event.getLong("recordsKept")).isEqualTo(1);
        assertThat(event.getLong("recordsSkipped"))
                .isEqualTo(event.getLong("totalRecords") - 1);
        assertThat(event.getLong("totalRecords"))
                .as("the label's own statistics leave one row group, and all 100 of its"
                        + " records are evaluated one at a time")
                .isEqualTo(100);
    }

    @Test
    void nestedReaderReportsCountsForItsFile() throws Exception {
        // The nested reader wires the tally separately from the flat one — its own
        // constructor and its own batch-swap hook — so it needs its own coverage.
        // 9 rows in 3 row groups of 3: zip > 81000 drops the first group on
        // statistics, leaves one match in the second and all three in the third.
        Path nested = Paths.get("src/test/resources/filter_pushdown_nested.parquet");
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(nested));
             RowReader rows = reader.buildRowReader()
                     .projection(ColumnProjection.columns("id", "address"))
                     .filter(FilterPredicate.gt("address.zip", 81000))
                     .build()) {
            assertThat(countRows(rows)).isEqualTo(4);
        }
        awaitEvents();

        RecordedEvent event = singleEvent();
        assertThat(event.getString("file")).endsWith("filter_pushdown_nested.parquet");
        assertThat(event.getString("predicate")).isEqualTo("gt(address.zip, ?)");
        assertThat(event.getLong("totalRecords")).isEqualTo(6);
        assertThat(event.getLong("recordsKept")).isEqualTo(4);
        assertThat(event.getLong("recordsSkipped")).isEqualTo(2);
    }

    @Test
    void columnReaderSelectionReportsCountsForItsFile() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE));
             ColumnReaders columns = reader.buildColumnReaders(ColumnProjection.columns("id"))
                     .filter(ID_OVER_150)
                     .build()) {
            ColumnReader id = columns.getColumnReader("id");
            long rows = 0;
            while (id.nextBatch()) {
                rows += id.getRecordCount();
            }
            assertThat(rows).isEqualTo(KEPT);
        }
        awaitEvents();

        RecordedEvent event = singleEvent();
        assertThat(event.getString("file")).endsWith("filter_pushdown_int.parquet");
        assertThat(event.getLong("totalRecords")).isEqualTo(EVALUATED);
        assertThat(event.getLong("recordsKept")).isEqualTo(KEPT);
        assertThat(event.getLong("recordsSkipped")).isEqualTo(EVALUATED - KEPT);
    }

    @Test
    void multiFileReadReportsOneEventPerFile(@TempDir Path tempDir) throws Exception {
        Path first = Files.copy(FIXTURE, tempDir.resolve("part-0.parquet"));
        Path second = Files.copy(FIXTURE, tempDir.resolve("part-1.parquet"));

        try (Hardwood hardwood = Hardwood.create();
             ParquetFileReader reader = hardwood.openAll(InputFile.ofPaths(List.of(first, second)));
             RowReader rows = reader.buildRowReader().filter(ID_OVER_150).build()) {
            assertThat(countRows(rows)).isEqualTo(2 * KEPT);
        }
        awaitEvents();

        List<RecordedEvent> events = events(RECORD_FILTER_EVENT).toList();
        assertThat(events)
                .as("the counts are attributed per file, not pooled across the read")
                .hasSize(2);
        assertThat(events).extracting(e -> Paths.get(e.getString("file")).getFileName().toString())
                .containsExactly("part-0.parquet", "part-1.parquet");
        for (RecordedEvent event : events) {
            assertThat(event.getLong("totalRecords")).isEqualTo(EVALUATED);
            assertThat(event.getLong("recordsKept")).isEqualTo(KEPT);
        }
    }

    @Test
    void multiFileColumnReaderReportsOneEventPerFile(@TempDir Path tempDir) throws Exception {
        // The column-reader path is the one place a batch is attributed to a file
        // through a reader accessor rather than a field on the batch itself, so the
        // assumption that its batches never straddle a file boundary needs its own
        // multi-file case — the row-reader one above cannot cover it.
        Path first = Files.copy(FIXTURE, tempDir.resolve("part-0.parquet"));
        Path second = Files.copy(FIXTURE, tempDir.resolve("part-1.parquet"));

        try (Hardwood hardwood = Hardwood.create();
             ParquetFileReader reader = hardwood.openAll(InputFile.ofPaths(List.of(first, second)));
             ColumnReaders columns = reader.buildColumnReaders(ColumnProjection.columns("id"))
                     .filter(ID_OVER_150)
                     .build()) {
            ColumnReader id = columns.getColumnReader("id");
            long rows = 0;
            while (id.nextBatch()) {
                rows += id.getRecordCount();
            }
            assertThat(rows).isEqualTo(2 * KEPT);
        }
        awaitEvents();

        List<RecordedEvent> events = events(RECORD_FILTER_EVENT).toList();
        assertThat(events)
                .as("the counts are attributed per file, not pooled across the read")
                .hasSize(2);
        assertThat(events).extracting(e -> Paths.get(e.getString("file")).getFileName().toString())
                .containsExactly("part-0.parquet", "part-1.parquet");
        for (RecordedEvent event : events) {
            assertThat(event.getLong("totalRecords")).isEqualTo(EVALUATED);
            assertThat(event.getLong("recordsKept")).isEqualTo(KEPT);
            assertThat(event.getLong("recordsSkipped")).isEqualTo(EVALUATED - KEPT);
        }
    }

    @Test
    void unfilteredReadEmitsNoRecordFilterEvent() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE));
             RowReader rows = reader.buildRowReader().build()) {
            assertThat(countRows(rows)).isEqualTo(300);
        }
        awaitEvents();

        assertThat(events(RECORD_FILTER_EVENT).count())
                .as("no predicate, no record-level evaluation to report")
                .isZero();
        assertThat(events("dev.hardwood.FileOpened").count())
                .as("the read did happen — the sibling event proves the recording caught it")
                .isGreaterThan(0);
    }

    private RecordedEvent singleEvent() {
        List<RecordedEvent> events = events(RECORD_FILTER_EVENT).toList();
        assertThat(events)
                .as("one event for the single file read")
                .hasSize(1);
        return events.getFirst();
    }

    private static long countRows(RowReader rows) {
        long count = 0;
        while (rows.hasNext()) {
            rows.next();
            count++;
        }
        return count;
    }
}
