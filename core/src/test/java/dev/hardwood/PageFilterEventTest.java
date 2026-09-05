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
import dev.hardwood.schema.ColumnProjection;
import jdk.jfr.consumer.RecordedEvent;

import static org.assertj.core.api.Assertions.assertThat;

/// Asserts that Column Index page push-down emits `dev.hardwood.PageFilter`, one event
/// per column chunk whose pages the push-down considered.
///
/// The event exists to answer "how much did page pruning actually skip?", which
/// `RowGroupScanned` cannot: its `pageCount` is the kept count with no total to divide
/// by, and its `scanStrategy` reads `offset-index` whether or not a mask was applied.
/// So the cases pinned here are the ones that distinguish those outcomes — pages
/// skipped, no page skipped, every page skipped, and no filter at all.
class PageFilterEventTest extends AbstractJfrRecorderTest {

    /// 1 row group, 10000 rows, sorted `id` [0,9999] and `value` [1000,10999],
    /// Parquet v2 with a Column Index, 10 pages of 1024 values per column.
    private static final Path SORTED_FILE = Paths.get("src/test/resources/column_index_pushdown.parquet");

    /// Same geometry, with a `category` column cycling through 10 distinct values,
    /// so every page holds every category.
    private static final Path DICT_FILE = Paths.get("src/test/resources/column_index_pushdown_dict.parquet");

    private static final String PAGE_FILTER_EVENT = "dev.hardwood.PageFilter";

    private static final int PAGES_PER_COLUMN = 10;

    @Test
    void selectiveFilterReportsSkippedPagesPerColumn() throws Exception {
        // id < 1000 lives entirely in the first of the 10 pages. Both projected
        // columns are page-aligned on the same row boundaries, so each drops the
        // same nine pages.
        long rows = read(SORTED_FILE, ColumnProjection.columns("id", "value"),
                FilterPredicate.lt("id", 1000L));
        assertThat(rows).isEqualTo(1000);

        List<RecordedEvent> events = events(PAGE_FILTER_EVENT).toList();
        assertThat(events)
                .as("one event per projected column of the single row group")
                .hasSize(2);
        assertThat(events).extracting(e -> e.getString("column"))
                .containsExactlyInAnyOrder("id", "value");

        for (RecordedEvent event : events) {
            assertThat(event.getString("file")).endsWith("column_index_pushdown.parquet");
            assertThat(event.getString("predicate")).isEqualTo("lt(id, ?)");
            assertThat(event.getInt("rowGroupIndex")).isZero();
            assertThat(event.getInt("totalPages")).isEqualTo(PAGES_PER_COLUMN);
            assertThat(event.getInt("pagesKept")).isEqualTo(1);
            assertThat(event.getInt("pagesSkipped")).isEqualTo(PAGES_PER_COLUMN - 1);
        }
    }

    @Test
    void filterMatchingEveryPageReportsNothingSkipped() throws Exception {
        // Every page of `category` holds all ten distinct values, so the predicate
        // keeps all of them. The event must still fire: "the filter ran and pruned
        // nothing" is exactly what a skip ratio has to be able to say.
        long rows = read(DICT_FILE, ColumnProjection.columns("category"),
                FilterPredicate.eq("category", "cat_3"));
        assertThat(rows).isEqualTo(1000);

        RecordedEvent event = events(PAGE_FILTER_EVENT).findFirst().orElseThrow();
        assertThat(event.getString("column")).isEqualTo("category");
        assertThat(event.getInt("totalPages")).isGreaterThan(1);
        assertThat(event.getInt("pagesKept")).isEqualTo(event.getInt("totalPages"));
        assertThat(event.getInt("pagesSkipped")).isZero();
    }

    @Test
    void fullyPrunedColumnIsReported() throws Exception {
        // AND(id < 1000, id >= 9000) survives row-group statistics — the group's
        // [0,9999] bounds satisfy both leaves — but no page satisfies both, so the
        // column's fetch plan is empty. That is the most effective pruning there is
        // and the one case that emits no RowGroupScanned event at all, since no
        // indexed plan gets built.
        long rows = read(SORTED_FILE, ColumnProjection.columns("id"),
                FilterPredicate.and(
                        FilterPredicate.lt("id", 1000L),
                        FilterPredicate.gtEq("id", 9000L)));
        assertThat(rows).isZero();

        RecordedEvent event = events(PAGE_FILTER_EVENT).findFirst().orElseThrow();
        assertThat(event.getString("column")).isEqualTo("id");
        assertThat(event.getInt("totalPages")).isEqualTo(PAGES_PER_COLUMN);
        assertThat(event.getInt("pagesKept")).isZero();
        assertThat(event.getInt("pagesSkipped")).isEqualTo(PAGES_PER_COLUMN);

        assertThat(events("dev.hardwood.RowGroupScanned").count())
                .as("no indexed plan is built for a fully pruned column")
                .isZero();
    }

    @Test
    void tailReadWithoutPredicateEmitsNoPageFilterEvent() throws Exception {
        // A tail read carries no predicate: it synthesizes a row range to skip the
        // rows before the tail, and the pages that range drops were dropped by
        // `tail(N)`, not by the Column Index. Reporting them would overstate what
        // push-down achieved on a read that did none.
        long rows = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(SORTED_FILE));
             RowReader rowReader = reader.buildRowReader()
                     .projection(ColumnProjection.columns("id"))
                     .tail(1000)
                     .build()) {
            while (rowReader.hasNext()) {
                rowReader.next();
                rows++;
            }
        }
        awaitEvents();
        assertThat(rows).isEqualTo(1000);

        assertThat(events(PAGE_FILTER_EVENT).count())
                .as("no predicate ran, so no page was filtered by push-down")
                .isZero();
    }

    @Test
    void unfilteredReadEmitsNoPageFilterEvent() throws Exception {
        long rows = read(SORTED_FILE, ColumnProjection.columns("id", "value"), null);
        assertThat(rows).isEqualTo(10000);

        assertThat(events(PAGE_FILTER_EVENT).count())
                .as("absence of the event means no page was a candidate for skipping")
                .isZero();
        assertThat(events("dev.hardwood.RowGroupScanned").count())
                .as("the read did happen — the sibling event proves the recording caught it")
                .isGreaterThan(0);
    }

    /// Reads `file` through a row reader and returns the number of rows produced,
    /// then stops the recording.
    private long read(Path file, ColumnProjection projection, FilterPredicate filter) throws Exception {
        long rows = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            ParquetFileReader.RowReaderBuilder builder = reader.buildRowReader().projection(projection);
            if (filter != null) {
                builder = builder.filter(filter);
            }
            try (RowReader rowReader = builder.build()) {
                while (rowReader.hasNext()) {
                    rowReader.next();
                    rows++;
                }
            }
        }
        awaitEvents();
        return rows;
    }
}
