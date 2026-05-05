/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.jfr.AbstractJfrRecorderTest;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import jdk.jfr.consumer.RecordedEvent;

import static org.assertj.core.api.Assertions.assertThat;

/// Asserts that [SequentialFetchPlan]'s trailing-page early exit actually
/// short-circuits header scanning once the iterator's row cursor crosses
/// `matchingRows.endRow()`. Without this guard, a leading-prefix matching
/// range still costs a full column-chunk header walk — silently
/// regressing the win.
///
/// Constructs two fetch plans against the same column: one with
/// [RowRanges#ALL] (baseline) and one with `[0, 200)` on a 2000-row
/// fixture. The bounded plan must scan dramatically fewer pages; the
/// `pageCount` field of [dev.hardwood.jfr.RowGroupScannedEvent] is the
/// observable that distinguishes "stopped scanning" from "kept reading
/// headers but dropped them".
class SequentialFetchPlanEarlyExitTest extends AbstractJfrRecorderTest {

    private static final Path FIXTURE =
            Paths.get("src/test/resources/misaligned_pages_no_index.parquet");
    private static final String COLUMN = "wide";
    private static final long TOTAL_ROWS = 2_000;
    private static final long BOUNDED_END = 200L;

    /// Two synthetic row-group indices used to disambiguate the JFR events
    /// emitted by the two iterations against the same file/column.
    private static final int RG_INDEX_ALL = 0;
    private static final int RG_INDEX_BOUNDED = 1;

    @Test
    void testTrailingPageEarlyExitStopsHeaderScan() throws Exception {
        FileMetaData fileMetaData;
        FileSchema schema;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE))) {
            fileMetaData = reader.getFileMetaData();
            schema = reader.getFileSchema();
        }

        RowGroup rowGroup = fileMetaData.rowGroups().get(0);
        ColumnSchema columnSchema = schema.getColumn(COLUMN);
        int colIdx = schema.getColumns().indexOf(columnSchema);
        assertThat(colIdx).as("column '%s' must exist in the fixture", COLUMN).isNotNegative();
        ColumnChunk columnChunk = rowGroup.columns().get(colIdx);

        try (HardwoodContextImpl context = HardwoodContextImpl.create();
             InputFile fileForAll = InputFile.of(FIXTURE);
             InputFile fileForBounded = InputFile.of(FIXTURE)) {
            fileForAll.open();
            fileForBounded.open();

            SequentialFetchPlan allPlan = SequentialFetchPlan.build(
                    fileForAll, columnSchema, columnChunk, context,
                    RG_INDEX_ALL, fileForAll.name(), 0L,
                    List.of(), RowRanges.ALL, TOTAL_ROWS);
            drain(allPlan.pages());

            SequentialFetchPlan boundedPlan = SequentialFetchPlan.build(
                    fileForBounded, columnSchema, columnChunk, context,
                    RG_INDEX_BOUNDED, fileForBounded.name(), 0L,
                    List.of(), RowRanges.range(0, BOUNDED_END), TOTAL_ROWS);
            drain(boundedPlan.pages());
        }

        awaitEvents();

        int allPages = pageCountFor(RG_INDEX_ALL);
        int boundedPages = pageCountFor(RG_INDEX_BOUNDED);

        // The unbounded run must scan at least a few pages — the
        // fixture is sized for many small pages on the wide column.
        assertThat(allPages)
                .as("baseline pageCount for full column-chunk scan")
                .isGreaterThan(20);

        // The bounded run keeps only the leading 10% of rows, so it should
        // scan dramatically fewer pages. Asserting strictly less than a
        // quarter is tight enough to fail if the early-exit guard is ever
        // removed (the bounded run would then equal `allPages`).
        assertThat(boundedPages)
                .as("bounded pageCount must be strictly less than baseline")
                .isLessThan(allPages);
        assertThat(boundedPages)
                .as("early exit must trim trailing scans (allPages=%d)", allPages)
                .isLessThan(allPages / 4);
    }

    private static void drain(Iterator<PageInfo> iterator) {
        while (iterator.hasNext()) {
            iterator.next();
        }
    }

    private int pageCountFor(int rowGroupIndex) {
        List<RecordedEvent> matches = events("dev.hardwood.RowGroupScanned")
                .filter(e -> e.getInt("rowGroupIndex") == rowGroupIndex)
                .filter(e -> COLUMN.equals(e.getString("column")))
                .toList();
        assertThat(matches)
                .as("expected exactly one RowGroupScanned event for rg=%d, column=%s",
                        rowGroupIndex, COLUMN)
                .hasSize(1);
        return matches.get(0).getInt("pageCount");
    }
}
