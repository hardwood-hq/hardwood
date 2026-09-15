/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// Asserts that [SequentialFetchPlan]'s trailing-page early exit actually
/// short-circuits header scanning once the iterator's row cursor crosses
/// `matchingRows.endRow()`. Without this guard, a leading-prefix matching
/// range still costs a full column-chunk header walk — silently
/// regressing the win.
///
/// Constructs two fetch plans against the same column: one with
/// [RowRanges#ALL] (baseline) and one with `[0, 200)` on a 2000-row
/// fixture. The bounded plan must scan dramatically fewer pages;
/// [SequentialFetchPlan#scannedPages()] is the observable that distinguishes
/// "stopped scanning" from "kept reading headers but dropped them", and it is
/// final once the walk is drained.
class SequentialFetchPlanEarlyExitTest {

    private static final Path FIXTURE =
            Paths.get("src/test/resources/misaligned_pages_no_index.parquet");
    private static final String COLUMN = "wide";
    private static final long TOTAL_ROWS = 2_000;
    private static final long BOUNDED_END = 200L;
    private static final int ROW_GROUP = 0;

    @Test
    void testTrailingPageEarlyExitStopsHeaderScan() throws Exception {
        FileMetaData fileMetaData;
        FileSchema schema;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(FIXTURE))) {
            fileMetaData = reader.getFileMetaData();
            schema = reader.getFileSchema();
        }

        RowGroup rowGroup = fileMetaData.rowGroups().get(ROW_GROUP);
        ColumnSchema columnSchema = schema.getColumn(COLUMN);
        int colIdx = schema.getColumns().indexOf(columnSchema);
        assertThat(colIdx).as("column '%s' must exist in the fixture", COLUMN).isNotNegative();
        ColumnChunk columnChunk = rowGroup.columns().get(colIdx);

        int allPages;
        int boundedPages;
        try (HardwoodContextImpl context = HardwoodContextImpl.create();
             InputFile fileForAll = InputFile.of(FIXTURE);
             InputFile fileForBounded = InputFile.of(FIXTURE)) {
            fileForAll.open();
            fileForBounded.open();

            SequentialFetchPlan allPlan = SequentialFetchPlan.build(
                    fileForAll, columnSchema, columnChunk, context,
                    ROW_GROUP, fileForAll.name(), 0L,
                    List.of(), RowRanges.ALL, TOTAL_ROWS);
            drain(allPlan.pages());
            allPages = allPlan.scannedPages();

            SequentialFetchPlan boundedPlan = SequentialFetchPlan.build(
                    fileForBounded, columnSchema, columnChunk, context,
                    ROW_GROUP, fileForBounded.name(), 0L,
                    List.of(), RowRanges.range(0, BOUNDED_END), TOTAL_ROWS);
            drain(boundedPlan.pages());
            boundedPages = boundedPlan.scannedPages();
        }

        // The unbounded run must scan at least a few pages — the
        // fixture is sized for many small pages on the wide column.
        assertThat(allPages)
                .as("baseline pages scanned for full column-chunk scan")
                .isGreaterThan(20);

        // The bounded run keeps only the leading 10% of rows, so it should
        // scan dramatically fewer pages. Asserting strictly less than a
        // quarter is tight enough to fail if the early-exit guard is ever
        // removed (the bounded run would then equal `allPages`).
        assertThat(boundedPages)
                .as("bounded pages scanned must be strictly less than baseline")
                .isLessThan(allPages);
        assertThat(boundedPages)
                .as("early exit must trim trailing scans (allPages=%d)", allPages)
                .isLessThan(allPages / 4);
    }

    private static void drain(PageIterator iterator) throws IOException {
        while (iterator.hasNext()) {
            iterator.next();
        }
    }
}
