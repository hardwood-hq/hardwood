/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;

/// The page-format probe behind the per-page mask gate reads a nested column's first data page
/// header. It runs only when a row group's plans would apply a page mask, so a read that needs no
/// mask issues no probe.
///
/// Uses nested_v1_no_index.parquet: 2000 rows, a flat `narrow` column and a `tags` list, no
/// Page Index, `DATA_PAGE` (v1) pages.
class MaskProbeIoTest {

    private static final Path FIXTURE = Path.of("src/test/resources/nested_v1_no_index.parquet");
    private static final int TOTAL_ROWS = 2_000;

    @Test
    void unfilteredScanIssuesNoProbe() throws Exception {
        CountingInputFile inputFile = new CountingInputFile(InputFile.of(FIXTURE));
        inputFile.open();
        List<String> columns;
        long rows = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(inputFile);
                RowReader rowReader = reader.rowReader()) {
            columns = reader.getFileSchema().getColumns().stream()
                    .map(column -> column.fieldPath().toString())
                    .toList();
            while (rowReader.hasNext()) {
                rowReader.next();
                rows++;
            }
        }

        assertThat(rows).isEqualTo(TOTAL_ROWS);
        assertThat(inputFile.reads())
                .filteredOn(read -> read.reason().endsWith(" indexes"))
                .isEmpty();
        IoBudget.of(FIXTURE, columns, 0, TOTAL_ROWS).assertWithin(inputFile);
    }

    @Test
    void tailReadProbesTheNestedColumn() throws Exception {
        CountingInputFile inputFile = new CountingInputFile(InputFile.of(FIXTURE));
        inputFile.open();
        try (ParquetFileReader reader = ParquetFileReader.open(inputFile);
                RowReader rowReader = reader.buildRowReader().tail(600).build()) {
            while (rowReader.hasNext()) {
                rowReader.next();
            }
        }

        // The gate decides whether tail(N) skips leading pages by mask, so it is probed: one
        // page-header read of the v1 `tags` column, which closes the gate.
        assertThat(inputFile.reads())
                .filteredOn(read -> read.reason().endsWith(" indexes"))
                .hasSize(1);
    }
}
