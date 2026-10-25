/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.plan;

import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import dev.hardwood.InputFile;
import dev.hardwood.internal.io.CountingInputFile;
import dev.hardwood.internal.reader.fetch.IoBudget;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;

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

    /// On a file whose footer offsets are consistent, the probe reads the header at
    /// `data_page_offset` and nothing else: one read per row group, although the nested column is
    /// dictionary-encoded and its chunk starts with the dictionary page.
    ///
    /// Uses nested_dict_v2.parquet: 500 rows in two row groups, a dictionary-encoded `tags` list,
    /// no Page Index, `DATA_PAGE_V2` pages.
    @Test
    void tailReadProbesAWellFormedDictionaryEncodedColumnOncePerRowGroup() throws Exception {
        CountingInputFile inputFile = new CountingInputFile(
                InputFile.of(Path.of("src/test/resources/nested_dict_v2.parquet")));
        inputFile.open();
        try (ParquetFileReader reader = ParquetFileReader.open(inputFile);
                RowReader rowReader = reader.buildRowReader().tail(300).build()) {
            while (rowReader.hasNext()) {
                rowReader.next();
            }
        }

        assertThat(inputFile.reads())
                .filteredOn(read -> read.reason().endsWith(" indexes"))
                .extracting(read -> read.reason())
                .containsExactly("rg=0 indexes", "rg=1 indexes");
    }

    /// A filter whose Column Index keeps one page of `id` puts a page mask on the row group, so
    /// the gate is probed: one page-header read of the v1 `tags` column, which closes it, beside
    /// the read of the page index.
    ///
    /// Uses page_index_empty_ranges_unindexed_column.parquet: `id` (0–149) with a page index, a
    /// flat `value` and a nested v1 `tags` column without one.
    @ParameterizedTest
    @EnumSource(ReadKind.class)
    void aFilteredReadWhosePageFilterKeepsAPageProbesTheNestedColumn(ReadKind kind) throws Exception {
        CountingInputFile inputFile = new CountingInputFile(
                InputFile.of(Path.of("src/test/resources/page_index_empty_ranges_unindexed_column.parquet")));
        inputFile.open();
        long rows;
        long tagsPage;
        try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
            tagsPage = reader.getFileMetaData().rowGroups().get(0).columns().get(2).metaData().dataPageOffset();
            rows = kind.count(reader, ColumnProjection.all(), FilterPredicate.eq("id", 120));
        }

        assertThat(rows).isEqualTo(1);
        List<CountingInputFile.Read> indexReads = inputFile.reads().stream()
                .filter(read -> read.reason().endsWith(" indexes"))
                .toList();
        // The ColumnIndex and the OffsetIndex of `id`, and the probe of `tags`
        assertThat(indexReads).extracting(CountingInputFile.Read::reason)
                .containsExactly("rg=0 indexes", "rg=0 indexes", "rg=0 indexes");
        assertThat(indexReads).filteredOn(read -> read.offset() == tagsPage).hasSize(1);
    }
}
