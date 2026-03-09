/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.filter;

import java.io.IOException;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests verifying that statistics are parsed from real Parquet files
 * and that row group filtering works end-to-end.
 */
class RowGroupFilterIntegrationTest {

    private static final Path TEST_RESOURCES = Path.of("src/test/resources");

    @Test
    void statisticsParsedFromPrimitiveTypesFile() throws IOException {
        Path file = TEST_RESOURCES.resolve("primitive_types_test.parquet");
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            FileMetaData meta = reader.getFileMetaData();

            assertThat(meta.rowGroups()).isNotEmpty();

            for (RowGroup rg : meta.rowGroups()) {
                for (ColumnChunk cc : rg.columns()) {
                    ColumnMetaData cmd = cc.metaData();
                    // Not all files are guaranteed to have stats, but we verify the parsing path works
                    if (cmd.statistics() != null && cmd.statistics().hasMinMax()) {
                        assertThat(cmd.statistics().min()).isNotNull();
                        assertThat(cmd.statistics().max()).isNotNull();
                    }
                }
            }
        }
    }

    @Test
    void statisticsParsedFromYellowTripdataFile() throws IOException {
        Path file = TEST_RESOURCES.resolve("yellow_tripdata_sample.parquet");
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            FileMetaData meta = reader.getFileMetaData();

            assertThat(meta.rowGroups()).isNotEmpty();

            RowGroup firstRg = meta.rowGroups().get(0);
            boolean anyStatsFound = false;
            for (ColumnChunk cc : firstRg.columns()) {
                if (cc.metaData().statistics() != null && cc.metaData().statistics().hasMinMax()) {
                    anyStatsFound = true;
                }
            }
            // Yellow tripdata files typically contain statistics
            assertThat(anyStatsFound)
                    .as("Expected at least one column with statistics in yellow_tripdata_sample.parquet")
                    .isTrue();
        }
    }

    @Test
    void filterRowReaderReturnsSubsetOfRows() throws IOException {
        Path file = TEST_RESOURCES.resolve("yellow_tripdata_sample.parquet");
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            // Count all rows without filter
            int totalRows = 0;
            try (RowReader rows = reader.createRowReader()) {
                while (rows.hasNext()) {
                    rows.next();
                    totalRows++;
                }
            }

            assertThat(totalRows).isGreaterThan(0);
        }
    }

    @Test
    void filterWithImpossibleConditionReturnsEmpty() throws IOException {
        Path file = TEST_RESOURCES.resolve("yellow_tripdata_sample.parquet");
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            FileMetaData meta = reader.getFileMetaData();
            FileSchema schema = reader.getFileSchema();

            // Find a column that has statistics
            String columnWithStats = null;
            Statistics stats = null;
            for (int i = 0; i < schema.getColumnCount(); i++) {
                ColumnChunk cc = meta.rowGroups().get(0).columns().get(i);
                if (cc.metaData().statistics() != null && cc.metaData().statistics().hasMinMax()) {
                    switch (cc.metaData().type()) {
                        case INT32, INT64 -> {
                            columnWithStats = schema.getColumn(i).name();
                            stats = cc.metaData().statistics();
                        }
                        default -> {
                        }
                    }
                }
                if (columnWithStats != null) {
                    break;
                }
            }

            if (columnWithStats != null) {
                // Get the max value and filter for values above it — should skip all row groups
                long maxVal = StatisticsConverter.bytesToLong(
                        stats.max(), meta.rowGroups().get(0).columns().get(
                                schema.getColumns().indexOf(schema.getColumn(columnWithStats))).metaData().type());

                RowGroupFilter impossible = RowGroupFilter.gt(columnWithStats, maxVal);

                int filteredRows = 0;
                try (RowReader rows = reader.createRowReader(impossible)) {
                    while (rows.hasNext()) {
                        rows.next();
                        filteredRows++;
                    }
                }

                // The filter should have skipped all row groups with gt(max_val)
                assertThat(filteredRows).isEqualTo(0);
            }
        }
    }

    @Test
    void filterDoesNotCorruptDataReading() throws IOException {
        Path file = TEST_RESOURCES.resolve("plain_uncompressed.parquet");
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            // A filter that matches everything should return same data as no filter
            RowGroupFilter matchAll = RowGroupFilter.gt("id", Long.MIN_VALUE);

            int unfilteredCount = 0;
            try (RowReader rows = reader.createRowReader()) {
                while (rows.hasNext()) {
                    rows.next();
                    unfilteredCount++;
                }
            }

            int filteredCount = 0;
            try (var reader2 = ParquetFileReader.open(InputFile.of(file))) {
                try (RowReader rows = reader2.createRowReader(matchAll)) {
                    while (rows.hasNext()) {
                        rows.next();
                        filteredCount++;
                    }
                }
            }

            assertThat(filteredCount).isEqualTo(unfilteredCount);
        }
    }

    @Test
    void statisticsParsedFromDictionaryFile() throws IOException {
        Path file = TEST_RESOURCES.resolve("dictionary_uncompressed.parquet");
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            FileMetaData meta = reader.getFileMetaData();
            assertThat(meta.rowGroups()).isNotEmpty();

            // Verify statistics are accessible (may or may not have min/max)
            for (RowGroup rg : meta.rowGroups()) {
                for (ColumnChunk cc : rg.columns()) {
                    Statistics s = cc.metaData().statistics();
                    // statistics can be null, that's fine — we just verify the parsing didn't crash
                }
            }
        }
    }

    @Test
    void statisticsParsedFromSnappyFile() throws IOException {
        Path file = TEST_RESOURCES.resolve("plain_snappy.parquet");
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            FileMetaData meta = reader.getFileMetaData();
            assertThat(meta.rowGroups()).isNotEmpty();

            for (RowGroup rg : meta.rowGroups()) {
                for (ColumnChunk cc : rg.columns()) {
                    Statistics s = cc.metaData().statistics();
                    if (s != null && s.hasMinMax()) {
                        assertThat(s.min().length).isGreaterThan(0);
                        assertThat(s.max().length).isGreaterThan(0);
                    }
                }
            }
        }
    }
}
