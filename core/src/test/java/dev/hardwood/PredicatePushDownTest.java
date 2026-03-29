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

import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.MultiFileColumnReaders;
import dev.hardwood.reader.MultiFileParquetReader;
import dev.hardwood.reader.MultiFileRowReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;

import static org.assertj.core.api.Assertions.assertThat;

class PredicatePushDownTest {

    private static final Path INT_FILE = Paths.get("src/test/resources/filter_pushdown_int.parquet");
    private static final Path MIXED_FILE = Paths.get("src/test/resources/filter_pushdown_mixed.parquet");
    private static final Path LIST_FILE = Paths.get("src/test/resources/filter_pushdown_list.parquet");
    private static final Path NESTED_FILE = Paths.get("src/test/resources/filter_pushdown_nested.parquet");

    // ==================== ColumnReader with Filter ====================

    @Test
    void testColumnReaderFilterSkipsRowGroups() throws Exception {
        // RG0: id 1-100, RG1: id 101-200, RG2: id 201-300
        // Filter: id > 200 -> should only read RG2 (100 rows)
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(INT_FILE))) {
            FilterPredicate filter = FilterPredicate.gt("id", 200L);

            try (ColumnReader idReader = reader.createColumnReader("id", filter)) {
                int totalRows = 0;
                while (idReader.nextBatch()) {
                    long[] values = idReader.getLongs();
                    int count = idReader.getRecordCount();
                    totalRows += count;
                    for (int i = 0; i < count; i++) {
                        assertThat(values[i]).isGreaterThan(200L);
                    }
                }
                assertThat(totalRows).isEqualTo(100);
            }
        }
    }

    @Test
    void testColumnReaderFilterByIndex() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(INT_FILE))) {
            FilterPredicate filter = FilterPredicate.lt("id", 101L);

            // Column 0 is "id"
            try (ColumnReader idReader = reader.createColumnReader(0, filter)) {
                int totalRows = 0;
                while (idReader.nextBatch()) {
                    totalRows += idReader.getRecordCount();
                }
                assertThat(totalRows).isEqualTo(100);
            }
        }
    }

    @Test
    void testColumnReaderFilterMatchesAllRowGroups() throws Exception {
        // Filter that matches all row groups -> all 300 rows returned
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(INT_FILE))) {
            FilterPredicate filter = FilterPredicate.gt("id", 0L);

            try (ColumnReader idReader = reader.createColumnReader("id", filter)) {
                int totalRows = 0;
                while (idReader.nextBatch()) {
                    totalRows += idReader.getRecordCount();
                }
                assertThat(totalRows).isEqualTo(300);
            }
        }
    }

    @Test
    void testColumnReaderFilterMatchesNoRowGroups() throws Exception {
        // Filter that matches no row groups -> 0 rows returned
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(INT_FILE))) {
            FilterPredicate filter = FilterPredicate.gt("id", 300L);

            try (ColumnReader idReader = reader.createColumnReader("id", filter)) {
                assertThat(idReader.nextBatch()).isFalse();
            }
        }
    }

    @Test
    void testColumnReaderFilterMiddleRowGroup() throws Exception {
        // Filter: id >= 101 AND id <= 200 -> should read only RG1
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(INT_FILE))) {
            FilterPredicate filter = FilterPredicate.and(
                    FilterPredicate.gtEq("id", 101L),
                    FilterPredicate.ltEq("id", 200L)
            );

            try (ColumnReader idReader = reader.createColumnReader("id", filter)) {
                int totalRows = 0;
                while (idReader.nextBatch()) {
                    long[] values = idReader.getLongs();
                    int count = idReader.getRecordCount();
                    totalRows += count;
                    for (int i = 0; i < count; i++) {
                        assertThat(values[i]).isBetween(101L, 200L);
                    }
                }
                assertThat(totalRows).isEqualTo(100);
            }
        }
    }

    // ==================== RowReader with Filter ====================

    @Test
    void testRowReaderWithFilter() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(INT_FILE))) {
            FilterPredicate filter = FilterPredicate.gt("id", 200L);

            try (RowReader rows = reader.createRowReader(filter)) {
                int totalRows = 0;
                while (rows.hasNext()) {
                    rows.next();
                    totalRows++;
                    assertThat(rows.getLong("id")).isGreaterThan(200L);
                }
                assertThat(totalRows).isEqualTo(100);
            }
        }
    }

    @Test
    void testRowReaderWithProjectionAndFilter() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(INT_FILE))) {
            FilterPredicate filter = FilterPredicate.lt("id", 101L);
            ColumnProjection projection = ColumnProjection.columns("id", "label");

            try (RowReader rows = reader.createRowReader(projection, filter)) {
                int totalRows = 0;
                while (rows.hasNext()) {
                    rows.next();
                    totalRows++;
                    assertThat(rows.getLong("id")).isBetween(1L, 100L);
                    assertThat(rows.getString("label")).startsWith("rg1_");
                }
                assertThat(totalRows).isEqualTo(100);
            }
        }
    }

    @Test
    void testRowReaderFilterNoMatchReturnsZeroRows() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(INT_FILE))) {
            FilterPredicate filter = FilterPredicate.eq("id", 999L);

            try (RowReader rows = reader.createRowReader(filter)) {
                assertThat(rows.hasNext()).isFalse();
            }
        }
    }

    // ==================== Mixed Type Filters ====================

    @Test
    void testIntFilterOnMixedFile() throws Exception {
        // RG0: id 1-5, RG1: id 6-10, RG2: id 11-15
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(MIXED_FILE))) {
            FilterPredicate filter = FilterPredicate.gt("id", 10);

            try (ColumnReader idReader = reader.createColumnReader("id", filter)) {
                int totalRows = 0;
                while (idReader.nextBatch()) {
                    int[] values = idReader.getInts();
                    int count = idReader.getRecordCount();
                    totalRows += count;
                    for (int i = 0; i < count; i++) {
                        assertThat(values[i]).isGreaterThan(10);
                    }
                }
                assertThat(totalRows).isEqualTo(5);
            }
        }
    }

    @Test
    void testDoubleFilterOnMixedFile() throws Exception {
        // RG0: price 10-50, RG1: price 60-100, RG2: price 110-150
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(MIXED_FILE))) {
            FilterPredicate filter = FilterPredicate.ltEq("price", 50.0);

            try (ColumnReader priceReader = reader.createColumnReader("price", filter)) {
                int totalRows = 0;
                while (priceReader.nextBatch()) {
                    double[] values = priceReader.getDoubles();
                    int count = priceReader.getRecordCount();
                    totalRows += count;
                    for (int i = 0; i < count; i++) {
                        assertThat(values[i]).isLessThanOrEqualTo(50.0);
                    }
                }
                assertThat(totalRows).isEqualTo(5);
            }
        }
    }

    @Test
    void testFloatFilterOnMixedFile() throws Exception {
        // RG0: rating 1.0-5.0, RG1: rating 6.0-10.0, RG2: rating 1.5-5.5
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(MIXED_FILE))) {
            FilterPredicate filter = FilterPredicate.gt("rating", 5.5f);

            try (ColumnReader ratingReader = reader.createColumnReader("rating", filter)) {
                int totalRows = 0;
                while (ratingReader.nextBatch()) {
                    totalRows += ratingReader.getRecordCount();
                }
                // Only RG1 (rating 6.0-10.0) has values > 5.5
                assertThat(totalRows).isEqualTo(5);
            }
        }
    }

    @Test
    void testStringFilterOnMixedFile() throws Exception {
        // RG0: name apple-elderberry, RG1: fig-jackfruit, RG2: kiwi-orange
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(MIXED_FILE))) {
            // Filter for names >= "kiwi" -> should only read RG2
            FilterPredicate filter = FilterPredicate.gtEq("name", "kiwi");

            try (ColumnReader nameReader = reader.createColumnReader("name", filter)) {
                int totalRows = 0;
                while (nameReader.nextBatch()) {
                    totalRows += nameReader.getRecordCount();
                }
                assertThat(totalRows).isEqualTo(5);
            }
        }
    }

    @Test
    void testBooleanFilterOnMixedFile() throws Exception {
        // RG0: active all true, RG1: active all false, RG2: active mixed
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(MIXED_FILE))) {
            // Filter for active == false -> skip RG0 (all true)
            FilterPredicate filter = FilterPredicate.eq("active", false);

            try (ColumnReader activeReader = reader.createColumnReader("active", filter)) {
                int totalRows = 0;
                while (activeReader.nextBatch()) {
                    totalRows += activeReader.getRecordCount();
                }
                // RG1 (all false) + RG2 (mixed) = 10 rows
                assertThat(totalRows).isEqualTo(10);
            }
        }
    }

    // ==================== OR Filter ====================

    @Test
    void testOrFilterSelectsMultipleRowGroups() throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(INT_FILE))) {
            // id < 101 OR id > 200 -> should read RG0 and RG2
            FilterPredicate filter = FilterPredicate.or(
                    FilterPredicate.lt("id", 101L),
                    FilterPredicate.gt("id", 200L)
            );

            try (ColumnReader idReader = reader.createColumnReader("id", filter)) {
                int totalRows = 0;
                while (idReader.nextBatch()) {
                    totalRows += idReader.getRecordCount();
                }
                assertThat(totalRows).isEqualTo(200);
            }
        }
    }

    // ==================== Repeated (list) columns ====================

    @Test
    void testFilterOnRepeatedColumnViaRowReader() throws Exception {
        // RG0: scores 5-30, RG1: scores 100-200, RG2: scores 300-500
        // Filter: scores > 200 -> skip RG0 and RG1, keep only RG2
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(LIST_FILE))) {
            FilterPredicate filter = FilterPredicate.gt("scores", 200);

            try (RowReader rows = reader.createRowReader(filter)) {
                int totalRows = 0;
                while (rows.hasNext()) {
                    rows.next();
                    totalRows++;
                    assertThat(rows.getInt("id")).isGreaterThanOrEqualTo(7);
                }
                // RG2 has 3 records (rows 7, 8, 9)
                assertThat(totalRows).isEqualTo(3);
            }
        }
    }

    @Test
    void testFilterOnRepeatedColumnKeepsMatchingRowGroups() throws Exception {
        // Filter: scores < 50 -> matches RG0 (5-30), skip RG1 (100-200) and RG2 (300-500)
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(LIST_FILE))) {
            FilterPredicate filter = FilterPredicate.lt("scores", 50);

            try (RowReader rows = reader.createRowReader(filter)) {
                int totalRows = 0;
                while (rows.hasNext()) {
                    rows.next();
                    totalRows++;
                }
                assertThat(totalRows).isEqualTo(3);
            }
        }
    }

    @Test
    void testFilterOnFlatColumnFiltersRepeatedColumn() throws Exception {
        // Filter on flat "id" column, read all columns including repeated "scores"
        // id > 6 -> skip RG0 (id 1-3) and RG1 (id 4-6), keep RG2 (id 7-9)
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(LIST_FILE))) {
            FilterPredicate filter = FilterPredicate.gt("id", 6);

            try (RowReader rows = reader.createRowReader(filter)) {
                int totalRows = 0;
                while (rows.hasNext()) {
                    rows.next();
                    totalRows++;
                    assertThat(rows.getInt("id")).isGreaterThanOrEqualTo(7);
                }
                assertThat(totalRows).isEqualTo(3);
            }
        }
    }

    @Test
    void testFilterOnRepeatedColumnMatchesNoRowGroups() throws Exception {
        // Filter: scores > 500 -> no row group matches
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(LIST_FILE))) {
            FilterPredicate filter = FilterPredicate.gt("scores", 500);

            try (RowReader rows = reader.createRowReader(filter)) {
                assertThat(rows.hasNext()).isFalse();
            }
        }
    }

    // ==================== Nested struct columns ====================

    @Test
    void testFilterOnNestedColumnByDottedPath() throws Exception {
        // RG0: zip 70000-72000, RG1: zip 80000-82000, RG2: zip 90000-92000
        // Filter: address.zip > 82000 -> skip RG0 and RG1, keep only RG2
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(NESTED_FILE))) {
            FilterPredicate filter = FilterPredicate.gt("address.zip", 82000);

            try (RowReader rows = reader.createRowReader(filter)) {
                int totalRows = 0;
                while (rows.hasNext()) {
                    rows.next();
                    totalRows++;
                    assertThat(rows.getInt("id")).isGreaterThanOrEqualTo(7);
                }
                assertThat(totalRows).isEqualTo(3);
            }
        }
    }

    @Test
    void testFilterOnNestedStringColumn() throws Exception {
        // RG0: city Austin-Chicago, RG1: city Denver-Fresno, RG2: city Gary-Irvine
        // Filter: address.city >= "Gary" -> skip RG0 and RG1, keep RG2
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(NESTED_FILE))) {
            FilterPredicate filter = FilterPredicate.gtEq("address.city", "Gary");

            try (RowReader rows = reader.createRowReader(filter)) {
                int totalRows = 0;
                while (rows.hasNext()) {
                    rows.next();
                    totalRows++;
                }
                assertThat(totalRows).isEqualTo(3);
            }
        }
    }

    @Test
    void testFilterOnNestedColumnMatchesNoRowGroups() throws Exception {
        // Filter: address.zip > 92000 -> no matches
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(NESTED_FILE))) {
            FilterPredicate filter = FilterPredicate.gt("address.zip", 92000);

            try (RowReader rows = reader.createRowReader(filter)) {
                assertThat(rows.hasNext()).isFalse();
            }
        }
    }

    // ==================== Filter on non-filtered column ====================

    @Test
    void testFilterOnDifferentColumnThanRead() throws Exception {
        // Filter on "id" but read "value" column
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(INT_FILE))) {
            FilterPredicate filter = FilterPredicate.gt("id", 200L);

            try (ColumnReader valueReader = reader.createColumnReader("value", filter)) {
                int totalRows = 0;
                while (valueReader.nextBatch()) {
                    long[] values = valueReader.getLongs();
                    int count = valueReader.getRecordCount();
                    totalRows += count;
                    for (int i = 0; i < count; i++) {
                        assertThat(values[i]).isGreaterThan(200L);
                    }
                }
                assertThat(totalRows).isEqualTo(100);
            }
        }
    }

    // ==================== Multi-file RowReader with Filter ====================

    @Test
    void testMultiFileRowReaderWithFilter() throws Exception {
        // Two copies of INT_FILE: 300 rows each, 600 total
        // Filter: id > 200 -> RG2 from each file = 200 rows
        List<InputFile> files = InputFile.ofPaths(List.of(INT_FILE, INT_FILE));
        FilterPredicate filter = FilterPredicate.gt("id", 200L);

        try (Hardwood hardwood = Hardwood.create();
             MultiFileParquetReader parquet = hardwood.openAll(files);
             MultiFileRowReader rows = parquet.createRowReader(filter)) {

            int totalRows = 0;
            while (rows.hasNext()) {
                rows.next();
                totalRows++;
                assertThat(rows.getLong("id")).isGreaterThan(200L);
            }
            assertThat(totalRows).isEqualTo(200);
        }
    }

    @Test
    void testMultiFileRowReaderWithProjectionAndFilter() throws Exception {
        List<InputFile> files = InputFile.ofPaths(List.of(INT_FILE, INT_FILE));
        FilterPredicate filter = FilterPredicate.lt("id", 101L);
        ColumnProjection projection = ColumnProjection.columns("id", "label");

        try (Hardwood hardwood = Hardwood.create();
             MultiFileParquetReader parquet = hardwood.openAll(files);
             MultiFileRowReader rows = parquet.createRowReader(projection, filter)) {

            int totalRows = 0;
            while (rows.hasNext()) {
                rows.next();
                totalRows++;
                assertThat(rows.getLong("id")).isBetween(1L, 100L);
                assertThat(rows.getString("label")).startsWith("rg1_");
            }
            assertThat(totalRows).isEqualTo(200);
        }
    }

    @Test
    void testMultiFileRowReaderFilterMatchesNoRowGroups() throws Exception {
        List<InputFile> files = InputFile.ofPaths(List.of(INT_FILE, INT_FILE));
        FilterPredicate filter = FilterPredicate.gt("id", 300L);

        try (Hardwood hardwood = Hardwood.create();
             MultiFileParquetReader parquet = hardwood.openAll(files);
             MultiFileRowReader rows = parquet.createRowReader(filter)) {

            assertThat(rows.hasNext()).isFalse();
        }
    }

    // ==================== Multi-file ColumnReaders with Filter ====================

    @Test
    void testMultiFileColumnReadersWithFilter() throws Exception {
        // Filter: id > 200 -> RG2 from each file = 200 rows
        List<InputFile> files = InputFile.ofPaths(List.of(INT_FILE, INT_FILE));
        FilterPredicate filter = FilterPredicate.gt("id", 200L);

        try (Hardwood hardwood = Hardwood.create();
             MultiFileParquetReader parquet = hardwood.openAll(files);
             MultiFileColumnReaders columns = parquet.createColumnReaders(
                     ColumnProjection.columns("id", "value"), filter)) {

            ColumnReader idReader = columns.getColumnReader("id");
            ColumnReader valueReader = columns.getColumnReader("value");

            int totalRows = 0;
            while (idReader.nextBatch() & valueReader.nextBatch()) {
                int count = idReader.getRecordCount();
                long[] ids = idReader.getLongs();
                long[] values = valueReader.getLongs();
                for (int i = 0; i < count; i++) {
                    assertThat(ids[i]).isGreaterThan(200L);
                    assertThat(values[i]).isGreaterThan(200L);
                }
                totalRows += count;
            }
            assertThat(totalRows).isEqualTo(200);
        }
    }

    @Test
    void testMultiFileColumnReadersFilterMatchesNoRowGroups() throws Exception {
        List<InputFile> files = InputFile.ofPaths(List.of(INT_FILE, INT_FILE));
        FilterPredicate filter = FilterPredicate.eq("id", 999L);

        try (Hardwood hardwood = Hardwood.create();
             MultiFileParquetReader parquet = hardwood.openAll(files);
             MultiFileColumnReaders columns = parquet.createColumnReaders(
                     ColumnProjection.columns("id"), filter)) {

            ColumnReader idReader = columns.getColumnReader("id");
            assertThat(idReader.nextBatch()).isFalse();
        }
    }

    @Test
    void testMultiFileColumnReadersFilterOnDifferentColumn() throws Exception {
        // Filter on "id" but read "value" and "label"
        List<InputFile> files = InputFile.ofPaths(List.of(INT_FILE, INT_FILE));
        FilterPredicate filter = FilterPredicate.gt("id", 200L);

        try (Hardwood hardwood = Hardwood.create();
             MultiFileParquetReader parquet = hardwood.openAll(files);
             MultiFileColumnReaders columns = parquet.createColumnReaders(
                     ColumnProjection.columns("value", "label"), filter)) {

            ColumnReader valueReader = columns.getColumnReader("value");
            ColumnReader labelReader = columns.getColumnReader("label");

            int totalRows = 0;
            while (valueReader.nextBatch() & labelReader.nextBatch()) {
                int count = valueReader.getRecordCount();
                long[] values = valueReader.getLongs();
                String[] labels = labelReader.getStrings();
                for (int i = 0; i < count; i++) {
                    assertThat(values[i]).isGreaterThan(200L);
                    assertThat(labels[i]).startsWith("rg3_");
                }
                totalRows += count;
            }
            assertThat(totalRows).isEqualTo(200);
        }
    }

    // ==================== Record-Level Filtering ====================

    @Test
    void testRecordLevelFilterReturnsOnlyMatchingRows() throws Exception {
        // RG1 has id 101-200. EQ filter for id=150 should return exactly 1 row.
        // Without record-level filtering, all 100 rows from RG1 would be returned.
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(INT_FILE))) {
            FilterPredicate filter = FilterPredicate.eq("id", 150L);

            try (RowReader rows = reader.createRowReader(filter)) {
                int totalRows = 0;
                while (rows.hasNext()) {
                    rows.next();
                    totalRows++;
                    assertThat(rows.getLong("id")).isEqualTo(150L);
                }
                assertThat(totalRows).isEqualTo(1);
            }
        }
    }

    @Test
    void testRecordLevelFilterWithRangePredicate() throws Exception {
        // RG0: id 1-100. Filter: id > 90 AND id <= 100 → exactly 10 rows
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(INT_FILE))) {
            FilterPredicate filter = FilterPredicate.and(
                    FilterPredicate.gt("id", 90L),
                    FilterPredicate.ltEq("id", 100L));

            try (RowReader rows = reader.createRowReader(filter)) {
                int totalRows = 0;
                while (rows.hasNext()) {
                    rows.next();
                    totalRows++;
                    assertThat(rows.getLong("id")).isBetween(91L, 100L);
                }
                assertThat(totalRows).isEqualTo(10);
            }
        }
    }

    @Test
    void testRecordLevelFilterWithProjection() throws Exception {
        // EQ filter returns 1 row; verify projected columns are accessible
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(INT_FILE))) {
            FilterPredicate filter = FilterPredicate.eq("id", 250L);
            ColumnProjection projection = ColumnProjection.columns("id", "value", "label");

            try (RowReader rows = reader.createRowReader(projection, filter)) {
                assertThat(rows.hasNext()).isTrue();
                rows.next();
                assertThat(rows.getLong("id")).isEqualTo(250L);
                assertThat(rows.getLong("value")).isEqualTo(250L);
                assertThat(rows.getString("label")).isEqualTo("rg3_250");

                assertThat(rows.hasNext()).isFalse();
            }
        }
    }

    @Test
    void testRecordLevelFilterMultiFile() throws Exception {
        // Two copies of INT_FILE. EQ filter for id=50 → 1 matching row per file = 2 total
        List<InputFile> files = InputFile.ofPaths(List.of(INT_FILE, INT_FILE));
        FilterPredicate filter = FilterPredicate.eq("id", 50L);

        try (Hardwood hardwood = Hardwood.create();
             MultiFileParquetReader parquet = hardwood.openAll(files);
             MultiFileRowReader rows = parquet.createRowReader(filter)) {

            int totalRows = 0;
            while (rows.hasNext()) {
                rows.next();
                totalRows++;
                assertThat(rows.getLong("id")).isEqualTo(50L);
            }
            assertThat(totalRows).isEqualTo(2);
        }
    }

    @Test
    void testRecordLevelFilterNoMatch() throws Exception {
        // RG1 has id 101-200. Row-group stats keep RG1 for id > 100,
        // but no row has id == 999. Record-level filter should return zero rows.
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(INT_FILE))) {
            FilterPredicate filter = FilterPredicate.eq("id", 999L);

            try (RowReader rows = reader.createRowReader(filter)) {
                assertThat(rows.hasNext()).isFalse();
            }
        }
    }
}
