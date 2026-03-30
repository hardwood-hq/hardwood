/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    // ==================== Nested struct with logical type ====================

    private static final Path NESTED_TS_FILE = Paths.get("src/test/resources/filter_pushdown_nested_ts.parquet");

    @Test
    void testFilterOnNestedTimestampColumn() throws Exception {
        // event.ts: RG0 Jan 2024, RG1 Jun 2024, RG2 Dec 2024
        // Filter: event.ts >= 2024-06-01 → skip RG0, keep RG1 and RG2
        Instant cutoff = Instant.parse("2024-06-01T00:00:00Z");

        FilterPredicate filter = FilterPredicate.gtEq("event.ts", cutoff);
        List<Instant> timestamps = readFiltered(NESTED_TS_FILE, filter,
                r -> r.getStruct("event").getTimestamp("ts"));
            assertThat(timestamps).hasSize(6);
        for (Instant ts : timestamps) {
            assertThat(ts).isAfterOrEqualTo(cutoff);
        }
    }

    @Test
    void testFilterOnNestedTimestampSkipsAllRowGroups() throws Exception {
        // Filter: event.ts > 2025-01-01 → no row group matches
        Instant cutoff = Instant.parse("2025-01-01T00:00:00Z");

        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(NESTED_TS_FILE))) {
            FilterPredicate filter = FilterPredicate.gt("event.ts", cutoff);

            try (RowReader rows = reader.createRowReader(filter)) {
                assertThat(rows.hasNext()).isFalse();
            }
        }
    }

    @Test
    void testFilterOnNestedTimestampKeepsOnlyLastRowGroup() throws Exception {
        // Filter: event.ts >= 2024-12-01 → skip RG0 and RG1, keep only RG2
        Instant cutoff = Instant.parse("2024-12-01T00:00:00Z");

        FilterPredicate filter = FilterPredicate.gtEq("event.ts", cutoff);
        List<String> labels = readFiltered(NESTED_TS_FILE, filter,
                r -> r.getStruct("event").getString("label"));
        assertThat(labels).containsExactly("g", "h", "i");
    }

    // ==================== IN predicate end-to-end ====================

    @Test
    void testIntInPredicateEndToEnd() throws Exception {
        // INT_FILE: RG0 id 1-100, RG1 id 101-200, RG2 id 201-300
        // IN(id, 50, 250) → record-level filtering returns exactly 2 rows
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(INT_FILE))) {
            FilterPredicate filter = FilterPredicate.in("id", 50L, 250L);

            List<Long> ids = new ArrayList<>();
            try (RowReader rows = reader.createRowReader(filter)) {
                while (rows.hasNext()) {
                    rows.next();
                    ids.add(rows.getLong("id"));
                }
            }
            assertThat(ids).containsExactly(50L, 250L);
        }
    }

    @Test
    void testStringInPredicateEndToEnd() throws Exception {
        // INT_FILE: RG0 labels rg1_*, RG1 labels rg2_*, RG2 labels rg3_*
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(INT_FILE))) {
            FilterPredicate filter = FilterPredicate.inStrings("label", "rg1_1", "rg3_300");

            List<String> labels = new ArrayList<>();
            try (RowReader rows = reader.createRowReader(filter)) {
                while (rows.hasNext()) {
                    rows.next();
                    labels.add(rows.getString("label"));
                }
            }
            assertThat(labels).containsExactly("rg1_1", "rg3_300");
        }
    }

    // ==================== NOT(IN) end-to-end ====================

    @Test
    void testNotInPredicateEndToEnd() throws Exception {
        // INT_FILE: id values 1-300 across 3 row groups
        // NOT(IN(50, 150, 250)) → AND(NOT_EQ(50), NOT_EQ(150), NOT_EQ(250)) → all rows except those three
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(INT_FILE))) {
            FilterPredicate filter = FilterPredicate.not(FilterPredicate.in("id", 50L, 150L, 250L));

            List<Long> ids = new ArrayList<>();
            try (RowReader rows = reader.createRowReader(filter)) {
                while (rows.hasNext()) {
                    rows.next();
                    ids.add(rows.getLong("id"));
                }
            }
            assertThat(ids).hasSize(297); // 300 - 3
            assertThat(ids).doesNotContain(50L, 150L, 250L);
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

    // ==================== Logical vs physical predicate equivalence ====================

    private static final Path LOGICAL_TYPES_FILE = Paths.get("src/test/resources/logical_types_test.parquet");

    @Test
    void datePredicateReturnsSameRowsAsPhysicalInt() throws Exception {
        // birth_date: [1990-01-15, 1985-06-30, 2000-12-25]
        // Filter: birth_date > 1990-01-15 → should return only 2000-12-25
        LocalDate cutoff = LocalDate.of(1990, 1, 15);
        FilterPredicate logical = FilterPredicate.gt("birth_date", cutoff);
        FilterPredicate physical = FilterPredicate.gt("birth_date", (int) cutoff.toEpochDay());

        List<LocalDate> expected = List.of(LocalDate.of(2000, 12, 25));
        assertThat(readFiltered(logical, r -> r.getDate("birth_date"))).isEqualTo(expected);
        assertThat(readFiltered(physical, r -> r.getDate("birth_date"))).isEqualTo(expected);
    }

    @Test
    void timestampPredicateReturnsSameRowsAsPhysicalLong() throws Exception {
        // created_at_micros: [2025-01-01T10:30:00.123456Z, 2025-01-02T14:45:30.654321Z, 2025-01-03T09:15:45.111222Z]
        // Filter: created_at_micros >= 2025-01-02T00:00:00Z → should return last 2 rows
        Instant cutoff = Instant.parse("2025-01-02T00:00:00Z");
        long cutoffMicros = Math.addExact(
                Math.multiplyExact(cutoff.getEpochSecond(), 1_000_000L),
                cutoff.getNano() / 1_000L);
        FilterPredicate logical = FilterPredicate.gtEq("created_at_micros", cutoff);
        FilterPredicate physical = FilterPredicate.gtEq("created_at_micros", cutoffMicros);

        List<Instant> expected = List.of(
                Instant.parse("2025-01-02T14:45:30.654321Z"),
                Instant.parse("2025-01-03T09:15:45.111222Z"));
        assertThat(readFiltered(logical, r -> r.getTimestamp("created_at_micros"))).isEqualTo(expected);
        assertThat(readFiltered(physical, r -> r.getTimestamp("created_at_micros"))).isEqualTo(expected);
    }

    @Test
    void timePredicateReturnsSameRowsAsPhysicalInt() throws Exception {
        // wake_time_millis: [07:30, 08:00, 06:45]
        // Filter: wake_time_millis < 07:30 → should return only 06:45
        LocalTime cutoff = LocalTime.of(7, 30);
        int cutoffMillis = Math.toIntExact(cutoff.toNanoOfDay() / 1_000_000L);
        FilterPredicate logical = FilterPredicate.lt("wake_time_millis", cutoff);
        FilterPredicate physical = FilterPredicate.lt("wake_time_millis", cutoffMillis);

        List<LocalTime> expected = List.of(LocalTime.of(6, 45));
        assertThat(readFiltered(logical, r -> r.getTime("wake_time_millis"))).isEqualTo(expected);
        assertThat(readFiltered(physical, r -> r.getTime("wake_time_millis"))).isEqualTo(expected);
    }

    @Test
    void decimalPredicateReturnsCorrectRows() throws Exception {
        // balance: [1234.56, 9876.54, 5555.55] (DECIMAL(10,2) stored as FIXED_LEN_BYTE_ARRAY)
        // Filter: balance > 5555.55 → should return only 9876.54
        FilterPredicate filter = FilterPredicate.gt("balance", new BigDecimal("5555.55"));

        List<BigDecimal> expected = List.of(new BigDecimal("9876.54"));
        assertThat(readFiltered(filter, r -> r.getDecimal("balance"))).isEqualTo(expected);
    }

    private <T> List<T> readFiltered(FilterPredicate filter, RowValueExtractor<T> extractor) throws Exception {
        return readFiltered(LOGICAL_TYPES_FILE, filter, extractor);
    }

    private <T> List<T> readFiltered(Path file, FilterPredicate filter, RowValueExtractor<T> extractor) throws Exception {
        List<T> values = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file));
                RowReader rows = reader.createRowReader(filter)) {
            while (rows.hasNext()) {
                rows.next();
                values.add(extractor.extract(rows));
            }
        }
        return values;
    }

    @FunctionalInterface
    private interface RowValueExtractor<T> {
        T extract(RowReader row);
    }

    // ==================== Type mismatch ====================

    @Test
    void intPredicateOnStringColumnThrowsAtReaderCreation() throws Exception {
        // "name" is a STRING (BYTE_ARRAY) column; applying an int predicate must fail immediately
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(MIXED_FILE))) {
            assertThatThrownBy(() -> reader.createRowReader(FilterPredicate.eq("name", 42)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Column 'name' has physical type BYTE_ARRAY"
                            + "; given filter predicate type INT32 is incompatible");
        }
    }

    @Test
    void stringPredicateOnIntColumnThrowsAtReaderCreation() throws Exception {
        // "id" is an INT32 column; applying a string predicate must fail immediately
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(MIXED_FILE))) {
            assertThatThrownBy(() -> reader.createRowReader(FilterPredicate.eq("id", "hello")))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Column 'id' has physical type INT32"
                            + "; given filter predicate type BYTE_ARRAY is incompatible");
        }
    }

    @Test
    void longPredicateOnDoubleColumnThrowsAtReaderCreation() throws Exception {
        // "price" is a DOUBLE column; applying a long predicate must fail immediately
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(MIXED_FILE))) {
            assertThatThrownBy(() -> reader.createColumnReader("price", FilterPredicate.gt("price", 100L)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Column 'price' has physical type DOUBLE"
                            + "; given filter predicate type INT64 is incompatible");
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

    // ==================== IS NULL / IS NOT NULL end-to-end ====================

    private static final Path NULLS_FILE = Paths.get("src/test/resources/plain_uncompressed_with_nulls.parquet");

    @Test
    void testIsNullFilterReturnsOnlyNullRows() throws Exception {
        // File: id=[1,2,3], name=["alice", null, "charlie"]
        // IS NULL on "name" should return only row 1 (id=2)
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(NULLS_FILE))) {
            FilterPredicate filter = FilterPredicate.isNull("name");

            try (RowReader rows = reader.createRowReader(filter)) {
                int totalRows = 0;
                while (rows.hasNext()) {
                    rows.next();
                    totalRows++;
                    assertThat(rows.getLong("id")).isEqualTo(2L);
                    assertThat(rows.isNull("name")).isTrue();
                }
                assertThat(totalRows).isEqualTo(1);
            }
        }
    }

    @Test
    void testIsNotNullFilterReturnsOnlyNonNullRows() throws Exception {
        // File: id=[1,2,3], name=["alice", null, "charlie"]
        // IS NOT NULL on "name" should return rows 0 and 2 (id=1 and id=3)
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(NULLS_FILE))) {
            FilterPredicate filter = FilterPredicate.isNotNull("name");

            try (RowReader rows = reader.createRowReader(filter)) {
                List<Long> ids = new ArrayList<>();
                List<String> names = new ArrayList<>();
                while (rows.hasNext()) {
                    rows.next();
                    ids.add(rows.getLong("id"));
                    names.add(rows.getString("name"));
                }
                assertThat(ids).containsExactly(1L, 3L);
                assertThat(names).containsExactly("alice", "charlie");
            }
        }
    }

    @Test
    void testIsNotNullOnRequiredColumnReturnsAllRows() throws Exception {
        // IS NOT NULL on "id" (REQUIRED) -> all 3 rows
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(NULLS_FILE))) {
            FilterPredicate filter = FilterPredicate.isNotNull("id");

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
    void testIsNullCombinedWithAnd() throws Exception {
        // IS NULL on "name" AND id > 1 -> only id=2 matches (the null row)
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(NULLS_FILE))) {
            FilterPredicate filter = FilterPredicate.and(
                    FilterPredicate.isNull("name"),
                    FilterPredicate.gt("id", 1L));

            try (RowReader rows = reader.createRowReader(filter)) {
                int totalRows = 0;
                while (rows.hasNext()) {
                    rows.next();
                    totalRows++;
                    assertThat(rows.getLong("id")).isEqualTo(2L);
                }
                assertThat(totalRows).isEqualTo(1);
            }
        }
    }
}
