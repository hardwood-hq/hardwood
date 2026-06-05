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
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;

import static org.assertj.core.api.Assertions.assertThat;

/// Exact-semantics tests for the column-reader filter (#624).
///
/// A `ColumnReader` / `ColumnReaders` configured with `.filter(pred)` must
/// yield **only** the matching rows — exact, no client-side residual — equal to
/// the row reader's filtered result. The bug these tests pin is that the column
/// path used to return a page-granular **superset**: it pruned row groups and
/// pages by statistics but returned every row of a surviving page.
///
/// The lever that exposes it is a **non-row-group-aligned** threshold. In
/// `filter_pushdown_int.parquet` each row group is exactly 100 rows
/// (RG0: id 1-100, RG1: 101-200, RG2: 201-300; `value == id`; one page per row
/// group), so `lt(id, 150)` keeps all of RG0 plus the whole RG1 page — 200 rows
/// — while the exact answer is 149.
class ColumnReaderExactFilterTest {

    private static final Path INT_FILE = Paths.get("src/test/resources/filter_pushdown_int.parquet");
    private static final Path LIST_FILE = Paths.get("src/test/resources/filter_pushdown_list.parquet");
    private static final Path NESTED_FILE = Paths.get("src/test/resources/filter_pushdown_nested.parquet");

    // ==================== Flat, drain-side-eligible predicate ====================

    @Test
    void selfColumnExact() throws Exception {
        // Read id, filter on id. Non-aligned threshold: exact = id 1..149.
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(INT_FILE));
             ColumnReader idReader = reader.buildColumnReader("id")
                     .filter(FilterPredicate.lt("id", 150L)).build()) {

            int count = 0;
            long sum = 0;
            while (idReader.nextBatch()) {
                int n = idReader.getRecordCount();
                long[] ids = idReader.getLongs();
                for (int i = 0; i < n; i++) {
                    assertThat(ids[i]).isLessThan(150L);
                    sum += ids[i];
                }
                count += n;
            }
            assertThat(count).isEqualTo(149);
            assertThat(sum).isEqualTo(11175L); // 1+...+149
        }
    }

    @Test
    void crossColumnExactPredicateNotRead() throws Exception {
        // Read value, filter on id (id is not otherwise read). value == id.
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(INT_FILE));
             ColumnReader valueReader = reader.buildColumnReader("value")
                     .filter(FilterPredicate.lt("id", 150L)).build()) {

            int count = 0;
            long sum = 0;
            while (valueReader.nextBatch()) {
                int n = valueReader.getRecordCount();
                long[] values = valueReader.getLongs();
                for (int i = 0; i < n; i++) {
                    assertThat(values[i]).isLessThan(150L);
                    sum += values[i];
                }
                count += n;
            }
            assertThat(count).isEqualTo(149);
            assertThat(sum).isEqualTo(11175L);
        }
    }

    @Test
    void prunedRowGroupPlusResidualWithinSurvivor() throws Exception {
        // gt(id, 250): RG0 and RG1 dropped by statistics; within RG2 (201-300)
        // the threshold is non-aligned. Exact = id 251..300 (50 rows).
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(INT_FILE));
             ColumnReader idReader = reader.buildColumnReader("id")
                     .filter(FilterPredicate.gt("id", 250L)).build()) {

            int count = 0;
            while (idReader.nextBatch()) {
                int n = idReader.getRecordCount();
                long[] ids = idReader.getLongs();
                for (int i = 0; i < n; i++) {
                    assertThat(ids[i]).isGreaterThan(250L);
                }
                count += n;
            }
            assertThat(count).isEqualTo(50);
        }
    }

    @Test
    void groupedPredicateNotInProjection() throws Exception {
        // Project value + label; filter on id (not projected). Exact = 149 rows.
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(INT_FILE));
             ColumnReaders columns = reader.buildColumnReaders(ColumnProjection.columns("value", "label"))
                     .filter(FilterPredicate.lt("id", 150L)).build()) {

            int count = 0;
            long sum = 0;
            while (columns.nextBatch()) {
                int n = columns.getRecordCount();
                long[] values = columns.getColumnReader("value").getLongs();
                String[] labels = columns.getColumnReader("label").getStrings();
                for (int i = 0; i < n; i++) {
                    assertThat(values[i]).isLessThan(150L);
                    assertThat(labels[i]).matches("rg[12]_\\d+");
                    sum += values[i];
                }
                count += n;
            }
            assertThat(count).isEqualTo(149);
            assertThat(sum).isEqualTo(11175L);
        }
    }

    @Test
    void groupedPredicateInProjection() throws Exception {
        // Project id + value; filter on id. Both columns compact identically.
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(INT_FILE));
             ColumnReaders columns = reader.buildColumnReaders(ColumnProjection.columns("id", "value"))
                     .filter(FilterPredicate.lt("id", 150L)).build()) {

            int count = 0;
            while (columns.nextBatch()) {
                int n = columns.getRecordCount();
                long[] ids = columns.getColumnReader("id").getLongs();
                long[] values = columns.getColumnReader("value").getLongs();
                for (int i = 0; i < n; i++) {
                    assertThat(ids[i]).isLessThan(150L);
                    assertThat(values[i]).isEqualTo(ids[i]);
                }
                count += n;
            }
            assertThat(count).isEqualTo(149);
        }
    }

    // ==================== Flat, drain-side-ineligible predicate (RowMatcher fallback) ====================

    @Test
    void flatStringEqualityUsesFallbackAndIsExact() throws Exception {
        // A string equality predicate is ineligible for the drain-side compiler;
        // it must still filter exactly via the RowMatcher fallback.
        // label "rg2_150" identifies the single row id == 150.
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(INT_FILE));
             ColumnReader idReader = reader.buildColumnReader("id")
                     .filter(FilterPredicate.eq("label", "rg2_150")).build()) {

            List<Long> ids = new ArrayList<>();
            while (idReader.nextBatch()) {
                long[] vals = idReader.getLongs();
                for (int i = 0; i < idReader.getRecordCount(); i++) {
                    ids.add(vals[i]);
                }
            }
            assertThat(ids).containsExactly(150L);
        }
    }

    // ==================== Nested predicate column (RowMatcher fallback) ====================

    @Test
    void nestedPredicateReadingFlatColumn() throws Exception {
        // Filter on a nested leaf (address.zip) — ineligible, fallback path.
        // zip is monotonic 70000..92000 over ids 1..9; lt(zip, 81000) keeps
        // ids 1..4. Row-group stats keep RG0 (<=72000) and RG1 (80000..82000),
        // so the superset would be ids 1..6 — the exact answer is ids 1..4.
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(NESTED_FILE));
             ColumnReaders columns = reader.buildColumnReaders(ColumnProjection.columns("id"))
                     .filter(FilterPredicate.lt("address.zip", 81000)).build()) {

            List<Integer> ids = new ArrayList<>();
            while (columns.nextBatch()) {
                int[] vals = columns.getColumnReader("id").getInts();
                for (int i = 0; i < columns.getRecordCount(); i++) {
                    ids.add(vals[i]);
                }
            }
            assertThat(ids).containsExactly(1, 2, 3, 4);
        }
    }

    @Test
    void nestedPredicateReadingNestedPayload() throws Exception {
        // Same predicate, reading a nested payload column (address.city).
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(NESTED_FILE));
             ColumnReaders columns = reader.buildColumnReaders(ColumnProjection.columns("address.city"))
                     .filter(FilterPredicate.lt("address.zip", 81000)).build()) {

            List<String> cities = new ArrayList<>();
            while (columns.nextBatch()) {
                ColumnReader city = columns.getColumnReader("address.city");
                String[] vals = city.getStrings();
                for (int i = 0; i < city.getValueCount(); i++) {
                    cities.add(vals[i]);
                }
            }
            assertThat(cities).containsExactly("Austin", "Boston", "Chicago", "Denver");
        }
    }

    // ==================== Nested payload column (eligible flat predicate) ====================

    @Test
    void nestedPayloadCompactsToMatchingRecords() throws Exception {
        // Read the list<int> leaf scores.list.element while filtering flat id.
        // lt(id, 5) keeps ids 1..4 (rows: [10,20,30],[5,15],[25],[100,200]).
        // Exact: 4 records, 8 leaf values summing 405. The superset would be
        // ids 1..6 (RG0 + RG1).
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(LIST_FILE));
             ColumnReaders columns = reader.buildColumnReaders(ColumnProjection.columns("scores.list.element"))
                     .filter(FilterPredicate.lt("id", 5)).build()) {

            int records = 0;
            int leaves = 0;
            long sum = 0;
            while (columns.nextBatch()) {
                ColumnReader scores = columns.getColumnReader("scores.list.element");
                records += scores.getRecordCount();
                int n = scores.getValueCount();
                int[] vals = scores.getInts();
                for (int i = 0; i < n; i++) {
                    sum += vals[i];
                }
                leaves += n;
            }
            assertThat(records).isEqualTo(4);
            assertThat(leaves).isEqualTo(8);
            assertThat(sum).isEqualTo(405L);
        }
    }

    // ==================== Multi-column merge (distinct predicate columns) ====================

    @Test
    void multiColumnAndExercisesMergeBranch() throws Exception {
        // Two *distinct* predicate columns force the cross-column MergePlan path
        // (not the single MergePlan.Column fast path). value == id, so
        // id > 150 AND value < 250 keeps id 151..249.
        FilterPredicate filter = FilterPredicate.and(
                FilterPredicate.gt("id", 150L),
                FilterPredicate.lt("value", 250L));
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(INT_FILE));
             ColumnReaders columns = reader.buildColumnReaders(ColumnProjection.columns("id", "value"))
                     .filter(filter).build()) {

            int count = 0;
            while (columns.nextBatch()) {
                int n = columns.getRecordCount();
                long[] ids = columns.getColumnReader("id").getLongs();
                long[] values = columns.getColumnReader("value").getLongs();
                for (int i = 0; i < n; i++) {
                    assertThat(ids[i]).isGreaterThan(150L);
                    assertThat(values[i]).isLessThan(250L);
                }
                count += n;
            }
            assertThat(count).isEqualTo(99); // 151..249
        }
    }

    // ==================== Nulls: payload compaction and three-valued logic ====================

    private static final Path NULLS_FILE = Paths.get("src/test/resources/plain_uncompressed_with_nulls.parquet");

    @Test
    void keptRecordWithNullCompactsValidity() throws Exception {
        // File: id=[1,2,3], name=["alice", null, "charlie"]. gt(id,1) keeps ids
        // 2 and 3 — and the kept id 2 has a NULL name, exercising the lazy
        // validity-bitmap materialisation in the compacted payload column.
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(NULLS_FILE));
             ColumnReader nameReader = reader.buildColumnReader("name")
                     .filter(FilterPredicate.gt("id", 1L)).build()) {

            assertThat(nameReader.nextBatch()).isTrue();
            assertThat(nameReader.getRecordCount()).isEqualTo(2);
            assertThat(nameReader.getLeafValidity().isNull(0)).isTrue();   // id 2 -> null
            assertThat(nameReader.getLeafValidity().isNotNull(1)).isTrue(); // id 3 -> charlie
            String[] names = nameReader.getStrings();
            assertThat(names[0]).isNull();
            assertThat(names[1]).isEqualTo("charlie");
            assertThat(nameReader.nextBatch()).isFalse();
        }
    }

    @Test
    void nullInPredicateColumnNeverMatches() throws Exception {
        // isNotNull(name) keeps only the non-null names (ids 1 and 3); the null
        // row (id 2) is excluded. Reading id verifies SQL three-valued logic on
        // the predicate column.
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(NULLS_FILE));
             ColumnReader idReader = reader.buildColumnReader("id")
                     .filter(FilterPredicate.isNotNull("name")).build()) {

            List<Long> ids = new ArrayList<>();
            while (idReader.nextBatch()) {
                long[] vals = idReader.getLongs();
                for (int i = 0; i < idReader.getRecordCount(); i++) {
                    ids.add(vals[i]);
                }
            }
            assertThat(ids).containsExactly(1L, 3L);
        }
    }

    // ==================== Equivalence: distinct == grouped == row reader ====================

    /// Predicate shapes spanning the implementation's branches: single-column
    /// drain-side, multi-column AND/OR merge, the `RowMatcher` fallback (string
    /// equality), and a `NOT` lowering. Each must agree across distinct single
    /// readers, the grouped reader, and the row reader.
    static Stream<Arguments> equivalenceFilters() {
        return Stream.of(
                Arguments.of("self/cross drain-side", FilterPredicate.lt("id", 150L)),
                Arguments.of("multi-column AND",
                        FilterPredicate.and(FilterPredicate.gt("id", 150L), FilterPredicate.lt("value", 250L))),
                Arguments.of("multi-column OR",
                        FilterPredicate.or(FilterPredicate.lt("id", 50L), FilterPredicate.gt("value", 250L))),
                Arguments.of("fallback (string eq)", FilterPredicate.eq("label", "rg2_150")),
                Arguments.of("NOT lowering", FilterPredicate.not(FilterPredicate.in("id", 50L, 150L, 250L))));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("equivalenceFilters")
    void distinctGroupedAndRowReaderAgree(String name, FilterPredicate filter) throws Exception {
        // Oracle: the row reader's filtered result. No projection restriction —
        // the row reader requires its predicate column to be readable, whereas
        // the column readers augment the projection internally.
        List<Long> oracleIds = new ArrayList<>();
        long oracleValueSum = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(INT_FILE));
             RowReader rows = reader.buildRowReader()
                     .filter(filter).build()) {
            while (rows.hasNext()) {
                rows.next();
                oracleIds.add(rows.getLong("id"));
                oracleValueSum += rows.getLong("value");
            }
        }

        // Distinct single readers, each independently filtered.
        List<Long> distinctIds = new ArrayList<>();
        long distinctValueSum = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(INT_FILE));
             ColumnReader idReader = reader.buildColumnReader("id").filter(filter).build();
             ColumnReader valueReader = reader.buildColumnReader("value").filter(filter).build()) {
            while (idReader.nextBatch()) {
                long[] ids = idReader.getLongs();
                for (int i = 0; i < idReader.getRecordCount(); i++) {
                    distinctIds.add(ids[i]);
                }
            }
            while (valueReader.nextBatch()) {
                long[] values = valueReader.getLongs();
                for (int i = 0; i < valueReader.getRecordCount(); i++) {
                    distinctValueSum += values[i];
                }
            }
        }

        // Grouped readers over one shared selection.
        List<Long> groupedIds = new ArrayList<>();
        long groupedValueSum = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(INT_FILE));
             ColumnReaders columns = reader.buildColumnReaders(ColumnProjection.columns("id", "value"))
                     .filter(filter).build()) {
            while (columns.nextBatch()) {
                int n = columns.getRecordCount();
                long[] ids = columns.getColumnReader("id").getLongs();
                long[] values = columns.getColumnReader("value").getLongs();
                for (int i = 0; i < n; i++) {
                    groupedIds.add(ids[i]);
                    groupedValueSum += values[i];
                }
            }
        }

        assertThat(distinctIds).isEqualTo(oracleIds);
        assertThat(groupedIds).isEqualTo(oracleIds);
        assertThat(distinctValueSum).isEqualTo(oracleValueSum);
        assertThat(groupedValueSum).isEqualTo(oracleValueSum);
    }
}
