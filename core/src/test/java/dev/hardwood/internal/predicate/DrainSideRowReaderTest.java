/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// End-to-end coverage for the drain-side path in `FlatRowReader`: reads a real
/// Parquet file via the public API with a drain-eligible predicate and asserts
/// the surviving rows match what the predicate semantically demands. Exercises
/// `BatchMatchMerger` in its aliasing mode (compound merge and single-column
/// alias), the `nextSetBit` / `scanRunEnd` helpers, and the `combinedWords`
/// lifecycle across `loadNextBatch` — none of which the in-memory
/// `DrainSideOracleTest` touches, since it merges the bitmaps itself.
class DrainSideRowReaderTest {

    /// 3 row groups × 5 rows. Columns: id (INT32, 1..15), price (FLOAT64, sorted
    /// 10..150), rating (FLOAT32), name (STRING), active (BOOLEAN). See
    /// `tools/simple-datagen.py` for the fixture definition.
    private static final Path MIXED_FILE = Paths.get("src/test/resources/filter_pushdown_mixed.parquet");

    @Test
    void multiColumnAnd_drainSidePath_returnsExpectedRows() throws Exception {
        // id > 5 AND price < 100.0 — distinct top-level columns, supported (type, op):
        // takes the multi-column drain-side path via BatchFilterCompiler.tryCompile.
        FilterPredicate filter = FilterPredicate.and(
                FilterPredicate.gt("id", 5),
                FilterPredicate.lt("price", 100.0));

        List<Integer> expected = idsMatching(row -> row.id > 5 && row.price < 100.0);
        List<Integer> actual = idsWithFilter(filter);

        assertThat(actual).containsExactlyElementsOf(expected);
    }

    @Test
    void singleColumnDrainEligible_aliasesBatchMatches_returnsExpectedRows() throws Exception {
        // Single-leaf drain-eligible — exercises the `combinedWords` aliasing branch
        // in BatchMatchMerger where there is no merge to do.
        FilterPredicate filter = FilterPredicate.gt("id", 10);

        List<Integer> expected = idsMatching(row -> row.id > 10);
        List<Integer> actual = idsWithFilter(filter);

        assertThat(actual).containsExactlyElementsOf(expected);
    }

    @Test
    void sameColumnRange_composedViaAndBatchMatcher_returnsExpectedRows() throws Exception {
        // Two leaves on the same column compose via AndBatchMatcher; the result is
        // a single matcher in one column slot and the single-column intersect
        // fast path still applies.
        FilterPredicate filter = FilterPredicate.and(
                FilterPredicate.gtEq("id", 6),
                FilterPredicate.ltEq("id", 12));

        List<Integer> expected = idsMatching(row -> row.id >= 6 && row.id <= 12);
        List<Integer> actual = idsWithFilter(filter);

        assertThat(actual).containsExactlyElementsOf(expected);
    }

    @Test
    void nestedCompound_drainSidePath_returnsExpectedRows() throws Exception {
        // A compound whose child is itself a compound: the plan is
        // Or[Column(rating), And[Column(id), Column(price)]]. Every other case here
        // is one level deep, so this is the only one where BatchMatchMerger's plan
        // walk has to recurse to find a referenced column — a column it missed
        // would leave that bitmap slot unseated for the evaluator.
        FilterPredicate filter = FilterPredicate.or(
                FilterPredicate.and(
                        FilterPredicate.gt("id", 5),
                        FilterPredicate.lt("price", 100.0)),
                FilterPredicate.gt("rating", 5.0f));

        List<Integer> expected = idsMatching(
                row -> (row.id > 5 && row.price < 100.0) || row.rating > 5.0f);
        List<Integer> actual = idsWithFilter(filter);

        assertThat(actual).containsExactlyElementsOf(expected);
        assertThat(actual).containsExactly(6, 7, 8, 9, 10, 15);
    }

    @Test
    void emptyResultBatch_advancesToNextBatch_returnsEmpty() throws Exception {
        // id > 1000 matches nothing — every batch produces an all-zero combinedWords,
        // hitting the anyBit == 0L early-exit and the nextSetBit return-(-1) path.
        FilterPredicate filter = FilterPredicate.and(
                FilterPredicate.gt("id", 1000),
                FilterPredicate.lt("price", Double.MAX_VALUE));

        List<Integer> actual = idsWithFilter(filter);

        assertThat(actual).isEmpty();
    }

    @Test
    void matchAllPredicate_drainSidePath_returnsAllRows() throws Exception {
        // Match-all forces the consumer through every row via the runEndExclusive
        // fast path in hasNext — exercises scanRunEnd plus the dense iteration loop.
        FilterPredicate filter = FilterPredicate.and(
                FilterPredicate.gtEq("id", 0),
                FilterPredicate.lt("price", Double.MAX_VALUE));

        List<Integer> expected = idsMatching(row -> true);
        List<Integer> actual = idsWithFilter(filter);

        assertThat(actual).containsExactlyElementsOf(expected);
    }

    @Test
    void stringEq_drainSidePath_returnsExpectedRows() throws Exception {
        // String equality compiles to a byte-array matcher on the `name` column.
        FilterPredicate filter = FilterPredicate.eq("name", "grape");

        List<Integer> expected = idsMatching(row -> row.name.equals("grape"));

        assertThat(expected).as("fixture must contain a matching row").isNotEmpty();
        assertThat(idsWithFilter(filter)).containsExactlyElementsOf(expected);
    }

    @Test
    void stringRangeAndInt_drainSidePath_returnsExpectedRows() throws Exception {
        // name >= "c" AND id < 12 — a string range beside an int leaf, so the merge has to
        // combine a byte-array bitmap with a typed one.
        FilterPredicate filter = FilterPredicate.and(
                FilterPredicate.gtEq("name", "c"),
                FilterPredicate.lt("id", 12));

        List<Integer> expected = idsMatching(row -> row.name.compareTo("c") >= 0 && row.id < 12);

        assertThat(expected).as("fixture must contain matching rows").isNotEmpty();
        assertThat(idsWithFilter(filter)).containsExactlyElementsOf(expected);
    }

    @Test
    void stringIn_drainSidePath_returnsExpectedRows() throws Exception {
        // "missing" is absent from the fixture on purpose: a member matching nothing must not
        // change the result.
        FilterPredicate filter = FilterPredicate.inStrings("name", "banana", "kiwi", "missing");

        List<Integer> expected = idsMatching(row -> row.name.equals("banana") || row.name.equals("kiwi"));

        assertThat(expected).as("fixture must contain matching rows").isNotEmpty();
        assertThat(idsWithFilter(filter)).containsExactlyElementsOf(expected);
    }

    @Test
    void stringOrDouble_drainSidePath_returnsExpectedRows() throws Exception {
        // A string leaf OR'ed with a leaf on another column — merged by MergePlan.Or.
        FilterPredicate filter = FilterPredicate.or(
                FilterPredicate.eq("name", "apple"),
                FilterPredicate.gt("price", 140.0));

        List<Integer> expected = idsMatching(row -> row.name.equals("apple") || row.price > 140.0);

        assertThat(expected).as("fixture must contain matching rows").isNotEmpty();
        assertThat(idsWithFilter(filter)).containsExactlyElementsOf(expected);
    }

    /// 2 rows in one row group — `amount` 123.45 and 678.90 — with `id` (INT64) and `amount`, a
    /// top-level `DECIMAL(10, 2)` stored as `FIXED_LEN_BYTE_ARRAY`. Each literal below lies within the
    /// row group's `[min, max]`, so statistics decide neither way and the matcher runs. A decimal filter resolves to a byte-array leaf comparing two's
    /// complement bytes, so this is the byte-array matchers on a real file whose values are
    /// numbers rather than text. See `tools/simple-datagen.py`.
    private static final Path DECIMAL_FILE = Paths.get("src/test/resources/compat_decimal_10_2.parquet");

    @Test
    void fixedDecimalRange_drainSidePath_returnsExpectedRows() throws Exception {
        // Every value is padded to the column width, so the comparison is signed over equal
        // widths: the sign byte decides first, then the rest compares unsigned.
        // At the maximum: the bound itself must stay out.
        BigDecimal literal = new BigDecimal("678.90");
        FilterPredicate filter = FilterPredicate.lt("amount", literal);

        List<Long> expected = decimalIdsMatching(amount -> amount.compareTo(literal) < 0);
        assertThat(expected).as("the bound decides the one row at 678.90").containsExactly(1L);

        assertThat(expected).as("fixture must contain a matching row").isNotEmpty();
        assertThat(decimalIdsWithFilter(filter)).containsExactlyElementsOf(expected);
    }

    @Test
    void fixedDecimalEq_drainSidePath_matchesTheStoredValue() throws Exception {
        // Equality is byte equality here, which is sound only because a fixed-width column holds a
        // value as exactly one byte string.
        BigDecimal literal = new BigDecimal("678.90");
        FilterPredicate filter = FilterPredicate.eq("amount", literal);

        List<Long> expected = decimalIdsMatching(amount -> amount.compareTo(literal) == 0);

        assertThat(expected).as("fixture must contain a matching row").isNotEmpty();
        assertThat(decimalIdsWithFilter(filter)).containsExactlyElementsOf(expected);
    }

    @Test
    void fixedDecimalGtEq_drainSidePath_includesTheBound() throws Exception {
        // At the maximum: the bound itself must be kept. At the minimum, statistics would prove every
        // row matches and the matcher would never run.
        BigDecimal literal = new BigDecimal("678.90");
        FilterPredicate filter = FilterPredicate.gtEq("amount", literal);

        List<Long> expected = decimalIdsMatching(amount -> amount.compareTo(literal) >= 0);
        assertThat(expected).as("the bound decides the one row at 678.90").containsExactly(2L);

        assertThat(expected).as("fixture must contain matching rows").isNotEmpty();
        assertThat(decimalIdsWithFilter(filter)).containsExactlyElementsOf(expected);
    }

    private static List<Long> decimalIdsMatching(Predicate<BigDecimal> p) throws Exception {
        List<Long> out = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(DECIMAL_FILE));
             RowReader rows = reader.buildRowReader().build()) {
            while (rows.hasNext()) {
                rows.next();
                if (p.test(rows.getDecimal("amount"))) {
                    out.add(rows.getLong("id"));
                }
            }
        }
        return out;
    }

    private static List<Long> decimalIdsWithFilter(FilterPredicate filter) throws Exception {
        List<Long> out = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(DECIMAL_FILE));
             RowReader rows = reader.buildRowReader().filter(filter).build()) {
            assertDrainSide(filter, reader.getFileSchema());
            while (rows.hasNext()) {
                rows.next();
                out.add(rows.getLong("id"));
            }
        }
        return out;
    }

    private record Row(int id, double price, float rating, String name) {}

    private static List<Integer> idsMatching(Predicate<Row> p) throws Exception {
        List<Integer> out = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(MIXED_FILE));
             RowReader rows = reader.buildRowReader().build()) {
            while (rows.hasNext()) {
                rows.next();
                Row r = new Row(rows.getInt("id"), rows.getDouble("price"), rows.getFloat("rating"),
                        rows.getString("name"));
                if (p.test(r)) {
                    out.add(r.id);
                }
            }
        }
        return out;
    }

    private static List<Integer> idsWithFilter(FilterPredicate filter) throws Exception {
        List<Integer> out = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(MIXED_FILE));
             RowReader rows = reader.buildRowReader().filter(filter).build()) {
            assertDrainSide(filter, reader.getFileSchema());
            while (rows.hasNext()) {
                rows.next();
                out.add(rows.getInt("id"));
            }
        }
        return out;
    }

    /// Fails unless `filter` takes the drain-side path, applying the same gate `FlatRowReader`
    /// does: a non-null [BatchFilterCompiler#tryCompile] result. Without it, a filter that falls
    /// back to consumer-side filtering would still return the right rows and pass silently.
    private static void assertDrainSide(FilterPredicate filter, FileSchema schema) {
        ProjectedSchema projected = ProjectedSchema.create(schema, ColumnProjection.all());
        CompiledBatchFilter compiled = BatchFilterCompiler.tryCompile(
                FilterPredicateResolver.resolve(filter, schema), schema, projected::toProjectedIndex);
        assertThat(compiled).as("filter %s must be drain-side eligible", filter).isNotNull();
    }
}
