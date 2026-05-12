/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;

/// End-to-end coverage for the drain-side path in `FlatRowReader`: reads a real
/// Parquet file via the public API with a drain-eligible predicate and asserts
/// the surviving rows match what the predicate semantically demands. Exercises
/// `intersectMatches` (multi-column AND-merge and single-column alias), the
/// `nextSetBit` / `scanRunEnd` helpers, and the `combinedWords` lifecycle across
/// `loadNextBatch` — none of which the in-memory `DrainSideOracleTest` touches.
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
        // in intersectMatches where no AND-merge is required.
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

    private record Row(int id, double price) {}

    private static List<Integer> idsMatching(Predicate<Row> p) throws Exception {
        List<Integer> out = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(MIXED_FILE));
             RowReader rows = reader.buildRowReader().build()) {
            while (rows.hasNext()) {
                rows.next();
                Row r = new Row(rows.getInt("id"), rows.getDouble("price"));
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
            while (rows.hasNext()) {
                rows.next();
                out.add(rows.getInt("id"));
            }
        }
        return out;
    }
}
