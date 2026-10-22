/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.predicate.BoundsReadability;
import dev.hardwood.internal.predicate.FilterDecision;
import dev.hardwood.internal.predicate.FilterPredicateResolver;
import dev.hardwood.internal.predicate.LogContext;
import dev.hardwood.internal.predicate.ResolvedPredicate;
import dev.hardwood.internal.predicate.RowGroupFilterEvaluator;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;

import static dev.hardwood.internal.predicate.FilterDecision.ALWAYS_MATCHES;
import static dev.hardwood.internal.predicate.FilterDecision.CANNOT_MATCH;
import static dev.hardwood.internal.predicate.FilterDecision.MIGHT_MATCH;
import static org.assertj.core.api.Assertions.assertThat;

/// Holds the predicate corpus to what `tools/simple-datagen.py` wrote, so that a decoding defect
/// fails here, once, rather than passing on both sides of every predicate
/// [PredicatePathAgreementTest] answers.
class PredicateCorpusTest extends PredicateCorpus {

    /// `i32_req` is the only column whose row groups the harness sees decided as matching in full,
    /// which skips per-row evaluation. This pins that the multi-row-group layout keeps giving it such
    /// row groups, and that `i32`, holding the same values beside nulls, gets none.
    @Test
    void theMultiLayoutHoldsRowGroupsThatMatchInFull() throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(MULTI.path()), context)) {
            assertThat(rowGroupDecisions(reader, FilterPredicate.lt("i32_req", 0)))
                    .containsExactly(ALWAYS_MATCHES, ALWAYS_MATCHES, CANNOT_MATCH, CANNOT_MATCH);
            assertThat(rowGroupDecisions(reader, FilterPredicate.lt("i32", 0)))
                    .containsExactly(MIGHT_MATCH, MIGHT_MATCH, CANNOT_MATCH, CANNOT_MATCH);
        }
    }

    private static List<FilterDecision> rowGroupDecisions(ParquetFileReader reader, FilterPredicate predicate)
            throws IOException {
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(predicate, reader.getFileSchema());
        List<FilterDecision> decisions = new ArrayList<>();
        for (RowGroup rowGroup : reader.getFileMetaData().rowGroups()) {
            decisions.add(RowGroupFilterEvaluator.decideRowGroup(resolved, rowGroup, null, null,
                    new LogContext(null, ExceptionContext.UNKNOWN_ROW_GROUP), BoundsReadability.ALL));
        }
        return decisions;
    }

    /// Every value the oracle reads is the value the fixture generator wrote, through the logical
    /// and the physical accessor alike.
    @ParameterizedTest(name = "{0}")
    @MethodSource("layouts")
    void readsTheValuesTheFixtureHolds(Layout layout) {
        Map<String, List<String[]>> written = writtenValues(layout);
        List<String> differences = new ArrayList<>();
        for (Map.Entry<String, List<String[]>> column : written.entrySet()) {
            List<Object[]> read = values(layout, column.getKey());
            assertThat(read).as("%s: rows of '%s'", layout, column.getKey()).hasSameSizeAs(column.getValue());
            for (int row = 0; row < read.size(); row++) {
                String[] expected = column.getValue().get(row);
                String logical = rendered(read.get(row)[0]);
                String physical = rendered(read.get(row)[1]);
                if (!logical.equals(expected[0]) || !physical.equals(expected[1])) {
                    differences.add("row " + row + ", " + column.getKey() + ": read " + logical + " / " + physical
                            + ", written " + expected[0] + " / " + expected[1]);
                }
            }
        }
        assertThat(differences).as("%s", layout).isEmpty();
    }
}
