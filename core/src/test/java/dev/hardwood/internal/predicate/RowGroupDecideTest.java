/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.reader.FilterPredicate;

import static dev.hardwood.internal.predicate.StatsDecision.ALWAYS_MATCHES;
import static dev.hardwood.internal.predicate.StatsDecision.CANNOT_MATCH;
import static dev.hardwood.internal.predicate.StatsDecision.MIGHT_MATCH;
import static org.assertj.core.api.Assertions.assertThat;

/// [RowGroupFilterEvaluator#decideRowGroup] behavior over whole row groups: leaf decisions
/// with row-group [Statistics], `AND`/`OR` composition, null predicates, and the
/// equivalence of `canDropRowGroup` with the [StatsDecision#CANNOT_MATCH] decision.
class RowGroupDecideTest {

    private static final int COL = 0;

    @Test
    void rangePredicateOverFullySatisfyingRowGroup() {
        // Rows [10, 20], no nulls; GT 5 is satisfied by the whole interval.
        RowGroup rg = intRowGroup(10, 20, 0L);
        assertThat(decide(intGt(5), rg)).isEqualTo(ALWAYS_MATCHES);
        assertThat(decide(intGt(15), rg)).isEqualTo(MIGHT_MATCH);
        assertThat(decide(intGt(20), rg)).isEqualTo(CANNOT_MATCH);
    }

    @Test
    void nullsInRowGroupPreventAlwaysMatches() {
        RowGroup rg = intRowGroup(10, 20, 5L);
        assertThat(decide(intGt(5), rg)).isEqualTo(MIGHT_MATCH);
    }

    @Test
    void missingStatisticsYieldMightMatch() {
        RowGroup rg = rowGroup(PhysicalType.INT32, null, 100);
        assertThat(decide(intGt(5), rg)).isEqualTo(MIGHT_MATCH);
    }

    @Test
    void andComposition() {
        RowGroup rg = intRowGroup(10, 20, 0L);
        assertThat(decide(and(intGt(5), intLt(25)), rg)).isEqualTo(ALWAYS_MATCHES);
        assertThat(decide(and(intGt(5), intLt(15)), rg)).isEqualTo(MIGHT_MATCH);
        assertThat(decide(and(intGt(5), intGt(25)), rg)).isEqualTo(CANNOT_MATCH);
    }

    @Test
    void orComposition() {
        RowGroup rg = intRowGroup(10, 20, 0L);
        // One always-matching branch decides the disjunction.
        assertThat(decide(or(intGt(25), intGt(5)), rg)).isEqualTo(ALWAYS_MATCHES);
        assertThat(decide(or(intGt(25), intGt(15)), rg)).isEqualTo(MIGHT_MATCH);
        assertThat(decide(or(intGt(25), intLt(5)), rg)).isEqualTo(CANNOT_MATCH);
    }

    @Test
    void isNotNullDecisions() {
        assertThat(decide(isNotNull(), intRowGroup(10, 20, 0L))).isEqualTo(ALWAYS_MATCHES);
        assertThat(decide(isNotNull(), intRowGroup(10, 20, 5L))).isEqualTo(MIGHT_MATCH);
        // All 100 rows null
        assertThat(decide(isNotNull(), rowGroup(PhysicalType.INT32,
                new Statistics(null, null, 100L, null, false), 100))).isEqualTo(CANNOT_MATCH);
    }

    @Test
    void isNullNeverPromisesAlwaysMatches() {
        // Even a fully-null row group is not promised: null counts tally values, not rows.
        assertThat(decide(isNull(), rowGroup(PhysicalType.INT32,
                new Statistics(null, null, 100L, null, false), 100))).isEqualTo(MIGHT_MATCH);
        assertThat(decide(isNull(), intRowGroup(10, 20, 0L))).isEqualTo(CANNOT_MATCH);
    }

    @Test
    void bloomFilterSourceDoesNotAffectAlwaysMatches() {
        // A bloom filter proves absence only; its presence must not change the
        // always-matching decision derived from statistics.
        RowGroup rg = intRowGroup(10, 20, 0L);
        BloomFilterSource noFilters = columnIndex -> null;
        assertThat(RowGroupFilterEvaluator.decideRowGroup(intGt(5), rg, noFilters))
                .isEqualTo(ALWAYS_MATCHES);
    }

    @Test
    void canDropRowGroupIsTheCannotMatchDecision() {
        RowGroup rg = intRowGroup(10, 20, 0L);
        assertThat(RowGroupFilterEvaluator.canDropRowGroup(intGt(20), rg)).isTrue();
        assertThat(RowGroupFilterEvaluator.canDropRowGroup(intGt(15), rg)).isFalse();
        assertThat(RowGroupFilterEvaluator.canDropRowGroup(intGt(5), rg)).isFalse();
    }

    @Test
    void deprecatedMinMaxNeverPromisesAlwaysMatches() {
        Statistics deprecated = new Statistics(intBytes(10), intBytes(20), 0L, null, true);
        RowGroup rg = rowGroup(PhysicalType.INT32, deprecated, 100);
        assertThat(decide(intGt(5), rg)).isEqualTo(MIGHT_MATCH);
    }

    // ==================== Fixtures ====================

    private static StatsDecision decide(ResolvedPredicate predicate, RowGroup rowGroup) {
        return RowGroupFilterEvaluator.decideRowGroup(predicate, rowGroup, null);
    }

    private static ResolvedPredicate intGt(int value) {
        return new ResolvedPredicate.IntPredicate(COL, FilterPredicate.Operator.GT, value);
    }

    private static ResolvedPredicate intLt(int value) {
        return new ResolvedPredicate.IntPredicate(COL, FilterPredicate.Operator.LT, value);
    }

    private static ResolvedPredicate isNull() {
        return new ResolvedPredicate.IsNullPredicate(COL);
    }

    private static ResolvedPredicate isNotNull() {
        return new ResolvedPredicate.IsNotNullPredicate(COL);
    }

    private static ResolvedPredicate and(ResolvedPredicate... children) {
        return new ResolvedPredicate.And(List.of(children));
    }

    private static ResolvedPredicate or(ResolvedPredicate... children) {
        return new ResolvedPredicate.Or(List.of(children));
    }

    private static RowGroup intRowGroup(int min, int max, Long nullCount) {
        return rowGroup(PhysicalType.INT32,
                new Statistics(intBytes(min), intBytes(max), nullCount, null, false), 100);
    }

    private static RowGroup rowGroup(PhysicalType type, Statistics stats, long numRows) {
        ColumnMetaData cmd = new ColumnMetaData(
                type, List.of(Encoding.PLAIN), FieldPath.of("col"),
                CompressionCodec.UNCOMPRESSED, 100, 1000, 1000, Map.of(), 0, null, stats,
                null, null, null);
        ColumnChunk chunk = new ColumnChunk(cmd, null, null, null, null);
        return new RowGroup(List.of(chunk), 1000, numRows);
    }

    private static byte[] intBytes(int value) {
        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array();
    }
}
