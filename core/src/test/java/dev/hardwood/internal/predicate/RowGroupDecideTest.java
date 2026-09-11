/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.metadata.SizeStatistics;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.reader.FilterPredicate;

import static dev.hardwood.internal.predicate.FilterDecision.ALWAYS_MATCHES;
import static dev.hardwood.internal.predicate.FilterDecision.CANNOT_MATCH;
import static dev.hardwood.internal.predicate.FilterDecision.MIGHT_MATCH;
import static org.assertj.core.api.Assertions.assertThat;

/// [RowGroupFilterEvaluator#decideRowGroup] behavior over whole row groups: leaf decisions
/// with row-group [Statistics], `AND`/`OR` composition, null predicates, and one cutoff per
/// decision against a single row group.
class RowGroupDecideTest {

    /// A position with nothing to point at: these cases assert decisions, not diagnostics.
    private static final LogContext UNNAMED =
            new LogContext(null, ExceptionContext.UNKNOWN_ROW_GROUP);

    private static final int COL = 0;

    @RegisterExtension
    final CapturedWarnings warnings = new CapturedWarnings();

    @Test
    void rangePredicateOverFullySatisfyingRowGroup() throws IOException {
        // Rows [10, 20], no nulls; GT 5 is satisfied by the whole interval.
        RowGroup rg = intRowGroup(10, 20, 0L);
        assertThat(decide(intGt(5), rg)).isEqualTo(ALWAYS_MATCHES);
        assertThat(decide(intGt(15), rg)).isEqualTo(MIGHT_MATCH);
        assertThat(decide(intGt(20), rg)).isEqualTo(CANNOT_MATCH);
    }

    @Test
    void nullsInRowGroupPreventAlwaysMatches() throws IOException {
        RowGroup rg = intRowGroup(10, 20, 5L);
        assertThat(decide(intGt(5), rg)).isEqualTo(MIGHT_MATCH);
    }

    @Test
    void missingStatisticsYieldMightMatch() throws IOException {
        RowGroup rg = rowGroup(PhysicalType.INT32, null, 100);
        assertThat(decide(intGt(5), rg)).isEqualTo(MIGHT_MATCH);
    }

    @Test
    void andComposition() throws IOException {
        RowGroup rg = intRowGroup(10, 20, 0L);
        assertThat(decide(and(intGt(5), intLt(25)), rg)).isEqualTo(ALWAYS_MATCHES);
        assertThat(decide(and(intGt(5), intLt(15)), rg)).isEqualTo(MIGHT_MATCH);
        assertThat(decide(and(intGt(5), intGt(25)), rg)).isEqualTo(CANNOT_MATCH);
    }

    @Test
    void orComposition() throws IOException {
        RowGroup rg = intRowGroup(10, 20, 0L);
        // One always-matching branch decides the disjunction.
        assertThat(decide(or(intGt(25), intGt(5)), rg)).isEqualTo(ALWAYS_MATCHES);
        assertThat(decide(or(intGt(25), intGt(15)), rg)).isEqualTo(MIGHT_MATCH);
        assertThat(decide(or(intGt(25), intLt(5)), rg)).isEqualTo(CANNOT_MATCH);
    }

    @Test
    void isNotNullDecisions() throws IOException {
        assertThat(decide(isNotNull(), intRowGroup(10, 20, 0L))).isEqualTo(ALWAYS_MATCHES);
        assertThat(decide(isNotNull(), intRowGroup(10, 20, 5L))).isEqualTo(MIGHT_MATCH);
        // All 100 rows null
        assertThat(decide(isNotNull(), rowGroup(PhysicalType.INT32,
                new Statistics(null, null, 100L, null, false), 100))).isEqualTo(CANNOT_MATCH);
    }

    @Test
    void isNullDecisions() throws IOException {
        // All 100 rows null. A predicate names a non-repeated leaf, so its null count counts rows.
        assertThat(decide(isNull(), rowGroup(PhysicalType.INT32,
                new Statistics(null, null, 100L, null, false), 100))).isEqualTo(ALWAYS_MATCHES);
        assertThat(decide(isNull(), intRowGroup(10, 20, 5L))).isEqualTo(MIGHT_MATCH);
        assertThat(decide(isNull(), intRowGroup(10, 20, 0L))).isEqualTo(CANNOT_MATCH);
    }

    @Test
    void valuePredicateCannotMatchARowGroupNullOnEveryRow() throws IOException {
        // No bounds, as a writer records for a chunk holding no value, and a null count equal to
        // the row count. A null satisfies no value predicate, NOT_EQ included.
        RowGroup rg = rowGroup(PhysicalType.INT32, new Statistics(null, null, 100L, null, false), 100);
        assertThat(decide(intGt(5), rg)).isEqualTo(CANNOT_MATCH);
        assertThat(decide(new ResolvedPredicate.IntPredicate(COL, FilterPredicate.Operator.NOT_EQ, 5), rg))
                .isEqualTo(CANNOT_MATCH);
    }

    @Test
    void bloomFilterSourceDoesNotAffectAlwaysMatches() throws IOException {
        // A bloom filter proves absence only; its presence must not change the
        // always-matching decision derived from statistics.
        RowGroup rg = intRowGroup(10, 20, 0L);
        BloomFilterSource noFilters = columnIndex -> null;
        assertThat(RowGroupFilterEvaluator.decideRowGroup(intGt(5), rg, noFilters, null, UNNAMED, BoundsReadability.ALL))
                .isEqualTo(ALWAYS_MATCHES);
    }

    @Test
    void oneCutoffPerDecisionAgainstTheSameRowGroup() throws IOException {
        // Above the maximum, inside the range, below the minimum: the three answers a row
        // group's statistics can give. Asserted as decisions rather than through a boolean,
        // which could not tell the last two apart.
        RowGroup rg = intRowGroup(10, 20, 0L);
        assertThat(decide(intGt(20), rg)).isEqualTo(CANNOT_MATCH);
        assertThat(decide(intGt(15), rg)).isEqualTo(MIGHT_MATCH);
        assertThat(decide(intGt(5), rg)).isEqualTo(ALWAYS_MATCHES);
    }

    @Test
    void deprecatedMinMaxNeverPromisesAlwaysMatches() throws IOException {
        Statistics deprecated = new Statistics(intBytes(10), intBytes(20), 0L, null, true);
        RowGroup rg = rowGroup(PhysicalType.INT32, deprecated, 100);
        assertThat(decide(intGt(5), rg)).isEqualTo(MIGHT_MATCH);
    }

    @Test
    void discardedBoundsAreNamedByTheColumnTheRowGroupCarries() throws IOException {
        // The column path comes from the row group's own metadata rather than from the leaf,
        // which knows the column only by its index. A row group is the finest position the
        // decision has, so no page is named.
        RowGroup rg = rowGroup(PhysicalType.INT32,
                new Statistics(intBytes(20), intBytes(10), 0L, null, false), 100);

        FilterDecision decision = RowGroupFilterEvaluator.decideRowGroup(intGt(5), rg, null, null,
                new LogContext("orders.parquet", 4), BoundsReadability.ALL);

        assertThat(decision).isEqualTo(MIGHT_MATCH);
        assertThat(warnings.messages()).containsExactly(
                "[orders.parquet: row group 4, column 'order.price'] Ignoring the min/max "
                        + "statistics for pruning: the minimum sorts above the maximum. Rows they "
                        + "could have skipped are read and filtered instead.");
    }

    @Test
    void groupIsNullUsesDefinitionLevelHistogram() throws IOException {
        SizeStatistics sizeStatistics = new SizeStatistics(
                null,
                null,
                new long[]{ 0, 0, 100, 0 });

        RowGroup rg = rowGroup(
                PhysicalType.INT32,
                new Statistics(null, null, 100L, null, false),
                sizeStatistics,
                100);

        ResolvedPredicate predicate =
                new ResolvedPredicate.IsNullPredicate(COL, 2, 3);

        assertThat(decide(predicate, rg))
                .isEqualTo(CANNOT_MATCH);
    }

    @Test
    void groupIsNotNullDoesNotUseLeafNullCountToDrop() throws IOException {
        SizeStatistics sizeStatistics = new SizeStatistics(
                null,
                null,
                new long[]{ 0, 0, 100, 0 });

        RowGroup rg = rowGroup(
                PhysicalType.INT32,
                new Statistics(null, null, 100L, null, false),
                sizeStatistics,
                100);

        ResolvedPredicate predicate =
                new ResolvedPredicate.IsNotNullPredicate(COL, 2, 3);

        assertThat(decide(predicate, rg))
                .isNotEqualTo(CANNOT_MATCH);
    }

    @Test
    void groupIsNotNullDropsWhenHistogramShowsGroupAlwaysAbsent() throws IOException {
        SizeStatistics sizeStatistics = new SizeStatistics(
                null,
                null,
                new long[]{ 0, 100, 0, 0 });

        RowGroup rg = rowGroup(
                PhysicalType.INT32,
                new Statistics(null, null, 100L, null, false),
                sizeStatistics,
                100);

        ResolvedPredicate predicate =
                new ResolvedPredicate.IsNotNullPredicate(COL, 2, 3);

        assertThat(decide(predicate, rg))
                .isEqualTo(CANNOT_MATCH);
    }
    @Test
    void groupNullPredicateWithoutHistogramFallsBackToMightMatch() throws IOException {
        RowGroup rg = rowGroup(
                PhysicalType.INT32,
                new Statistics(null, null, 100L, null, false),
                null,
                100);

        ResolvedPredicate isNull =
                new ResolvedPredicate.IsNullPredicate(COL, 2, 3);

        ResolvedPredicate isNotNull =
                new ResolvedPredicate.IsNotNullPredicate(COL, 2, 3);


        assertThat(decide(isNull, rg))
                .isEqualTo(MIGHT_MATCH);

        assertThat(decide(isNotNull, rg))
                .isEqualTo(MIGHT_MATCH);
    }

    @Test
    void listNullPredicateWithoutHistogramStaysUndecided() throws IOException {
        // An optional LIST of optional elements: the list is present at level 1, an element
        // non-null at level 3. Each of the 100 rows holds one null and one non-null element, so
        // the null count equals the row count although no list is absent. Without a histogram
        // the null count is all there is, and it answers a different question than either null
        // predicate on the list asks.
        RowGroup rg = rowGroup(PhysicalType.INT32,
                new Statistics(intBytes(1), intBytes(9), 100L, null, false), 100);

        assertThat(decide(new ResolvedPredicate.IsNullPredicate(COL, 1, 3), rg)).isEqualTo(MIGHT_MATCH);
        assertThat(decide(new ResolvedPredicate.IsNotNullPredicate(COL, 1, 3), rg)).isEqualTo(MIGHT_MATCH);
    }

    @Test
    void groupIsNotNullAlwaysMatchesWhenHistogramShowsGroupAlwaysPresent() throws IOException {
        // Every entry at level 2 or 3: the group is present throughout, so every row matches and
        // the read can skip per-row evaluation for the whole row group.
        SizeStatistics sizeStatistics = new SizeStatistics(
                null,
                null,
                new long[]{ 0, 0, 40, 60 });

        RowGroup rg = rowGroup(
                PhysicalType.INT32,
                new Statistics(null, null, 40L, null, false),
                sizeStatistics,
                100);

        assertThat(decide(new ResolvedPredicate.IsNotNullPredicate(COL, 2, 3), rg))
                .isEqualTo(ALWAYS_MATCHES);
    }

    @Test
    void groupIsNullAlwaysMatchesWhenHistogramShowsGroupAlwaysAbsent() throws IOException {
        // The dual: nothing reaches level 2, so the group is absent on every row.
        SizeStatistics sizeStatistics = new SizeStatistics(
                null,
                null,
                new long[]{ 30, 70, 0, 0 });

        RowGroup rg = rowGroup(
                PhysicalType.INT32,
                new Statistics(null, null, 100L, null, false),
                sizeStatistics,
                100);

        assertThat(decide(new ResolvedPredicate.IsNullPredicate(COL, 2, 3), rg))
                .isEqualTo(ALWAYS_MATCHES);
    }

    @Test
    void groupNullPredicateWillNotProveEveryRowFromAHistogramThatMissesRows() throws IOException {
        // Every entry has the group present, but the histogram accounts for 90 of the 100 rows,
        // so "every entry" is not "every row" and the remainder is not assumed to match.
        SizeStatistics sizeStatistics = new SizeStatistics(
                null,
                null,
                new long[]{ 0, 0, 40, 50 });

        RowGroup rg = rowGroup(
                PhysicalType.INT32,
                new Statistics(null, null, 40L, null, false),
                sizeStatistics,
                100);

        assertThat(decide(new ResolvedPredicate.IsNotNullPredicate(COL, 2, 3), rg))
                .isEqualTo(MIGHT_MATCH);
    }

    @Test
    void groupNullPredicateIgnoresWrongHistogramLength() throws IOException {
        SizeStatistics sizeStatistics = new SizeStatistics(
                null,
                null,
                new long[]{ 0, 100, 0 });

        RowGroup rg = rowGroup(
                PhysicalType.INT32,
                new Statistics(null, null, 100L, null, false),
                sizeStatistics,
                100);

        ResolvedPredicate predicate =
                new ResolvedPredicate.IsNotNullPredicate(COL, 2, 3);

        assertThat(decide(predicate, rg))
                .isEqualTo(MIGHT_MATCH);
    }
    // ==================== Fixtures ====================

    private static FilterDecision decide(ResolvedPredicate predicate, RowGroup rowGroup)
            throws IOException {
        return RowGroupFilterEvaluator.decideRowGroup(predicate, rowGroup, null, null, UNNAMED, BoundsReadability.ALL);
    }

    private static ResolvedPredicate intGt(int value) {
        return new ResolvedPredicate.IntPredicate(COL, FilterPredicate.Operator.GT, value);
    }

    private static ResolvedPredicate intLt(int value) {
        return new ResolvedPredicate.IntPredicate(COL, FilterPredicate.Operator.LT, value);
    }

    private static ResolvedPredicate isNull() {
        return new ResolvedPredicate.IsNullPredicate(COL, 1, 1);
    }

    private static ResolvedPredicate isNotNull() {
        return new ResolvedPredicate.IsNotNullPredicate(COL, 1, 1);
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
        return rowGroup(type, stats, null, numRows);
    }

    private static RowGroup rowGroup(PhysicalType type, Statistics stats,
            SizeStatistics sizeStatistics, long numRows) {
        ColumnMetaData cmd = new ColumnMetaData(
                type, List.of(Encoding.PLAIN), FieldPath.of("order", "price"),
                CompressionCodec.UNCOMPRESSED, 100, 1000, 1000, Map.of(), 0, null, stats,
                null, null, null, List.of(), sizeStatistics);
        ColumnChunk chunk = new ColumnChunk(cmd, null, null, null, null, "");
        return new RowGroup(List.of(chunk), 1000, numRows);
    }

    private static byte[] intBytes(int value) {
        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array();
    }
}
