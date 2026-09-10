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
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.InputFile;
import dev.hardwood.internal.writer.ByteBufferOutputFile;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.FilterPredicate.Operator;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;

import static org.assertj.core.api.Assertions.assertThat;

/// Regression tests for #566 and #1016: float/double predicate pruning must not drop
/// matching rows when a unit may hold `NaN` — whether a bound carries it (#566) or the
/// bounds cannot exclude it because the unit records no `nan_count` of zero (#1016).
///
/// A `NaN` bound is invalid under `TYPE_ORDER`; under `IEEE_754_TOTAL_ORDER` it may legally
/// describe an all-`NaN` unit. The pair is discarded either way (#898 tracks interpreting the
/// all-`NaN` case).
///
/// The check lives where [MinMaxStats] sources the bounds, alongside the other reasons a pair
/// of bounds can be unusable (#1172), so these cases go in through a leaf and its statistics
/// rather than straight at a comparator. The comparators below are still exercised directly
/// where the bounds they are given are usable ones.
class NaNStatisticsFilterTest {

    @ParameterizedTest(name = "double {0} with NaN min")
    @EnumSource(Operator.class)
    void doubleNaNMinNeverDrops(Operator op) {
        assertThat(dropsDouble(op, 1.0, Double.NaN, 10.0))
                .as("NaN min must be treated as no-bound and never prune")
                .isFalse();
    }

    @ParameterizedTest(name = "double {0} with NaN max")
    @EnumSource(Operator.class)
    void doubleNaNMaxNeverDrops(Operator op) {
        assertThat(dropsDouble(op, 1.0, -10.0, Double.NaN))
                .as("NaN max must be treated as no-bound and never prune")
                .isFalse();
    }

    @ParameterizedTest(name = "double {0} with NaN min and max")
    @EnumSource(Operator.class)
    void doubleNaNBothNeverDrops(Operator op) {
        assertThat(dropsDouble(op, 1.0, Double.NaN, Double.NaN)).isFalse();
    }

    @ParameterizedTest(name = "float {0} with NaN min")
    @EnumSource(Operator.class)
    void floatNaNMinNeverDrops(Operator op) {
        assertThat(dropsFloat(op, 1.0f, Float.NaN, 10.0f))
                .as("NaN min must be treated as no-bound and never prune")
                .isFalse();
    }

    @ParameterizedTest(name = "float {0} with NaN max")
    @EnumSource(Operator.class)
    void floatNaNMaxNeverDrops(Operator op) {
        assertThat(dropsFloat(op, 1.0f, -10.0f, Float.NaN))
                .as("NaN max must be treated as no-bound and never prune")
                .isFalse();
    }

    @ParameterizedTest(name = "float {0} with NaN min and max")
    @EnumSource(Operator.class)
    void floatNaNBothNeverDrops(Operator op) {
        assertThat(dropsFloat(op, 1.0f, Float.NaN, Float.NaN)).isFalse();
    }

    /// `FLOAT16` bounds are two bytes wide and decode through their own path, so the check
    /// has to reach them as well as the four- and eight-byte ones.
    @ParameterizedTest(name = "float16 {0} with a NaN bound")
    @EnumSource(Operator.class)
    void float16NaNNeverDrops(Operator op) {
        ResolvedPredicate leaf = new ResolvedPredicate.Float16Predicate(0, op, 1.0f, false);
        MinMaxStats stats = MinMaxStats.of(
                new Statistics(float16Bytes(Float.NaN), float16Bytes(10.0f), 0L, null, false), leaf, BoundsReadability.ALL);

        assertThat(stats.canDrop(leaf)).isFalse();
        assertThat(stats.discardReason())
                .isEqualTo("one of them is NaN, which sorts above every finite value");
    }

    /// A `FLOAT16` leaf decodes onto [MinMaxStats.FloatStats] and the float comparator;
    /// these go through [MinMaxStats#of] so the routing itself is what they prove (#1016).
    @ParameterizedTest(name = "float16 {0}(NaN) on usable bounds expectDrop={1}")
    @EnumSource(Operator.class)
    void float16NaNProbeRoutesThroughTheFloatTable(Operator op) {
        ResolvedPredicate leaf = new ResolvedPredicate.Float16Predicate(0, op, Float.NaN, false);
        MinMaxStats stats = MinMaxStats.of(
                new Statistics(float16Bytes(1.0f), float16Bytes(10.0f), 0L, null, false), leaf,
                BoundsReadability.ALL);

        // Exhaustive over the enum: a new operator has to state its own expectation here.
        boolean expectDrop = switch (op) {
            case GT -> true;
            case EQ, NOT_EQ, LT, LT_EQ, GT_EQ -> false;
        };

        assertThat(stats.canDrop(leaf)).isEqualTo(expectDrop);
    }

    @Test
    void float16NonNaNProbeRoutesThroughTheFloatTable() {
        ResolvedPredicate gtAboveMax =
                new ResolvedPredicate.Float16Predicate(0, Operator.GT, 20.0f, false);
        MinMaxStats gtStats = MinMaxStats.of(
                new Statistics(float16Bytes(1.0f), float16Bytes(10.0f), 0L, null, false),
                gtAboveMax, BoundsReadability.ALL);
        ResolvedPredicate eqOutside =
                new ResolvedPredicate.Float16Predicate(0, Operator.EQ, 100.0f, false);
        MinMaxStats eqStats = MinMaxStats.of(
                new Statistics(float16Bytes(1.0f), float16Bytes(10.0f), 0L, null, false),
                eqOutside, BoundsReadability.ALL);

        assertThat(gtStats.canDrop(gtAboveMax)).isFalse();
        assertThat(eqStats.canDrop(eqOutside)).isTrue();
    }

    /// Only a recorded `nan_count` of zero proves a unit holds no `NaN`, and only then do the
    /// bounds rule out a predicate a `NaN` row satisfies. An absent count means `NaN` may be
    /// present, as the spec requires a reader to assume.
    @ParameterizedTest(name = "nan_count={0} expectDrop={1}")
    @MethodSource
    void nanCountDecidesWhetherTheBoundsRuleOutANaNRow(Long nanCount, boolean expectDrop) {
        ResolvedPredicate doubleGt = new ResolvedPredicate.DoublePredicate(0, Operator.GT, 20.0, false);
        MinMaxStats doubleStats = MinMaxStats.of(
                new Statistics(doubleBytes(1.0), doubleBytes(10.0), 0L, null, false, true, true, nanCount),
                doubleGt, BoundsReadability.ALL);
        ResolvedPredicate float16Gt = new ResolvedPredicate.Float16Predicate(0, Operator.GT, 20.0f, false);
        MinMaxStats float16Stats = MinMaxStats.of(
                new Statistics(float16Bytes(1.0f), float16Bytes(10.0f), 0L, null, false, true, true, nanCount),
                float16Gt, BoundsReadability.ALL);

        assertThat(doubleStats.canDrop(doubleGt)).isEqualTo(expectDrop);
        assertThat(float16Stats.canDrop(float16Gt)).isEqualTo(expectDrop);
    }

    static Stream<Arguments> nanCountDecidesWhetherTheBoundsRuleOutANaNRow() {
        return Stream.of(
                Arguments.of(null, false),
                Arguments.of(1L, false),
                Arguments.of(0L, true));
    }

    /// A NaN bound is discarded rather than compared against, and says so.
    @Test
    void naNBoundsAreDiscardedAtTheSource() {
        ResolvedPredicate leaf = new ResolvedPredicate.DoublePredicate(0, Operator.GT, 1.0, false);
        MinMaxStats stats = MinMaxStats.of(
                new Statistics(doubleBytes(Double.NaN), doubleBytes(10.0), 0L, null, false),
                leaf, BoundsReadability.ALL);

        assertThat(stats)
                .isInstanceOf(MinMaxStats.NullCountOnlyStats.class)
                .extracting(MinMaxStats::discardReason)
                .isEqualTo("one of them is NaN, which sorts above every finite value");
    }

    @ParameterizedTest(name = "double {0}({1}) on [{2},{3}] ieee754TotalOrder={4} nanFree={5} expectDrop={6}")
    @MethodSource
    void doubleOperatorTable(Operator op, double value, double min, double max,
            boolean ieee754TotalOrder, boolean nanFree, boolean expectDrop) {
        assertThat(StatisticsFilterSupport.canDropDouble(op, value, min, max, ieee754TotalOrder, nanFree))
                .isEqualTo(expectDrop);
    }

    static Stream<Arguments> doubleOperatorTable() {
        // One row per operator per probe kind, each run under both column orders and carrying
        // two expectations: where NaN rows may be present, and where nan_count proves there are
        // none. Without that proof the bounds cannot rule out a NaN row, so every operator a NaN
        // row satisfies keeps the unit — NOT_EQ, GT, GT_EQ against a number, EQ, LT_EQ, GT_EQ
        // against NaN (#1016); GT(NaN) drops either way, since nothing sorts above NaN. With the
        // proof the total-order bounds rules apply unchanged. The rows are written out one per
        // operator rather than derived from the rule; float16NaNProbeRoutesThroughTheFloatTable
        // is where the enum itself drives the NaN table, so a new operator cannot slip past both.
        List<Arguments> rows = new ArrayList<>();
        //                  op               value       min  max   NaN possible  NaN-free
        addDoubleRows(rows, Operator.EQ,     Double.NaN, 1.0, 10.0, false,        true);
        addDoubleRows(rows, Operator.NOT_EQ, Double.NaN, 1.0, 10.0, false,        false);
        addDoubleRows(rows, Operator.LT,     Double.NaN, 1.0, 10.0, false,        false);
        addDoubleRows(rows, Operator.LT_EQ,  Double.NaN, 1.0, 10.0, false,        false);
        addDoubleRows(rows, Operator.GT,     Double.NaN, 1.0, 10.0, true,         true);
        addDoubleRows(rows, Operator.GT_EQ,  Double.NaN, 1.0, 10.0, false,        true);
        addDoubleRows(rows, Operator.EQ,     5.0,        1.0, 10.0, false,        false);
        addDoubleRows(rows, Operator.EQ,     100.0,      1.0, 10.0, true,         true);
        addDoubleRows(rows, Operator.NOT_EQ, 5.0,        1.0, 10.0, false,        false);
        addDoubleRows(rows, Operator.NOT_EQ, 1.0,        1.0, 1.0,  false,        true);
        addDoubleRows(rows, Operator.LT,     0.0,        1.0, 10.0, true,         true);
        addDoubleRows(rows, Operator.LT,     5.0,        1.0, 10.0, false,        false);
        addDoubleRows(rows, Operator.LT_EQ,  0.0,        1.0, 10.0, true,         true);
        addDoubleRows(rows, Operator.LT_EQ,  5.0,        1.0, 10.0, false,        false);
        addDoubleRows(rows, Operator.GT,     20.0,       1.0, 10.0, false,        true);
        addDoubleRows(rows, Operator.GT,     5.0,        1.0, 10.0, false,        false);
        addDoubleRows(rows, Operator.GT_EQ,  20.0,       1.0, 10.0, false,        true);
        addDoubleRows(rows, Operator.GT_EQ,  5.0,        1.0, 10.0, false,        false);
        return rows.stream();
    }

    @ParameterizedTest(name = "float {0}({1}) on [{2},{3}] ieee754TotalOrder={4} nanFree={5} expectDrop={6}")
    @MethodSource
    void floatOperatorTable(Operator op, float value, float min, float max,
            boolean ieee754TotalOrder, boolean nanFree, boolean expectDrop) {
        assertThat(StatisticsFilterSupport.canDropFloat(op, value, min, max, ieee754TotalOrder, nanFree))
                .isEqualTo(expectDrop);
    }

    static Stream<Arguments> floatOperatorTable() {
        // See [#doubleOperatorTable]: the float table mirrors the double one.
        List<Arguments> rows = new ArrayList<>();
        //                 op               value      min   max    NaN possible  NaN-free
        addFloatRows(rows, Operator.EQ,     Float.NaN, 1.0f, 10.0f, false,        true);
        addFloatRows(rows, Operator.NOT_EQ, Float.NaN, 1.0f, 10.0f, false,        false);
        addFloatRows(rows, Operator.LT,     Float.NaN, 1.0f, 10.0f, false,        false);
        addFloatRows(rows, Operator.LT_EQ,  Float.NaN, 1.0f, 10.0f, false,        false);
        addFloatRows(rows, Operator.GT,     Float.NaN, 1.0f, 10.0f, true,         true);
        addFloatRows(rows, Operator.GT_EQ,  Float.NaN, 1.0f, 10.0f, false,        true);
        addFloatRows(rows, Operator.EQ,     5.0f,      1.0f, 10.0f, false,        false);
        addFloatRows(rows, Operator.EQ,     100.0f,    1.0f, 10.0f, true,         true);
        addFloatRows(rows, Operator.NOT_EQ, 5.0f,      1.0f, 10.0f, false,        false);
        addFloatRows(rows, Operator.NOT_EQ, 1.0f,      1.0f, 1.0f,  false,        true);
        addFloatRows(rows, Operator.LT,     0.0f,      1.0f, 10.0f, true,         true);
        addFloatRows(rows, Operator.LT,     5.0f,      1.0f, 10.0f, false,        false);
        addFloatRows(rows, Operator.LT_EQ,  0.0f,      1.0f, 10.0f, true,         true);
        addFloatRows(rows, Operator.LT_EQ,  5.0f,      1.0f, 10.0f, false,        false);
        addFloatRows(rows, Operator.GT,     20.0f,     1.0f, 10.0f, false,        true);
        addFloatRows(rows, Operator.GT,     5.0f,      1.0f, 10.0f, false,        false);
        addFloatRows(rows, Operator.GT_EQ,  20.0f,     1.0f, 10.0f, false,        true);
        addFloatRows(rows, Operator.GT_EQ,  5.0f,      1.0f, 10.0f, false,        false);
        return rows.stream();
    }

    /// Order-aware ±0 pruning (#595). Under the type-defined ordering (`ieee754TotalOrder == false`)
    /// the spec leaves `+0`/`-0` ambiguous, so a zero bound is widened and the opposite zero is not
    /// dropped. Under the IEEE 754 total order the bounds are exact, so `-0 < +0` prunes precisely.
    @Test
    void typeDefinedZeroBoundDoesNotDropOppositeZero() {
        // min = +0, max = +0 may also hold -0 → == -0 must not drop.
        assertThat(StatisticsFilterSupport.canDropFloat(Operator.EQ, -0.0f, 0.0f, 0.0f, false, false)).isFalse();
        assertThat(StatisticsFilterSupport.canDropDouble(Operator.EQ, -0.0, 0.0, 0.0, false, false)).isFalse();
        // max = -0, min = -0 may also hold +0 → == +0 must not drop.
        assertThat(StatisticsFilterSupport.canDropFloat(Operator.EQ, 0.0f, -0.0f, -0.0f, false, false)).isFalse();
        assertThat(StatisticsFilterSupport.canDropDouble(Operator.EQ, 0.0, -0.0, -0.0, false, false)).isFalse();
    }

    @Test
    void ieee754ZeroBoundPrunesExactly() {
        // Total order is unambiguous: min = +0 genuinely excludes -0 → == -0 prunes.
        assertThat(StatisticsFilterSupport.canDropFloat(Operator.EQ, -0.0f, 0.0f, 0.0f, true, false)).isTrue();
        assertThat(StatisticsFilterSupport.canDropDouble(Operator.EQ, -0.0, 0.0, 0.0, true, false)).isTrue();
        // max = -0 genuinely excludes +0 → == +0 prunes.
        assertThat(StatisticsFilterSupport.canDropFloat(Operator.EQ, 0.0f, -0.0f, -0.0f, true, false)).isTrue();
        assertThat(StatisticsFilterSupport.canDropDouble(Operator.EQ, 0.0, -0.0, -0.0, true, false)).isTrue();
    }

    @Test
    void zeroBoundsStillPruneNonZeroValuesUnderBothOrders() {
        assertThat(StatisticsFilterSupport.canDropFloat(Operator.EQ, 5.0f, -0.0f, 0.0f, false, false)).isTrue();
        assertThat(StatisticsFilterSupport.canDropFloat(Operator.EQ, 5.0f, -0.0f, 0.0f, true, false)).isTrue();
    }

    /// Writer-to-reader regression (#1016): the writer keeps `NaN` out of the bounds and records
    /// a non-zero `nan_count` for these columns, so every predicate a `NaN` row satisfies must
    /// return it — asserted on the returned values, not just a row count.
    @Test
    void writerToReaderRoundTripReturnsNaNRowsThatPredicatesMatch() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("mixed", PhysicalType.DOUBLE, RepetitionType.REQUIRED)
                .addColumn("collapsed", PhysicalType.DOUBLE, RepetitionType.REQUIRED)
                .build();
        double[] mixed = {1.0, 2.0, Double.NaN, 3.0};
        double[] collapsed = {1.0, 1.0, Double.NaN, 1.0};

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(batch -> batch.doubles(0, mixed).doubles(1, collapsed));
        }

        assertThat(readMatching(out, "mixed", FilterPredicate.eq("mixed", Double.NaN)))
                .containsExactly(Double.NaN);
        assertThat(readMatching(out, "mixed", FilterPredicate.gtEq("mixed", Double.NaN)))
                .containsExactly(Double.NaN);
        assertThat(readMatching(out, "mixed", FilterPredicate.gt("mixed", 10.0)))
                .containsExactly(Double.NaN);
        assertThat(readMatching(out, "mixed", FilterPredicate.gtEq("mixed", 10.0)))
                .containsExactly(Double.NaN);
        assertThat(readMatching(out, "collapsed", FilterPredicate.notEq("collapsed", 1.0)))
                .containsExactly(Double.NaN);
    }

    /// The writer records a `nan_count` of zero for a column holding no `NaN`, and that lets
    /// the bounds prune the predicates a `NaN` row would otherwise keep (#1016).
    @Test
    void writerNaNCountOfZeroLetsTheBoundsPrune() throws Exception {
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("finite", PhysicalType.DOUBLE, RepetitionType.REQUIRED)
                .build();
        double[] finite = {1.0, 2.0, 3.0};

        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(batch -> batch.doubles(0, finite));
        }

        Statistics statistics;
        try (ParquetFileReader reader =
                     ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())))) {
            statistics = reader.getFileMetaData().rowGroups().get(0).columns().get(0).metaData().statistics();
        }

        assertThat(statistics.nanCount()).isEqualTo(0L);
        for (ResolvedPredicate leaf : List.of(
                new ResolvedPredicate.DoublePredicate(0, Operator.GT, 10.0, false),
                new ResolvedPredicate.DoublePredicate(0, Operator.GT_EQ, 10.0, false),
                new ResolvedPredicate.DoublePredicate(0, Operator.EQ, Double.NaN, false))) {
            assertThat(MinMaxStats.of(statistics, leaf, BoundsReadability.ALL).canDrop(leaf))
                    .as("%s", leaf)
                    .isTrue();
        }
    }

    private static List<Double> readMatching(ByteBufferOutputFile out, String column,
            FilterPredicate filter) throws Exception {
        try (ParquetFileReader reader =
                     ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(out.toByteArray())));
             ColumnReader values = reader.buildColumnReader(column).filter(filter).build()) {
            List<Double> matched = new ArrayList<>();
            while (values.nextBatch()) {
                double[] batch = values.getDoubles();
                for (int i = 0; i < values.getRecordCount(); i++) {
                    matched.add(batch[i]);
                }
            }
            return matched;
        }
    }

    // ==================== Fixtures ====================

    /// Adds one operator table row under both column orders, once where `NaN` may be present
    /// and once where the unit is proven `NaN`-free.
    private static void addDoubleRows(List<Arguments> rows, Operator op, double value, double min,
            double max, boolean dropsWhereNaNMayBePresent, boolean dropsWhereNaNFree) {
        for (boolean ieee754TotalOrder : new boolean[]{false, true}) {
            rows.add(Arguments.of(op, value, min, max, ieee754TotalOrder, false, dropsWhereNaNMayBePresent));
            rows.add(Arguments.of(op, value, min, max, ieee754TotalOrder, true, dropsWhereNaNFree));
        }
    }

    /// See [#addDoubleRows].
    private static void addFloatRows(List<Arguments> rows, Operator op, float value, float min,
            float max, boolean dropsWhereNaNMayBePresent, boolean dropsWhereNaNFree) {
        for (boolean ieee754TotalOrder : new boolean[]{false, true}) {
            rows.add(Arguments.of(op, value, min, max, ieee754TotalOrder, false, dropsWhereNaNMayBePresent));
            rows.add(Arguments.of(op, value, min, max, ieee754TotalOrder, true, dropsWhereNaNFree));
        }
    }

    /// Whether the leaf drops a unit with these bounds, sourced the way the evaluators source
    /// them so that the usability check runs.
    private static boolean dropsDouble(Operator op, double value, double min, double max) {
        ResolvedPredicate leaf = new ResolvedPredicate.DoublePredicate(0, op, value, false);
        return MinMaxStats.of(new Statistics(doubleBytes(min), doubleBytes(max), 0L, null, false), leaf, BoundsReadability.ALL)
                .canDrop(leaf);
    }

    /// See [#dropsDouble].
    private static boolean dropsFloat(Operator op, float value, float min, float max) {
        ResolvedPredicate leaf = new ResolvedPredicate.FloatPredicate(0, op, value, false);
        return MinMaxStats.of(new Statistics(floatBytes(min), floatBytes(max), 0L, null, false), leaf, BoundsReadability.ALL)
                .canDrop(leaf);
    }

    private static byte[] float16Bytes(float value) {
        short raw = Float.floatToFloat16(value);
        return new byte[]{ (byte) (raw & 0xFF), (byte) ((raw >> 8) & 0xFF) };
    }

    private static byte[] floatBytes(float value) {
        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putFloat(value).array();
    }

    private static byte[] doubleBytes(double value) {
        return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putDouble(value).array();
    }
}
