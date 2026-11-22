/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.predicate.ResolvedPredicate.And;
import dev.hardwood.internal.predicate.ResolvedPredicate.BinaryInPredicate;
import dev.hardwood.internal.predicate.ResolvedPredicate.BinaryPredicate;
import dev.hardwood.internal.predicate.ResolvedPredicate.BooleanPredicate;
import dev.hardwood.internal.predicate.ResolvedPredicate.DoubleInPredicate;
import dev.hardwood.internal.predicate.ResolvedPredicate.DoublePredicate;
import dev.hardwood.internal.predicate.ResolvedPredicate.EveryNonNullRowPredicate;
import dev.hardwood.internal.predicate.ResolvedPredicate.Float16InPredicate;
import dev.hardwood.internal.predicate.ResolvedPredicate.Float16Predicate;
import dev.hardwood.internal.predicate.ResolvedPredicate.FloatInPredicate;
import dev.hardwood.internal.predicate.ResolvedPredicate.FloatPredicate;
import dev.hardwood.internal.predicate.ResolvedPredicate.GeospatialPredicate;
import dev.hardwood.internal.predicate.ResolvedPredicate.IntInPredicate;
import dev.hardwood.internal.predicate.ResolvedPredicate.IntPredicate;
import dev.hardwood.internal.predicate.ResolvedPredicate.IsNotNullPredicate;
import dev.hardwood.internal.predicate.ResolvedPredicate.IsNullPredicate;
import dev.hardwood.internal.predicate.ResolvedPredicate.LongInPredicate;
import dev.hardwood.internal.predicate.ResolvedPredicate.LongPredicate;
import dev.hardwood.internal.predicate.ResolvedPredicate.NoRowPredicate;
import dev.hardwood.internal.predicate.ResolvedPredicate.Or;
import dev.hardwood.internal.predicate.ResolvedPredicate.UnsignedIntInPredicate;
import dev.hardwood.internal.predicate.ResolvedPredicate.UnsignedIntPredicate;
import dev.hardwood.internal.predicate.ResolvedPredicate.UnsignedLongInPredicate;
import dev.hardwood.internal.predicate.ResolvedPredicate.UnsignedLongPredicate;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.RowGroupPredicate;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// Asserts the predicate rendering the Filter JFR events carry (#1053): operators
/// and column names survive, every literal is elided, and membership renders its
/// arity rather than its values — including the binary and `*In` shapes, where a
/// naive `toString` would also be wrong.
class PredicateRendererTest {

    /// 0 id INT64, 1 label BYTE_ARRAY, 2 city BYTE_ARRAY, 3 zip INT32,
    /// 4 score DOUBLE, 5 flag BOOLEAN, 6 ratio FLOAT,
    /// 7 half FLOAT16, 8 payload BYTE_ARRAY.
    private static final FileSchema SCHEMA = FileSchema.builder("schema")
            .primitive("id", PhysicalType.INT64, RepetitionType.REQUIRED)
            .primitive("label", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL,
                    col -> col.logicalType(LogicalType.string()))
            .primitive("city", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL,
                    col -> col.logicalType(LogicalType.string()))
            .primitive("zip", PhysicalType.INT32, RepetitionType.REQUIRED)
            .primitive("score", PhysicalType.DOUBLE, RepetitionType.REQUIRED)
            .primitive("flag", PhysicalType.BOOLEAN, RepetitionType.REQUIRED)
            .primitive("ratio", PhysicalType.FLOAT, RepetitionType.REQUIRED)
            .primitive("half", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED,
                    col -> col.typeLength(2).logicalType(LogicalType.float16()))
            .primitive("payload", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL)
            .build();

    @Test
    void comparisonLeavesRenderOperatorColumnAndPlaceholder() {
        assertThat(PredicateRenderer.render(new LongPredicate(0, FilterPredicate.Operator.GT, 150L), SCHEMA))
                .isEqualTo("gt(id, ?)");
        assertThat(PredicateRenderer.render(
                new BinaryPredicate(1, FilterPredicate.Operator.EQ,
                        "someone@example.com".getBytes(StandardCharsets.UTF_8),
                        BinaryPredicate.Comparison.BYTE_STRING), SCHEMA))
                .isEqualTo("eq(label, ?)");
        assertThat(PredicateRenderer.render(new IntPredicate(3, FilterPredicate.Operator.LT, 90210), SCHEMA))
                .isEqualTo("lt(zip, ?)");
        assertThat(PredicateRenderer.render(new IntPredicate(3, FilterPredicate.Operator.LT_EQ, 90210), SCHEMA))
                .isEqualTo("ltEq(zip, ?)");
        assertThat(PredicateRenderer.render(new DoublePredicate(4, FilterPredicate.Operator.GT_EQ, 9.5), SCHEMA))
                .isEqualTo("gtEq(score, ?)");
        assertThat(PredicateRenderer.render(new DoublePredicate(4, FilterPredicate.Operator.NOT_EQ, 9.5), SCHEMA))
                .isEqualTo("notEq(score, ?)");
    }

    @Test
    void noLiteralReachesTheOutput() {
        String rendered = PredicateRenderer.render(
                new BinaryPredicate(1, FilterPredicate.Operator.EQ,
                        "someone@example.com".getBytes(StandardCharsets.UTF_8),
                        BinaryPredicate.Comparison.BYTE_STRING), SCHEMA);
        assertThat(rendered)
                .doesNotContain("someone@example.com")
                .doesNotContain("[B@");
        assertThat(PredicateRenderer.render(new LongPredicate(0, FilterPredicate.Operator.GT, 150L), SCHEMA))
                .doesNotContain("150");
        assertThat(PredicateRenderer.render(new DoublePredicate(4, FilterPredicate.Operator.EQ, 9.5), SCHEMA))
                .doesNotContain("9.5");
        assertThat(PredicateRenderer.render(new BooleanPredicate(5, FilterPredicate.Operator.EQ, true), SCHEMA))
                .doesNotContain("true");
    }

    @Test
    void inPredicatesRenderArityNotValues() {
        assertThat(PredicateRenderer.render(new IntInPredicate(3, new int[] {1, 2, 3, 4, 5}), SCHEMA))
                .isEqualTo("in(zip, ?×5)");
        assertThat(PredicateRenderer.render(new LongInPredicate(0, new long[] {150L, 151L}), SCHEMA))
                .isEqualTo("in(id, ?×2)");
        assertThat(PredicateRenderer.render(new BinaryInPredicate(1,
                new byte[][] {"a".getBytes(StandardCharsets.UTF_8), "b".getBytes(StandardCharsets.UTF_8)},
                BinaryPredicate.Comparison.BYTE_STRING), SCHEMA))
                .isEqualTo("in(label, ?×2)");
        assertThat(PredicateRenderer.render(new DoubleInPredicate(4, new double[] {1.5, 2.5, 3.5}), SCHEMA))
                .isEqualTo("in(score, ?×3)");
        assertThat(PredicateRenderer.render(new FloatInPredicate(6, new float[] {0.5f}), SCHEMA))
                .isEqualTo("in(ratio, ?×1)");
        assertThat(PredicateRenderer.render(new Float16InPredicate(7, new float[] {1.0f, 2.0f}), SCHEMA))
                .isEqualTo("in(half, ?×2)");
        assertThat(PredicateRenderer.render(new UnsignedIntInPredicate(3, new int[] {7}), SCHEMA))
                .isEqualTo("in(zip, ?×1)");
        assertThat(PredicateRenderer.render(new UnsignedLongInPredicate(0, new long[] {7L}), SCHEMA))
                .isEqualTo("in(id, ?×1)");
    }

    @Test
    void nullAndConstantLeavesRenderInFull() {
        assertThat(PredicateRenderer.render(new IsNullPredicate(2, 1), SCHEMA)).isEqualTo("isNull(city)");
        assertThat(PredicateRenderer.render(new IsNotNullPredicate(2, 1), SCHEMA)).isEqualTo("isNotNull(city)");
        assertThat(PredicateRenderer.render(new EveryNonNullRowPredicate(0), SCHEMA))
                .isEqualTo("everyNonNullRow(id)");
        assertThat(PredicateRenderer.render(new NoRowPredicate(0), SCHEMA)).isEqualTo("noRow(id)");
    }

    @Test
    void unsignedAndFloat16LeavesRenderLikeTheirSignedSiblings() {
        assertThat(PredicateRenderer.render(
                new UnsignedIntPredicate(3, FilterPredicate.Operator.GT, -1), SCHEMA)).isEqualTo("gt(zip, ?)");
        assertThat(PredicateRenderer.render(
                new UnsignedLongPredicate(0, FilterPredicate.Operator.LT_EQ, -1L), SCHEMA))
                .isEqualTo("ltEq(id, ?)");
        assertThat(PredicateRenderer.render(new Float16Predicate(7, FilterPredicate.Operator.LT, 1.5f), SCHEMA))
                .isEqualTo("lt(half, ?)");
        assertThat(PredicateRenderer.render(new FloatPredicate(6, FilterPredicate.Operator.GT, 0.5f), SCHEMA))
                .isEqualTo("gt(ratio, ?)");
        assertThat(PredicateRenderer.render(new BooleanPredicate(5, FilterPredicate.Operator.NOT_EQ, false), SCHEMA))
                .isEqualTo("notEq(flag, ?)");
    }

    @Test
    void geospatialRendersItsBoundsInFull() {
        assertThat(PredicateRenderer.render(new GeospatialPredicate(0, -10.5, 20.0, 30.25, 40.75), SCHEMA))
                .isEqualTo("intersects(id, -10.5, 20.0, 30.25, 40.75)");
    }

    @Test
    void compoundsNestWithElidedLiterals() {
        assertThat(PredicateRenderer.render(new And(java.util.List.of(
                new LongPredicate(0, FilterPredicate.Operator.GT, 150L),
                new BinaryPredicate(1, FilterPredicate.Operator.EQ,
                        "someone@example.com".getBytes(StandardCharsets.UTF_8),
                        BinaryPredicate.Comparison.BYTE_STRING))), SCHEMA))
                .isEqualTo("and(gt(id, ?), eq(label, ?))");
        assertThat(PredicateRenderer.render(new Or(java.util.List.of(
                new IsNullPredicate(2, 1),
                new IntPredicate(3, FilterPredicate.Operator.LT, 90210))), SCHEMA))
                .isEqualTo("or(isNull(city), lt(zip, ?))");
    }

    @Test
    void rowGroupPredicateRendersByteRangesInFull() {
        assertThat(PredicateRenderer.render(RowGroupPredicate.byteRange(0, 134217728)))
                .isEqualTo("byteRange(0, 134217728)");
        assertThat(PredicateRenderer.render(RowGroupPredicate.and(
                RowGroupPredicate.byteRange(0, 100), RowGroupPredicate.byteRange(100, 200))))
                .isEqualTo("and(byteRange(0, 100), byteRange(100, 200))");
    }
}
