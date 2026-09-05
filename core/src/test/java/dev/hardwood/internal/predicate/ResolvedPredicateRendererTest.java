/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

class ResolvedPredicateRendererTest {

    private static final FileSchema SCHEMA = FileSchema.builder("schema")
            .addColumn("int_col", PhysicalType.INT32, RepetitionType.REQUIRED)
            .addColumn("long_col", PhysicalType.INT64, RepetitionType.REQUIRED)
            .addColumn("float_col", PhysicalType.FLOAT, RepetitionType.REQUIRED)
            .addColumn("float16_col", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.REQUIRED, 2)
            .addColumn("double_col", PhysicalType.DOUBLE, RepetitionType.REQUIRED)
            .addColumn("boolean_col", PhysicalType.BOOLEAN, RepetitionType.REQUIRED)
            .addColumn("binary_col", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED)
            .addColumn("int_in_col", PhysicalType.INT32, RepetitionType.REQUIRED)
            .addColumn("long_in_col", PhysicalType.INT64, RepetitionType.REQUIRED)
            .addColumn("binary_in_col", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED)
            .addColumn("nullable_col", PhysicalType.INT32, RepetitionType.OPTIONAL)
            .addColumn("geo_col", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED)
            .build();

    @Test
    void elidesEveryLiteralBearingLeaf() {
        assertThat(render(new ResolvedPredicate.IntPredicate(0, FilterPredicate.Operator.EQ, 123456)))
                .isEqualTo("eq(int_col, ?)");
        assertThat(render(new ResolvedPredicate.LongPredicate(1, FilterPredicate.Operator.NOT_EQ, 987654321L)))
                .isEqualTo("notEq(long_col, ?)");
        assertThat(render(new ResolvedPredicate.FloatPredicate(2, FilterPredicate.Operator.LT, 12.5F)))
                .isEqualTo("lt(float_col, ?)");
        assertThat(render(new ResolvedPredicate.Float16Predicate(3, FilterPredicate.Operator.LT_EQ, 13.5F)))
                .isEqualTo("ltEq(float16_col, ?)");
        assertThat(render(new ResolvedPredicate.DoublePredicate(4, FilterPredicate.Operator.GT, 14.5D)))
                .isEqualTo("gt(double_col, ?)");
        assertThat(render(new ResolvedPredicate.BooleanPredicate(5, FilterPredicate.Operator.EQ, true)))
                .isEqualTo("eq(boolean_col, ?)");
        assertThat(render(new ResolvedPredicate.BinaryPredicate(
                6, FilterPredicate.Operator.EQ, "secret@example.com".getBytes(StandardCharsets.UTF_8), false)))
                .isEqualTo("eq(binary_col, ?)");
        assertThat(render(new ResolvedPredicate.IntInPredicate(7, new int[]{ 123, 456, 789 })))
                .isEqualTo("in(int_in_col, ?×3)");
        assertThat(render(new ResolvedPredicate.LongInPredicate(8, new long[]{ 123L, 456L })))
                .isEqualTo("in(long_in_col, ?×2)");
        assertThat(render(new ResolvedPredicate.BinaryInPredicate(9, new byte[][]{
                "first-secret".getBytes(StandardCharsets.UTF_8),
                "second-secret".getBytes(StandardCharsets.UTF_8) })))
                .isEqualTo("in(binary_in_col, ?×2)");
    }

    @Test
    void rendersNullChecksAndCompoundStructure() {
        ResolvedPredicate predicate = new ResolvedPredicate.And(List.of(
                new ResolvedPredicate.IsNullPredicate(10),
                new ResolvedPredicate.Or(List.of(
                        new ResolvedPredicate.IsNotNullPredicate(10),
                        new ResolvedPredicate.IntPredicate(0, FilterPredicate.Operator.GT, 42)))));

        assertThat(render(predicate))
                .isEqualTo("and(isNull(nullable_col), or(isNotNull(nullable_col), gt(int_col, ?)))");
    }

    @Test
    void rendersGeospatialBoundsInFull() {
        ResolvedPredicate predicate = new ResolvedPredicate.GeospatialPredicate(11, 1.5, 2.5, 3.5, 4.5);

        assertThat(render(predicate)).isEqualTo("intersects(geo_col, 1.5, 2.5, 3.5, 4.5)");
    }

    @Test
    void usesTheFullPathForNestedColumns() {
        FileSchema nested = FileSchema.builder("schema")
                .struct("address", RepetitionType.OPTIONAL,
                        address -> address.addColumn("zip", PhysicalType.INT32, RepetitionType.REQUIRED))
                .build();
        ResolvedPredicate predicate = new ResolvedPredicate.IntPredicate(0, FilterPredicate.Operator.EQ, 90210);

        assertThat(ResolvedPredicateRenderer.render(predicate, nested)).isEqualTo("eq(address.zip, ?)");
    }

    private static String render(ResolvedPredicate predicate) {
        return ResolvedPredicateRenderer.render(predicate, SCHEMA);
    }
}
