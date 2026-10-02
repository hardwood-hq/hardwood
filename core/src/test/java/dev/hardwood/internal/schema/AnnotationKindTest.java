/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.schema;

import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.ConvertedType;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// The facts [AnnotationKind] states about an annotation alone. The union field ids are checked
/// against an independent implementation of `parquet.thrift` in `LogicalTypeReferenceCodecTest`.
class AnnotationKindTest {

    /// The annotations parquet-format gives no order, against those it does. A column without one
    /// records no bounds, reads none and takes no ordered predicate.
    @Test
    void theFormatNamesNoOrderForIntervalGeospatialUnknownAndTheGroupAnnotations() {
        assertThat(AnnotationKind.namesAnOrder(LogicalType.interval())).isFalse();
        assertThat(AnnotationKind.namesAnOrder(LogicalType.geometry(null))).isFalse();
        assertThat(AnnotationKind.namesAnOrder(LogicalType.geography(null, null))).isFalse();
        assertThat(AnnotationKind.namesAnOrder(LogicalType.nullType())).isFalse();
        assertThat(AnnotationKind.namesAnOrder(LogicalType.variant(1))).isFalse();
        assertThat(AnnotationKind.namesAnOrder(LogicalType.list())).isFalse();
        assertThat(AnnotationKind.namesAnOrder(LogicalType.map())).isFalse();

        assertThat(AnnotationKind.namesAnOrder(LogicalType.string())).isTrue();
        assertThat(AnnotationKind.namesAnOrder(LogicalType.decimal(10, 2))).isTrue();
        assertThat(AnnotationKind.namesAnOrder(LogicalType.float16())).isTrue();
        assertThat(AnnotationKind.namesAnOrder(LogicalType.timestamp(true, LogicalType.TimeUnit.MICROS))).isTrue();
        // An unannotated column takes the order of its physical type.
        assertThat(AnnotationKind.namesAnOrder(null)).isTrue();
    }

    /// Only an unsigned `INT` compares unsigned, at every width; an unannotated column and every
    /// other annotation compare signed.
    @Test
    void onlyAnUnsignedIntOrdersUnsigned() {
        assertThat(AnnotationKind.ordersUnsigned(LogicalType.intType(8, false))).isTrue();
        assertThat(AnnotationKind.ordersUnsigned(LogicalType.intType(64, false))).isTrue();

        assertThat(AnnotationKind.ordersUnsigned(LogicalType.intType(8, true))).isFalse();
        assertThat(AnnotationKind.ordersUnsigned(LogicalType.intType(64, true))).isFalse();
        assertThat(AnnotationKind.ordersUnsigned(LogicalType.decimal(9, 2))).isFalse();
        assertThat(AnnotationKind.ordersUnsigned(null)).isFalse();
    }

    /// `LIST`, `MAP` and `VARIANT` annotate a group, every other annotation a primitive.
    @Test
    void theStructureAnnotationsAnnotateAGroup() {
        assertThat(AnnotationKind.of(LogicalType.list()).annotatesGroup()).isTrue();
        assertThat(AnnotationKind.of(LogicalType.map()).annotatesGroup()).isTrue();
        assertThat(AnnotationKind.of(LogicalType.variant(1)).annotatesGroup()).isTrue();

        assertThat(AnnotationKind.of(LogicalType.string()).annotatesGroup()).isFalse();
        assertThat(AnnotationKind.of(LogicalType.nullType()).annotatesGroup()).isFalse();
    }

    /// The widths a `FIXED_LEN_BYTE_ARRAY` declared without one takes, each legal under
    /// [AnnotationPairings#check]. `TIMESTAMP` implies none, an `INT64` carrying it as well.
    @Test
    void theImpliedWidthIsLegalForItsAnnotation() {
        assertThat(AnnotationKind.impliedFixedWidth(LogicalType.uuid())).isEqualTo(16);
        assertThat(AnnotationKind.impliedFixedWidth(LogicalType.interval())).isEqualTo(12);
        assertThat(AnnotationKind.impliedFixedWidth(LogicalType.float16())).isEqualTo(2);
        assertThat(AnnotationKind.impliedFixedWidth(LogicalType.decimal(10, 2))).isEqualTo(5);
        for (LogicalType annotation : List.of(LogicalType.uuid(), LogicalType.interval(), LogicalType.float16(),
                LogicalType.decimal(10, 2))) {
            assertThat(AnnotationPairings.check(PhysicalType.FIXED_LEN_BYTE_ARRAY,
                    AnnotationKind.impliedFixedWidth(annotation), annotation))
                    .as("%s", annotation)
                    .isInstanceOf(Pairing.Legal.class);
        }

        assertThat(AnnotationKind.impliedFixedWidth(LogicalType.timestamp(true, LogicalType.TimeUnit.NANOS)))
                .isNull();
        assertThat(AnnotationKind.impliedFixedWidth(LogicalType.string())).isNull();
        assertThat(AnnotationKind.impliedFixedWidth(null)).isNull();
    }

    /// A fact that depends on the record is read off a record of the constant's own kind alone: a
    /// `JSON` passed to `STRING` would otherwise be given `STRING`'s converted type.
    @Test
    void anAnnotationOfAnotherKindIsRefused() {
        assertThat(AnnotationKind.STRING.convertedType(LogicalType.string())).isEqualTo(ConvertedType.UTF8);

        assertThatThrownBy(() -> AnnotationKind.STRING.convertedType(LogicalType.json()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("JSON is of kind JSON, not STRING");
    }
}
