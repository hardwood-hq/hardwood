/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.schema;

import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;

import static org.assertj.core.api.Assertions.assertThat;

/// Which pairings of physical type, annotation and width [AnnotationPairings#check] calls legal,
/// and the fault it answers for the rest.
///
/// The messages a refusal reaches a caller as are pinned where each caller raises them:
/// `ConversionFaultTest` for the reader's wording, `FileSchemaLogicalTypeTest` for the writer's.
class AnnotationPairingsTest {

    /// Every annotation over the physical type and width parquet-format defines it on. A cell left
    /// unasserted is how a pairing the reader keeps and the writer refuses went unnoticed (#1406),
    /// so each legal cell of the grid is named here.
    @ParameterizedTest
    @MethodSource("legalCells")
    void anAnnotationIsLegalOverThePhysicalTypesTheFormatDefinesItOver(PhysicalType type, Integer typeLength,
            LogicalType annotation) {
        assertThat(check(type, typeLength, annotation)).isInstanceOf(Pairing.Legal.class);
    }

    private static Stream<Arguments> legalCells() {
        return Stream.of(
                Arguments.of(PhysicalType.BYTE_ARRAY, null, LogicalType.string()),
                Arguments.of(PhysicalType.BYTE_ARRAY, null, LogicalType.enumType()),
                Arguments.of(PhysicalType.BYTE_ARRAY, null, LogicalType.json()),
                Arguments.of(PhysicalType.BYTE_ARRAY, null, LogicalType.bson()),
                Arguments.of(PhysicalType.BYTE_ARRAY, null, LogicalType.geometry(null)),
                Arguments.of(PhysicalType.BYTE_ARRAY, null, LogicalType.geography(null, null)),
                Arguments.of(PhysicalType.INT32, null, LogicalType.date()),
                Arguments.of(PhysicalType.INT32, null, LogicalType.time(true, LogicalType.TimeUnit.MILLIS)),
                Arguments.of(PhysicalType.INT64, null, LogicalType.time(true, LogicalType.TimeUnit.MICROS)),
                Arguments.of(PhysicalType.INT64, null, LogicalType.time(true, LogicalType.TimeUnit.NANOS)),
                Arguments.of(PhysicalType.INT32, null, LogicalType.intType(8, true)),
                Arguments.of(PhysicalType.INT32, null, LogicalType.intType(16, false)),
                Arguments.of(PhysicalType.INT32, null, LogicalType.intType(32, true)),
                Arguments.of(PhysicalType.INT64, null, LogicalType.intType(64, false)),
                Arguments.of(PhysicalType.INT64, null, timestamp()),
                Arguments.of(PhysicalType.FIXED_LEN_BYTE_ARRAY, 12, timestamp()),
                Arguments.of(PhysicalType.FIXED_LEN_BYTE_ARRAY, 16, LogicalType.uuid()),
                Arguments.of(PhysicalType.FIXED_LEN_BYTE_ARRAY, 12, LogicalType.interval()),
                Arguments.of(PhysicalType.FIXED_LEN_BYTE_ARRAY, 2, LogicalType.float16()),
                // UNKNOWN says every value is null, which no physical type contradicts.
                Arguments.of(PhysicalType.BOOLEAN, null, LogicalType.nullType()),
                Arguments.of(PhysicalType.INT96, null, LogicalType.nullType()),
                // An unannotated column of any physical type.
                Arguments.of(PhysicalType.BOOLEAN, null, null),
                Arguments.of(PhysicalType.INT64, null, null),
                Arguments.of(PhysicalType.INT96, null, null),
                Arguments.of(PhysicalType.FLOAT, null, null),
                Arguments.of(PhysicalType.DOUBLE, null, null),
                Arguments.of(PhysicalType.BYTE_ARRAY, null, null),
                Arguments.of(PhysicalType.FIXED_LEN_BYTE_ARRAY, 7, null));
    }

    /// The annotations parquet-format gives no order, against those it does. A column without one
    /// records no bounds, reads none and takes no ordered predicate.
    @Test
    void theFormatNamesNoOrderForIntervalGeospatialUnknownAndTheGroupAnnotations() {
        assertThat(AnnotationPairings.namesAnOrder(LogicalType.interval())).isFalse();
        assertThat(AnnotationPairings.namesAnOrder(LogicalType.geometry(null))).isFalse();
        assertThat(AnnotationPairings.namesAnOrder(LogicalType.geography(null, null))).isFalse();
        assertThat(AnnotationPairings.namesAnOrder(LogicalType.nullType())).isFalse();
        assertThat(AnnotationPairings.namesAnOrder(LogicalType.variant(1))).isFalse();
        assertThat(AnnotationPairings.namesAnOrder(LogicalType.list())).isFalse();
        assertThat(AnnotationPairings.namesAnOrder(LogicalType.map())).isFalse();

        assertThat(AnnotationPairings.namesAnOrder(LogicalType.string())).isTrue();
        assertThat(AnnotationPairings.namesAnOrder(LogicalType.decimal(10, 2))).isTrue();
        assertThat(AnnotationPairings.namesAnOrder(LogicalType.float16())).isTrue();
        assertThat(AnnotationPairings.namesAnOrder(timestamp())).isTrue();
        // An unannotated column takes the order of its physical type.
        assertThat(AnnotationPairings.namesAnOrder(null)).isTrue();
    }

    /// A `DECIMAL` is the one annotation four physical types hold, each to its own digit limit.
    @Test
    void aDecimalIsLegalOnFourPhysicalTypesWithinTheirDigits() {
        assertThat(check(PhysicalType.INT32, null, LogicalType.decimal(9, 2))).isInstanceOf(Pairing.Legal.class);
        assertThat(check(PhysicalType.INT64, null, LogicalType.decimal(18, 2))).isInstanceOf(Pairing.Legal.class);
        assertThat(check(PhysicalType.BYTE_ARRAY, null, LogicalType.decimal(40, 2)))
                .isInstanceOf(Pairing.Legal.class);
        assertThat(check(PhysicalType.FIXED_LEN_BYTE_ARRAY, 8, LogicalType.decimal(18, 2)))
                .isInstanceOf(Pairing.Legal.class);
    }

    @Test
    void anAnnotationOnAPhysicalTypeItIsNotDefinedOverIsIllegal() {
        assertThat(fault(PhysicalType.INT32, null, LogicalType.string()))
                .isEqualTo(new Pairing.Fault.WrongPhysicalType(List.of(PhysicalType.BYTE_ARRAY)));
        assertThat(fault(PhysicalType.INT64, null, LogicalType.date()))
                .isEqualTo(new Pairing.Fault.WrongPhysicalType(List.of(PhysicalType.INT32)));
        assertThat(fault(PhysicalType.INT32, null, timestamp()))
                .isEqualTo(new Pairing.Fault.WrongPhysicalType(
                        List.of(PhysicalType.INT64, PhysicalType.FIXED_LEN_BYTE_ARRAY)));
        assertThat(fault(PhysicalType.BOOLEAN, null, LogicalType.decimal(4, 2)))
                .isEqualTo(new Pairing.Fault.WrongPhysicalType(List.of(PhysicalType.INT32, PhysicalType.INT64,
                        PhysicalType.BYTE_ARRAY, PhysicalType.FIXED_LEN_BYTE_ARRAY)));
        assertThat(fault(PhysicalType.INT64, null, LogicalType.intType(32, true)))
                .isEqualTo(new Pairing.Fault.WrongPhysicalType(List.of(PhysicalType.INT32)));
    }

    @Test
    void anAnnotationOnTheWrongWidthIsIllegal() {
        assertThat(fault(PhysicalType.FIXED_LEN_BYTE_ARRAY, 8, LogicalType.uuid()))
                .isEqualTo(new Pairing.Fault.WrongWidth(16));
        assertThat(fault(PhysicalType.FIXED_LEN_BYTE_ARRAY, 3, LogicalType.float16()))
                .isEqualTo(new Pairing.Fault.WrongWidth(2));
        assertThat(fault(PhysicalType.FIXED_LEN_BYTE_ARRAY, 8, LogicalType.interval()))
                .isEqualTo(new Pairing.Fault.WrongWidth(12));
        assertThat(fault(PhysicalType.FIXED_LEN_BYTE_ARRAY, 16, timestamp()))
                .isEqualTo(new Pairing.Fault.WrongWidth(12));
    }

    /// A `FIXED_LEN_BYTE_ARRAY` that declares no width states nothing the annotation contradicts.
    /// `FixedWidthValidator` refuses that column by name, and the writer refuses the missing width
    /// where a schema is declared, so the pairing itself is legal here.
    @Test
    void anUndeclaredWidthIsNotTheAnnotationsFault() {
        assertThat(check(PhysicalType.FIXED_LEN_BYTE_ARRAY, null, LogicalType.uuid()))
                .isInstanceOf(Pairing.Legal.class);
        assertThat(check(PhysicalType.FIXED_LEN_BYTE_ARRAY, null, LogicalType.decimal(40, 2)))
                .isInstanceOf(Pairing.Legal.class);
    }

    @Test
    void aDecimalBeyondTheDigitsItsColumnHoldsIsIllegal() {
        assertThat(fault(PhysicalType.INT32, null, LogicalType.decimal(10, 2)))
                .isEqualTo(new Pairing.Fault.PrecisionTooLarge(10, 9));
        assertThat(fault(PhysicalType.INT64, null, LogicalType.decimal(19, 2)))
                .isEqualTo(new Pairing.Fault.PrecisionTooLarge(19, 18));
        assertThat(fault(PhysicalType.FIXED_LEN_BYTE_ARRAY, 3, LogicalType.decimal(7, 2)))
                .isEqualTo(new Pairing.Fault.PrecisionTooLarge(7, 6));
    }

    @Test
    void aGroupAnnotationOnAPrimitiveIsIllegal() {
        assertThat(fault(PhysicalType.BYTE_ARRAY, null, LogicalType.list()))
                .isEqualTo(new Pairing.Fault.GroupAnnotation());
        assertThat(fault(PhysicalType.BYTE_ARRAY, null, LogicalType.map()))
                .isEqualTo(new Pairing.Fault.GroupAnnotation());
        assertThat(fault(PhysicalType.BYTE_ARRAY, null, LogicalType.variant(1)))
                .isEqualTo(new Pairing.Fault.GroupAnnotation());
    }

    /// `UNKNOWN` says every value is null, which no physical type contradicts.
    @Test
    void unknownIsLegalOverEveryPhysicalType() {
        for (PhysicalType type : PhysicalType.values()) {
            Integer width = type == PhysicalType.FIXED_LEN_BYTE_ARRAY ? 4 : null;
            assertThat(AnnotationPairings.check(type, width, LogicalType.nullType()))
                    .as("UNKNOWN over %s", type)
                    .isInstanceOf(Pairing.Legal.class);
        }
    }

    private static Pairing check(PhysicalType type, Integer typeLength, LogicalType annotation) {
        return AnnotationPairings.check(type, typeLength, annotation);
    }

    private static Pairing.Fault fault(PhysicalType type, Integer typeLength, LogicalType annotation) {
        Pairing pairing = check(type, typeLength, annotation);
        assertThat(pairing).isInstanceOf(Pairing.Illegal.class);
        return ((Pairing.Illegal) pairing).fault();
    }

    private static LogicalType timestamp() {
        return LogicalType.timestamp(true, LogicalType.TimeUnit.MICROS);
    }
}
