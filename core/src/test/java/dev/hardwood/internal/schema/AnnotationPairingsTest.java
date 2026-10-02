/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.schema;

import java.math.BigInteger;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.metadata.ColumnOrder;
import dev.hardwood.metadata.ConvertedType;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    /// A `FIXED_LEN_BYTE_ARRAY` that declares no width, or a width that is not positive, states
    /// nothing the annotation contradicts. `FixedWidthValidator` refuses that column by name, and
    /// the writer refuses the width where a schema is declared, so the pairing itself is legal
    /// here, for every annotation that fixes or counts a width alike.
    @ParameterizedTest
    @MethodSource("unusableWidths")
    void anUnusableWidthIsNotTheAnnotationsFault(Integer typeLength) {
        assertThat(check(PhysicalType.FIXED_LEN_BYTE_ARRAY, typeLength, LogicalType.uuid()))
                .isInstanceOf(Pairing.Legal.class);
        assertThat(check(PhysicalType.FIXED_LEN_BYTE_ARRAY, typeLength, LogicalType.interval()))
                .isInstanceOf(Pairing.Legal.class);
        assertThat(check(PhysicalType.FIXED_LEN_BYTE_ARRAY, typeLength, LogicalType.float16()))
                .isInstanceOf(Pairing.Legal.class);
        assertThat(check(PhysicalType.FIXED_LEN_BYTE_ARRAY, typeLength, timestamp()))
                .isInstanceOf(Pairing.Legal.class);
        assertThat(check(PhysicalType.FIXED_LEN_BYTE_ARRAY, typeLength, LogicalType.decimal(40, 2)))
                .isInstanceOf(Pairing.Legal.class);
    }

    private static Stream<Integer> unusableWidths() {
        return Stream.of(null, 0, -1);
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

    /// The order a byte-stored column's values sort in, which the writer's bounds and the
    /// resolver's byte literals both read.
    @Test
    void aByteColumnSortsInTheOrderItsAnnotationNames() {
        assertThat(AnnotationPairings.byteColumnOrder(PhysicalType.BYTE_ARRAY, null))
                .isEqualTo(ByteColumnOrder.BYTES);
        assertThat(AnnotationPairings.byteColumnOrder(PhysicalType.FIXED_LEN_BYTE_ARRAY, LogicalType.uuid()))
                .isEqualTo(ByteColumnOrder.BYTES);
        assertThat(AnnotationPairings.byteColumnOrder(PhysicalType.BYTE_ARRAY, LogicalType.decimal(20, 2)))
                .isEqualTo(ByteColumnOrder.SIGNED_BIG_ENDIAN);
        assertThat(AnnotationPairings.byteColumnOrder(PhysicalType.FIXED_LEN_BYTE_ARRAY, LogicalType.decimal(9, 2)))
                .isEqualTo(ByteColumnOrder.SIGNED_BIG_ENDIAN);
        assertThat(AnnotationPairings.byteColumnOrder(PhysicalType.FIXED_LEN_BYTE_ARRAY, timestamp()))
                .isEqualTo(ByteColumnOrder.SIGNED_LITTLE_ENDIAN);
        assertThat(AnnotationPairings.byteColumnOrder(PhysicalType.FIXED_LEN_BYTE_ARRAY, LogicalType.float16()))
                .isEqualTo(ByteColumnOrder.HALF_FLOAT);
        assertThat(AnnotationPairings.byteColumnOrder(PhysicalType.INT96, null))
                .isEqualTo(ByteColumnOrder.INT96_INSTANT);
        assertThat(AnnotationPairings.byteColumnOrder(PhysicalType.FIXED_LEN_BYTE_ARRAY, LogicalType.interval()))
                .isEqualTo(ByteColumnOrder.NONE);
        assertThat(AnnotationPairings.byteColumnOrder(PhysicalType.INT96, LogicalType.nullType()))
                .isEqualTo(ByteColumnOrder.INT96_INSTANT);
    }

    /// `IEEE_754_TOTAL_ORDER` is defined for a `FLOAT`, a `DOUBLE` and a `FLOAT16` alone, the type
    /// order is the annotation's, and an order this release does not recognize names nothing.
    @Test
    void aColumnOrderNamesAnOrderOnlyOverTheColumnsItIsDefinedFor() {
        assertThat(AnnotationPairings.namesAnOrder(ColumnOrder.IEEE754_TOTAL_ORDER, PhysicalType.FLOAT, null))
                .isTrue();
        assertThat(AnnotationPairings.namesAnOrder(ColumnOrder.IEEE754_TOTAL_ORDER, PhysicalType.DOUBLE, null))
                .isTrue();
        assertThat(AnnotationPairings.namesAnOrder(ColumnOrder.IEEE754_TOTAL_ORDER,
                PhysicalType.FIXED_LEN_BYTE_ARRAY, LogicalType.float16())).isTrue();
        assertThat(AnnotationPairings.namesAnOrder(ColumnOrder.IEEE754_TOTAL_ORDER, PhysicalType.INT32, null))
                .isFalse();
        assertThat(AnnotationPairings.namesAnOrder(ColumnOrder.IEEE754_TOTAL_ORDER,
                PhysicalType.FIXED_LEN_BYTE_ARRAY, null)).isFalse();
        assertThat(AnnotationPairings.namesAnOrder(ColumnOrder.IEEE754_TOTAL_ORDER, PhysicalType.FLOAT,
                LogicalType.nullType())).isFalse();

        assertThat(AnnotationPairings.namesAnOrder(ColumnOrder.TYPE_DEFINED_ORDER, PhysicalType.INT32, null))
                .isTrue();
        assertThat(AnnotationPairings.namesAnOrder(ColumnOrder.TYPE_DEFINED_ORDER,
                PhysicalType.FIXED_LEN_BYTE_ARRAY, LogicalType.interval())).isFalse();
        assertThat(AnnotationPairings.namesAnOrder(ColumnOrder.UNKNOWN, PhysicalType.INT32, null)).isFalse();
    }

    @Test
    void aColumnNotStoredAsBytesHasNoByteOrder() {
        assertThatThrownBy(() -> AnnotationPairings.byteColumnOrder(PhysicalType.INT64, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("INT64 is not stored as bytes");
    }

    /// The legacy `TIMESTAMP_MILLIS` and `TIMESTAMP_MICROS` annotate an `INT64` alone, although
    /// the `TIMESTAMP` they stand for is also carried by a `FIXED_LEN_BYTE_ARRAY(12)`; every other
    /// converted type adds nothing to its logical counterpart's pairing.
    @Test
    void theLegacyTimestampsAnnotateAnInt64Alone() {
        assertThat(AnnotationPairings.checkConverted(PhysicalType.INT64, ConvertedType.TIMESTAMP_MILLIS))
                .isInstanceOf(Pairing.Legal.class);
        assertThat(AnnotationPairings.checkConverted(PhysicalType.FIXED_LEN_BYTE_ARRAY, ConvertedType.TIMESTAMP_MICROS))
                .isEqualTo(new Pairing.Illegal(new Pairing.Fault.WrongPhysicalType(List.of(PhysicalType.INT64))));
        assertThat(AnnotationPairings.checkConverted(PhysicalType.INT32, ConvertedType.TIMESTAMP_MILLIS))
                .isEqualTo(new Pairing.Illegal(new Pairing.Fault.WrongPhysicalType(List.of(PhysicalType.INT64))));
        assertThat(AnnotationPairings.checkConverted(PhysicalType.INT32, ConvertedType.TIME_MILLIS))
                .isInstanceOf(Pairing.Legal.class);
    }

    /// The digits a `DECIMAL` carrier holds, as [AnnotationPairings#maxDecimalPrecision] counts
    /// them for both the writer and the reader.
    @Test
    void theIntegerCarriersHoldNineAndEighteenDigits() {
        assertThat(AnnotationPairings.maxDecimalPrecision(PhysicalType.INT32)).isEqualTo(9);
        assertThat(AnnotationPairings.maxDecimalPrecision(PhysicalType.INT64)).isEqualTo(18);
        assertThat(AnnotationPairings.maxDecimalPrecision(PhysicalType.BYTE_ARRAY))
                .isEqualTo(Long.MAX_VALUE);
    }

    /// Counted against the digits of the largest value each width holds, `2^(8n - 1) - 1`.
    @Test
    void aFixedWidthHoldsTheDigitsOfItsLargestValue() {
        for (int width = 1; width <= 1024; width++) {
            int exact = BigInteger.ONE.shiftLeft(8 * width - 1).subtract(BigInteger.ONE)
                    .toString().length() - 1;
            assertThat(AnnotationPairings.maxDecimalPrecision(width))
                    .as("width %d", width)
                    .isEqualTo(exact);
        }
    }

    /// The widest width an `i32` can declare holds more digits than an `int` counts.
    @Test
    void theWidestWidthIsCountedWithoutOverflow() {
        assertThat(AnnotationPairings.maxDecimalPrecision(Integer.MAX_VALUE)).isEqualTo(5_171_655_943L);
    }

    @Test
    void aTypeThatStoresNoDecimalIsRefused() {
        assertThatThrownBy(() -> AnnotationPairings.maxDecimalPrecision(PhysicalType.BOOLEAN))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("DECIMAL is not stored in BOOLEAN");
    }

    /// A width that is not positive has no digits to count.
    @Test
    void aFixedWidthThatIsNotPositiveIsRefused() {
        assertThatThrownBy(() -> AnnotationPairings.maxDecimalPrecision(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("A FIXED_LEN_BYTE_ARRAY DECIMAL needs a positive width, not 0");
        assertThatThrownBy(() -> AnnotationPairings.maxDecimalPrecision(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("A FIXED_LEN_BYTE_ARRAY DECIMAL needs a positive width, not -1");
    }

    /// A `FIXED_LEN_BYTE_ARRAY`'s digits are its width's, so asking its type alone is a misuse.
    @Test
    void aFixedLenByteArrayTypeHasNoDigitsOfItsOwn() {
        assertThatThrownBy(() -> AnnotationPairings.maxDecimalPrecision(PhysicalType.FIXED_LEN_BYTE_ARRAY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("A FIXED_LEN_BYTE_ARRAY DECIMAL holds the digits of its width, not of its type");
    }
}
