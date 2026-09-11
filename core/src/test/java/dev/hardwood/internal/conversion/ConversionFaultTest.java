/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.conversion;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;

import static org.assertj.core.api.Assertions.assertThat;

/// Which annotations a column can carry, as [LogicalTypeConverter#conversionFault] answers it.
///
/// An annotation is faulted where the format rules it out for the column's physical type or
/// width. `LogicalTypeValidator` applies the same rule to the writer; the two differ only where
/// a file on disk leaves nothing to prove wrong — an undeclared width, an opaque payload,
/// `NULL` — and the cases below pin those.
class ConversionFaultTest {

    @Test
    void anAnnotationItsPhysicalTypeCarriesHasNoFault() {
        assertThat(fault(PhysicalType.BYTE_ARRAY, null, LogicalType.string())).isNull();
        assertThat(fault(PhysicalType.INT32, null, LogicalType.date())).isNull();
        assertThat(fault(PhysicalType.INT64, null, timestamp())).isNull();
        assertThat(fault(PhysicalType.FIXED_LEN_BYTE_ARRAY, 2, LogicalType.float16())).isNull();
        assertThat(fault(PhysicalType.FIXED_LEN_BYTE_ARRAY, 16, LogicalType.uuid())).isNull();
        assertThat(fault(PhysicalType.FIXED_LEN_BYTE_ARRAY, 12, LogicalType.interval())).isNull();
    }

    @Test
    void anUnannotatedColumnHasNoFault() {
        assertThat(fault(PhysicalType.INT64, null, null)).isNull();
    }

    @Test
    void anAnnotationItsPhysicalTypeCannotCarryIsFaulted() {
        assertThat(fault(PhysicalType.INT64, null, LogicalType.date()))
                .isEqualTo("DATE is read from INT32, but the column is INT64");
        assertThat(fault(PhysicalType.INT32, null, LogicalType.string()))
                .isEqualTo("STRING is read from BYTE_ARRAY, but the column is INT32");
        assertThat(fault(PhysicalType.INT32, null, timestamp()))
                .isEqualTo("TIMESTAMP is read from INT64, but the column is INT32");
    }

    @Test
    void aFixedWidthAnnotationOnTheWrongWidthIsFaulted() {
        assertThat(fault(PhysicalType.FIXED_LEN_BYTE_ARRAY, 3, LogicalType.float16()))
                .isEqualTo("FLOAT16 is exactly 2 bytes, but the column declares 3");
        assertThat(fault(PhysicalType.FIXED_LEN_BYTE_ARRAY, 8, LogicalType.uuid()))
                .isEqualTo("UUID is exactly 16 bytes, but the column declares 8");
        assertThat(fault(PhysicalType.FIXED_LEN_BYTE_ARRAY, 8, LogicalType.interval()))
                .isEqualTo("INTERVAL is exactly 12 bytes, but the column declares 8");
    }

    /// The wrong physical type is reported before the width, because a column that is not
    /// fixed-width has no width to be wrong about.
    @Test
    void theWrongPhysicalTypeIsReportedAheadOfTheWidth() {
        assertThat(fault(PhysicalType.INT64, null, LogicalType.float16()))
                .isEqualTo("FLOAT16 is read from FIXED_LEN_BYTE_ARRAY, but the column is INT64");
        assertThat(fault(PhysicalType.INT64, null, LogicalType.interval()))
                .isEqualTo("INTERVAL is read from FIXED_LEN_BYTE_ARRAY, but the column is INT64");
        assertThat(fault(PhysicalType.BYTE_ARRAY, null, LogicalType.uuid()))
                .isEqualTo("UUID is read from FIXED_LEN_BYTE_ARRAY, but the column is BYTE_ARRAY");
    }

    /// `TIME(MILLIS)` is stored in an `INT32` and `TIME(MICROS / NANOS)` in an `INT64`, so
    /// each unit is faulted on the other width.
    @Test
    void aTimeUnitOnTheOtherWidthIsFaulted() {
        assertThat(fault(PhysicalType.INT32, null, time(LogicalType.TimeUnit.MILLIS))).isNull();
        assertThat(fault(PhysicalType.INT64, null, time(LogicalType.TimeUnit.MICROS))).isNull();
        assertThat(fault(PhysicalType.INT64, null, time(LogicalType.TimeUnit.NANOS))).isNull();
        assertThat(fault(PhysicalType.INT64, null, time(LogicalType.TimeUnit.MILLIS)))
                .isEqualTo("TIME(MILLIS) is read from INT32, but the column is INT64");
        assertThat(fault(PhysicalType.INT32, null, time(LogicalType.TimeUnit.MICROS)))
                .isEqualTo("TIME(MICROS) is read from INT64, but the column is INT32");
        assertThat(fault(PhysicalType.INT32, null, time(LogicalType.TimeUnit.NANOS)))
                .isEqualTo("TIME(NANOS) is read from INT64, but the column is INT32");
    }

    /// `INT(8)`, `INT(16)` and `INT(32)` are stored in an `INT32` and `INT(64)` in an
    /// `INT64`, signed or not, so each bit width is faulted on the other width.
    @Test
    void anIntBitWidthOnTheOtherWidthIsFaulted() {
        assertThat(fault(PhysicalType.INT32, null, LogicalType.intType(8, true))).isNull();
        assertThat(fault(PhysicalType.INT32, null, LogicalType.intType(32, false))).isNull();
        assertThat(fault(PhysicalType.INT64, null, LogicalType.intType(64, false))).isNull();
        assertThat(fault(PhysicalType.INT64, null, LogicalType.intType(8, true)))
                .isEqualTo("INT(8) is read from INT32, but the column is INT64");
        assertThat(fault(PhysicalType.INT64, null, LogicalType.intType(32, false)))
                .isEqualTo("INT(32) is read from INT32, but the column is INT64");
        assertThat(fault(PhysicalType.INT32, null, LogicalType.intType(64, true)))
                .isEqualTo("INT(64) is read from INT64, but the column is INT32");
    }

    /// A `DECIMAL`'s precision must fit the digits its carrier holds: nine for an `INT32`,
    /// eighteen for an `INT64`, and for a `FIXED_LEN_BYTE_ARRAY` what the two's complement of
    /// its width spans. A `BYTE_ARRAY` holds any number.
    @Test
    void aDecimalBeyondItsCarriersDigitsIsFaulted() {
        assertThat(fault(PhysicalType.INT32, null, LogicalType.decimal(9, 2))).isNull();
        assertThat(fault(PhysicalType.INT64, null, LogicalType.decimal(18, 0))).isNull();
        assertThat(fault(PhysicalType.BYTE_ARRAY, null, LogicalType.decimal(1000, 0))).isNull();
        assertThat(fault(PhysicalType.INT32, null, LogicalType.decimal(12, 2)))
                .isEqualTo("DECIMAL(12, 2) has 12 digits, but INT32 holds at most 9");
        assertThat(fault(PhysicalType.INT64, null, LogicalType.decimal(19, 0)))
                .isEqualTo("DECIMAL(19, 0) has 19 digits, but INT64 holds at most 18");
        assertThat(fault(PhysicalType.FIXED_LEN_BYTE_ARRAY, 3, LogicalType.decimal(7, 2)))
                .isEqualTo("DECIMAL(7, 2) has 7 digits, but FIXED_LEN_BYTE_ARRAY(3) holds at most 6");
        assertThat(fault(PhysicalType.FIXED_LEN_BYTE_ARRAY, 16, LogicalType.decimal(39, 0)))
                .isEqualTo("DECIMAL(39, 0) has 39 digits, but FIXED_LEN_BYTE_ARRAY(16) holds at most 38");
    }

    /// Geometry and geography payloads are carried through untouched, so no physical type
    /// is imposed on them.
    @Test
    void anOpaquePayloadImposesNoPhysicalType() {
        assertThat(fault(PhysicalType.INT32, null, LogicalType.geometry(null))).isNull();
    }

    /// `JSON`, `BSON` and `ENUM` all carry a `BYTE_ARRAY` payload, so each is faulted on
    /// any other physical type even though only `STRING` is decoded to a `String`.
    @Test
    void aByteArrayPayloadAnnotationIsFaultedOnAnyOtherPhysicalType() {
        assertThat(fault(PhysicalType.BYTE_ARRAY, null, LogicalType.json())).isNull();
        assertThat(fault(PhysicalType.BYTE_ARRAY, null, LogicalType.bson())).isNull();
        assertThat(fault(PhysicalType.BYTE_ARRAY, null, LogicalType.enumType())).isNull();
        assertThat(fault(PhysicalType.INT64, null, LogicalType.json()))
                .isEqualTo("JSON is read from BYTE_ARRAY, but the column is INT64");
        assertThat(fault(PhysicalType.INT32, null, LogicalType.bson()))
                .isEqualTo("BSON is read from BYTE_ARRAY, but the column is INT32");
        assertThat(fault(PhysicalType.DOUBLE, null, LogicalType.enumType()))
                .isEqualTo("ENUM is read from BYTE_ARRAY, but the column is DOUBLE");
    }

    /// `DECIMAL` is the one annotation four physical types can carry, and the message
    /// lists them in the order the conversion accepts them.
    @Test
    void decimalIsCarriedByFourPhysicalTypes() {
        assertThat(fault(PhysicalType.INT32, null, decimal())).isNull();
        assertThat(fault(PhysicalType.INT64, null, decimal())).isNull();
        assertThat(fault(PhysicalType.BYTE_ARRAY, null, decimal())).isNull();
        assertThat(fault(PhysicalType.FIXED_LEN_BYTE_ARRAY, 8, decimal())).isNull();
        assertThat(fault(PhysicalType.BOOLEAN, null, decimal()))
                .isEqualTo("DECIMAL is read from INT32, INT64, BYTE_ARRAY or"
                        + " FIXED_LEN_BYTE_ARRAY, but the column is BOOLEAN");
    }

    /// A `DECIMAL` on a `FIXED_LEN_BYTE_ARRAY` takes any width that holds its digits, however
    /// much wider than it needs: three bytes span six digits, sixteen span thirty-eight.
    @Test
    void aFixedLenDecimalTakesAnyWidthThatHoldsItsDigits() {
        assertThat(fault(PhysicalType.FIXED_LEN_BYTE_ARRAY, 3, LogicalType.decimal(6, 2))).isNull();
        assertThat(fault(PhysicalType.FIXED_LEN_BYTE_ARRAY, 16, LogicalType.decimal(38, 2))).isNull();
        assertThat(fault(PhysicalType.FIXED_LEN_BYTE_ARRAY, 32, decimal())).isNull();
    }

    /// `LIST`, `MAP` and `VARIANT` annotate a group. Reaching a primitive leaf, they name a
    /// pairing no version of the format defines, so they are dropped like any other — which
    /// is what keeps them out of `convert`, whose structural arms throw.
    @Test
    void aStructuralAnnotationOnAPrimitiveIsFaulted() {
        assertThat(fault(PhysicalType.BYTE_ARRAY, null, LogicalType.list()))
                .isEqualTo("LIST annotates a group, but the column is a primitive");
        assertThat(fault(PhysicalType.BYTE_ARRAY, null, LogicalType.map()))
                .isEqualTo("MAP annotates a group, but the column is a primitive");
        assertThat(fault(PhysicalType.BYTE_ARRAY, null, LogicalType.variant(1)))
                .isEqualTo("VARIANT annotates a group, but the column is a primitive");
    }

    /// `NULL` is legal over any physical type — it says every value in the column is null.
    /// Only the values can contradict it, and this answers from the schema alone.
    @Test
    void nullIsLegalOverAnyPhysicalType() {
        assertThat(fault(PhysicalType.INT32, null, LogicalType.nullType())).isNull();
        assertThat(fault(PhysicalType.BYTE_ARRAY, null, LogicalType.nullType())).isNull();
    }

    /// A footer that omits `type_length` states no width for the annotation to contradict,
    /// so nothing here is provably wrong and the annotation is kept. Such a column cannot be
    /// decoded at all, and `FixedWidthValidator` refuses it by name — reporting it as a bad
    /// annotation would drop a sound one and describe the wrong defect. A `DECIMAL` has no
    /// digits to count in a width that is absent or not positive, so neither is its fault.
    @Test
    void anUndeclaredWidthIsNotTheAnnotationsFault() {
        assertThat(fault(PhysicalType.FIXED_LEN_BYTE_ARRAY, null, LogicalType.float16()))
                .isNull();
        assertThat(fault(PhysicalType.FIXED_LEN_BYTE_ARRAY, null, decimal())).isNull();
        assertThat(fault(PhysicalType.FIXED_LEN_BYTE_ARRAY, 0, decimal())).isNull();
    }

    private static String fault(PhysicalType type, Integer typeLength, LogicalType logicalType) {
        return LogicalTypeConverter.conversionFault(type, typeLength, logicalType);
    }

    private static LogicalType.DecimalType decimal() {
        return LogicalType.decimal(9, 4);
    }

    private static LogicalType.TimeType time(LogicalType.TimeUnit unit) {
        return LogicalType.time(true, unit);
    }

    private static LogicalType.TimestampType timestamp() {
        return LogicalType.timestamp(true, LogicalType.TimeUnit.MILLIS);
    }
}
