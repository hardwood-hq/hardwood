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
/// The answer must match what the conversions themselves accept: too lenient and a column
/// reaches a conversion that cannot decode it, too strict and a file that reads today stops
/// opening. `LogicalTypeValidator` states the writer's rule, which is deliberately stricter,
/// and the cases below pin the two apart where they differ.
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

    /// Reading accepts either width for `TIME` and `INT` whatever the unit or bit width,
    /// where the writer pins `TIME(MILLIS)` to `INT32`. Applying the writer's rule here
    /// would refuse files that read today.
    @Test
    void readingIsLenientWhereWritingIsStrict() {
        assertThat(fault(PhysicalType.INT64, null,
                LogicalType.time(true, LogicalType.TimeUnit.MILLIS))).isNull();
        assertThat(fault(PhysicalType.INT64, null, LogicalType.intType(8, true))).isNull();
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

    /// A `DECIMAL` on a `FIXED_LEN_BYTE_ARRAY` imposes no particular width: the precision
    /// decides how many bytes the unscaled value needs, and the conversion reads whatever
    /// the column declares.
    @Test
    void aFixedLenDecimalImposesNoWidth() {
        assertThat(fault(PhysicalType.FIXED_LEN_BYTE_ARRAY, 3, decimal())).isNull();
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
    /// annotation would drop a sound one and describe the wrong defect.
    @Test
    void anUndeclaredWidthIsNotTheAnnotationsFault() {
        assertThat(fault(PhysicalType.FIXED_LEN_BYTE_ARRAY, null, LogicalType.float16()))
                .isNull();
    }

    private static String fault(PhysicalType type, Integer typeLength, LogicalType logicalType) {
        return LogicalTypeConverter.conversionFault(type, typeLength, logicalType);
    }

    private static LogicalType.DecimalType decimal() {
        return LogicalType.decimal(10, 4);
    }

    private static LogicalType.TimestampType timestamp() {
        return LogicalType.timestamp(true, LogicalType.TimeUnit.MILLIS);
    }
}
