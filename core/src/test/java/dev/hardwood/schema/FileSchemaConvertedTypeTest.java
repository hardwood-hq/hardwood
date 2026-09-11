/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.schema;

import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.ConvertedType;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Characterizes [FileSchema]'s funneling of legacy `converted_type` annotations
/// through `effectiveLogicalType`. Files written by older parquet-mr / Hive /
/// Impala / Spark set only `converted_type` (no modern `LogicalType`); the schema
/// builder must promote each primitive annotation to its `LogicalType` so the read
/// path — which keys exclusively off `logicalType()` — decodes the column correctly.
class FileSchemaConvertedTypeTest {

    private static final String ROOT = "schema";
    private static final String COLUMN = "col";

    /// A legacy `converted_type` is promoted to its modern annotation first, so the same
    /// rule drops it when the column's physical type cannot carry it — a writer that put
    /// `UTF8` on an `INT32` produced a file no version of the format defines, whichever of
    /// the two annotations it used to say so.
    @Test
    void aPromotedConvertedTypeItsPhysicalTypeCannotCarryIsDropped() {
        assertThat(resolveColumn(PhysicalType.INT32, ConvertedType.UTF8).logicalType()).isNull();
        assertThat(resolveColumn(PhysicalType.BYTE_ARRAY, ConvertedType.DATE).logicalType()).isNull();
    }

    /// The same holds where the converted type names a width or a digit count the physical
    /// type does not have: `TIME_MILLIS` and `INT_8` are 32-bit, and an `INT32` holds nine
    /// digits, not twelve.
    @Test
    void aPromotedConvertedTypeItsCarrierCannotHoldIsDropped() {
        assertThat(resolveColumn(PhysicalType.INT64, ConvertedType.TIME_MILLIS).logicalType()).isNull();
        assertThat(resolveColumn(PhysicalType.INT64, ConvertedType.INT_8).logicalType()).isNull();
        assertThat(resolveColumn(PhysicalType.INT32, ConvertedType.DECIMAL, 2, 12).logicalType()).isNull();
    }

    /// Build a one-column schema whose single leaf carries only the given
    /// `convertedType` (no modern logical type), and return the resolved column.
    private static ColumnSchema resolveColumn(PhysicalType type, ConvertedType convertedType) {
        return resolveColumn(type, convertedType, null, null);
    }

    private static ColumnSchema resolveColumn(
            PhysicalType type, ConvertedType convertedType, Integer scale, Integer precision) {
        SchemaElement root = SchemaElement.group(ROOT, RepetitionType.REQUIRED, 1);
        SchemaElement leaf = new SchemaElement(
                COLUMN, type, null, RepetitionType.OPTIONAL, null, convertedType, scale, precision, null, null);
        FileSchema schema = FileSchema.fromSchemaElements(List.of(root, leaf));
        return schema.getColumn(COLUMN);
    }

    @Test
    void utf8ResolvesToStringType() {
        assertThat(resolveColumn(PhysicalType.BYTE_ARRAY, ConvertedType.UTF8).logicalType())
                .isEqualTo(LogicalType.string());
    }

    @Test
    void enumResolvesToEnumType() {
        assertThat(resolveColumn(PhysicalType.BYTE_ARRAY, ConvertedType.ENUM).logicalType())
                .isEqualTo(LogicalType.enumType());
    }

    @Test
    void jsonResolvesToJsonType() {
        assertThat(resolveColumn(PhysicalType.BYTE_ARRAY, ConvertedType.JSON).logicalType())
                .isEqualTo(LogicalType.json());
    }

    @Test
    void bsonResolvesToBsonType() {
        assertThat(resolveColumn(PhysicalType.BYTE_ARRAY, ConvertedType.BSON).logicalType())
                .isEqualTo(LogicalType.bson());
    }

    @Test
    void intervalResolvesToIntervalType() {
        assertThat(resolveColumn(PhysicalType.FIXED_LEN_BYTE_ARRAY, ConvertedType.INTERVAL).logicalType())
                .isEqualTo(LogicalType.interval());
    }

    @Test
    void dateResolvesToDateType() {
        assertThat(resolveColumn(PhysicalType.INT32, ConvertedType.DATE).logicalType())
                .isEqualTo(LogicalType.date());
    }

    @Test
    void decimalResolvesToDecimalTypeWithScaleAndPrecision() {
        assertThat(resolveColumn(PhysicalType.INT64, ConvertedType.DECIMAL, 2, 18).logicalType())
                .isEqualTo(LogicalType.decimal(18, 2));
    }

    @Test
    void decimalDefaultsScaleToZeroWhenAbsent() {
        assertThat(resolveColumn(PhysicalType.INT32, ConvertedType.DECIMAL, null, 9).logicalType())
                .isEqualTo(LogicalType.decimal(9, 0));
    }

    @Test
    void decimalWithoutPrecisionIsRejected() {
        assertThatThrownBy(() -> resolveColumn(PhysicalType.INT32, ConvertedType.DECIMAL, 2, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("DECIMAL converted type requires a precision: col");
    }

    @Test
    void timeMillisResolvesToUtcAdjustedMillis() {
        assertThat(resolveColumn(PhysicalType.INT32, ConvertedType.TIME_MILLIS).logicalType())
                .isEqualTo(LogicalType.time(true, LogicalType.TimeUnit.MILLIS));
    }

    @Test
    void timeMicrosResolvesToUtcAdjustedMicros() {
        assertThat(resolveColumn(PhysicalType.INT64, ConvertedType.TIME_MICROS).logicalType())
                .isEqualTo(LogicalType.time(true, LogicalType.TimeUnit.MICROS));
    }

    @Test
    void timestampMillisResolvesToUtcAdjustedMillis() {
        assertThat(resolveColumn(PhysicalType.INT64, ConvertedType.TIMESTAMP_MILLIS).logicalType())
                .isEqualTo(LogicalType.timestamp(true, LogicalType.TimeUnit.MILLIS));
    }

    @Test
    void timestampMicrosResolvesToUtcAdjustedMicros() {
        assertThat(resolveColumn(PhysicalType.INT64, ConvertedType.TIMESTAMP_MICROS).logicalType())
                .isEqualTo(LogicalType.timestamp(true, LogicalType.TimeUnit.MICROS));
    }

    @Test
    void int8ResolvesToSigned8Bit() {
        assertThat(resolveColumn(PhysicalType.INT32, ConvertedType.INT_8).logicalType())
                .isEqualTo(LogicalType.intType(8, true));
    }

    @Test
    void int16ResolvesToSigned16Bit() {
        assertThat(resolveColumn(PhysicalType.INT32, ConvertedType.INT_16).logicalType())
                .isEqualTo(LogicalType.intType(16, true));
    }

    @Test
    void int32ResolvesToSigned32Bit() {
        assertThat(resolveColumn(PhysicalType.INT32, ConvertedType.INT_32).logicalType())
                .isEqualTo(LogicalType.intType(32, true));
    }

    @Test
    void int64ResolvesToSigned64Bit() {
        assertThat(resolveColumn(PhysicalType.INT64, ConvertedType.INT_64).logicalType())
                .isEqualTo(LogicalType.intType(64, true));
    }

    @Test
    void uint8ResolvesToUnsigned8Bit() {
        assertThat(resolveColumn(PhysicalType.INT32, ConvertedType.UINT_8).logicalType())
                .isEqualTo(LogicalType.intType(8, false));
    }

    @Test
    void uint16ResolvesToUnsigned16Bit() {
        assertThat(resolveColumn(PhysicalType.INT32, ConvertedType.UINT_16).logicalType())
                .isEqualTo(LogicalType.intType(16, false));
    }

    @Test
    void uint32ResolvesToUnsigned32Bit() {
        assertThat(resolveColumn(PhysicalType.INT32, ConvertedType.UINT_32).logicalType())
                .isEqualTo(LogicalType.intType(32, false));
    }

    @Test
    void uint64ResolvesToUnsigned64Bit() {
        assertThat(resolveColumn(PhysicalType.INT64, ConvertedType.UINT_64).logicalType())
                .isEqualTo(LogicalType.intType(64, false));
    }

    @Test
    void modernLogicalTypeWinsOverConvertedType() {
        SchemaElement root = SchemaElement.group(ROOT, RepetitionType.REQUIRED, 1);
        // converted_type=UTF8, but logical type is explicitly an Int, logical type must win.
        LogicalType.IntType modern = LogicalType.intType(32, false);
        SchemaElement leaf = new SchemaElement(
                COLUMN, PhysicalType.INT32, null, RepetitionType.OPTIONAL, null,
                ConvertedType.UTF8, null, null, null, modern);
        FileSchema schema = FileSchema.fromSchemaElements(List.of(root, leaf));
        assertThat(schema.getColumn(COLUMN).logicalType()).isEqualTo(modern);
    }
}
