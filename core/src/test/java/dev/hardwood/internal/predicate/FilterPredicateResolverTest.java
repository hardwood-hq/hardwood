/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FilterPredicateResolverTest {

    // ==================== Date ====================

    @Test
    void resolveDateToInt() {
        FileSchema schema = schemaWithLogicalType("col", PhysicalType.INT32, new LogicalType.DateType());
        LocalDate date = LocalDate.of(2024, 6, 15);
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.gt("col", date), schema);

        assertThat(resolved).isInstanceOf(ResolvedPredicate.IntPredicate.class);
        ResolvedPredicate.IntPredicate ip = (ResolvedPredicate.IntPredicate) resolved;
        assertThat(ip.value()).isEqualTo(Math.toIntExact(date.toEpochDay()));
        assertThat(ip.op()).isEqualTo(FilterPredicate.Operator.GT);
        assertThat(ip.columnIndex()).isEqualTo(0);
    }

    @Test
    void resolveDateEpoch() {
        FileSchema schema = schemaWithLogicalType("col", PhysicalType.INT32, new LogicalType.DateType());
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.eq("col", LocalDate.of(1970, 1, 1)), schema);

        assertThat(((ResolvedPredicate.IntPredicate) resolved).value()).isEqualTo(0);
    }

    // ==================== Instant ====================

    @Test
    void resolveInstantMillisToLong() {
        FileSchema schema = schemaWithLogicalType("ts", PhysicalType.INT64,
                new LogicalType.TimestampType(true, LogicalType.TimeUnit.MILLIS));
        Instant instant = Instant.parse("2024-06-15T12:30:00Z");
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.eq("ts", instant), schema);

        assertThat(resolved).isInstanceOf(ResolvedPredicate.LongPredicate.class);
        assertThat(((ResolvedPredicate.LongPredicate) resolved).value())
                .isEqualTo(instant.toEpochMilli());
    }

    @Test
    void resolveInstantMicrosToLong() {
        FileSchema schema = schemaWithLogicalType("ts", PhysicalType.INT64,
                new LogicalType.TimestampType(true, LogicalType.TimeUnit.MICROS));
        Instant instant = Instant.parse("2024-06-15T12:30:00.123456Z");
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.gt("ts", instant), schema);
        long expected = Math.addExact(
                Math.multiplyExact(instant.getEpochSecond(), 1_000_000L),
                instant.getNano() / 1_000L);

        assertThat(((ResolvedPredicate.LongPredicate) resolved).value()).isEqualTo(expected);
    }

    @Test
    void resolveInstantNanosToLong() {
        FileSchema schema = schemaWithLogicalType("ts", PhysicalType.INT64,
                new LogicalType.TimestampType(true, LogicalType.TimeUnit.NANOS));
        Instant instant = Instant.parse("2024-06-15T12:30:00.123456789Z");
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.eq("ts", instant), schema);
        long expected = Math.addExact(
                Math.multiplyExact(instant.getEpochSecond(), 1_000_000_000L),
                instant.getNano());
        assertThat(((ResolvedPredicate.LongPredicate) resolved).value()).isEqualTo(expected);
    }

    @Test
    void resolveInstantOnNonTimestampColumnThrows() {
        FileSchema schema = schemaWithLogicalType("col", PhysicalType.INT64, new LogicalType.DateType());
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.eq("col", Instant.now()), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("TIMESTAMP");
    }

    // ==================== LocalTime ====================

    @Test
    void resolveTimeMillisToInt() {
        FileSchema schema = schemaWithLogicalType("t", PhysicalType.INT32,
                new LogicalType.TimeType(false, LogicalType.TimeUnit.MILLIS));
        LocalTime time = LocalTime.of(12, 30, 45);
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.lt("t", time), schema);
        assertThat(resolved).isInstanceOf(ResolvedPredicate.IntPredicate.class);
        assertThat(((ResolvedPredicate.IntPredicate) resolved).value())
                .isEqualTo(Math.toIntExact(time.toNanoOfDay() / 1_000_000L));
    }

    @Test
    void resolveTimeMicrosToLong() {
        FileSchema schema = schemaWithLogicalType("t", PhysicalType.INT64,
                new LogicalType.TimeType(false, LogicalType.TimeUnit.MICROS));
        LocalTime time = LocalTime.of(12, 30, 45, 123_456_000);
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.eq("t", time), schema);
        assertThat(resolved).isInstanceOf(ResolvedPredicate.LongPredicate.class);
        assertThat(((ResolvedPredicate.LongPredicate) resolved).value())
                .isEqualTo(time.toNanoOfDay() / 1_000L);
    }

    @Test
    void resolveTimeOnNonTimeColumnThrows() {
        FileSchema schema = schemaWithLogicalType("col", PhysicalType.INT32, new LogicalType.DateType());
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.eq("col", LocalTime.NOON), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("TIME");
    }

    // ==================== Decimal ====================

    @Test
    void resolveDecimalInt32() {
        FileSchema schema = schemaWithLogicalType("amount", PhysicalType.INT32,
                new LogicalType.DecimalType(2, 9));
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.gt("amount", new BigDecimal("99.99")), schema);
        assertThat(resolved).isInstanceOf(ResolvedPredicate.IntPredicate.class);
        // 99.99 with scale 2 → unscaled 9999
        assertThat(((ResolvedPredicate.IntPredicate) resolved).value()).isEqualTo(9999);
    }

    @Test
    void resolveDecimalInt64() {
        FileSchema schema = schemaWithLogicalType("amount", PhysicalType.INT64,
                new LogicalType.DecimalType(4, 18));
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.eq("amount", new BigDecimal("123.4567")), schema);
        assertThat(resolved).isInstanceOf(ResolvedPredicate.LongPredicate.class);
        assertThat(((ResolvedPredicate.LongPredicate) resolved).value()).isEqualTo(1234567L);
    }

    @Test
    void resolveDecimalFixedLenByteArray() {
        FileSchema schema = schemaWithLogicalType("amount", PhysicalType.FIXED_LEN_BYTE_ARRAY, 16,
                new LogicalType.DecimalType(2, 30));
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.eq("amount", new BigDecimal("1.00")), schema);
        assertThat(resolved).isInstanceOf(ResolvedPredicate.BinaryPredicate.class);
        ResolvedPredicate.BinaryPredicate bp = (ResolvedPredicate.BinaryPredicate) resolved;
        assertThat(bp.signed()).isTrue();
        // 1.00 with scale 2 → unscaled 100 → padded to 16 bytes
        byte[] expected = FilterPredicateResolver.toFixedLenDecimalBytes(
                new BigDecimal("1.00").setScale(2).unscaledValue(), 16);
        assertThat(bp.value()).isEqualTo(expected);
    }

    @Test
    void resolveDecimalOnNonDecimalColumnThrows() {
        FileSchema schema = schemaWithLogicalType("col", PhysicalType.INT32, new LogicalType.DateType());
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.eq("col", new BigDecimal("1.0")), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("DECIMAL");
    }

    @Test
    void resolveNegativeDecimalInt32() {
        FileSchema schema = schemaWithLogicalType("amount", PhysicalType.INT32,
                new LogicalType.DecimalType(2, 9));
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.gt("amount", new BigDecimal("-99.99")), schema);
        assertThat(resolved).isInstanceOf(ResolvedPredicate.IntPredicate.class);
        // -99.99 with scale 2 → unscaled -9999
        assertThat(((ResolvedPredicate.IntPredicate) resolved).value()).isEqualTo(-9999);
    }

    @Test
    void resolveSignedBinaryOnDecimalColumn() {
        FileSchema schema = schemaWithLogicalType("amount", PhysicalType.FIXED_LEN_BYTE_ARRAY, 8,
                new LogicalType.DecimalType(2, 18));
        byte[] value = new byte[8];
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                new FilterPredicate.SignedBinaryColumnPredicate("amount", FilterPredicate.Operator.GT, value),
                schema);
        assertThat(resolved).isInstanceOf(ResolvedPredicate.BinaryPredicate.class);
        assertThat(((ResolvedPredicate.BinaryPredicate) resolved).signed()).isTrue();
    }

    @Test
    void resolveSignedBinaryOnNonDecimalColumnThrows() {
        FileSchema schema = schemaWithLogicalType("data", PhysicalType.FIXED_LEN_BYTE_ARRAY, 8, null);
        byte[] value = new byte[8];
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                new FilterPredicate.SignedBinaryColumnPredicate("data", FilterPredicate.Operator.EQ, value),
                schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("DecimalType");
    }

    @Test
    void resolveNegativeDecimalFixedLenByteArray() {
        FileSchema schema = schemaWithLogicalType("amount", PhysicalType.FIXED_LEN_BYTE_ARRAY, 8,
                new LogicalType.DecimalType(2, 18));
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.eq("amount", new BigDecimal("-1.50")), schema);
        assertThat(resolved).isInstanceOf(ResolvedPredicate.BinaryPredicate.class);
        ResolvedPredicate.BinaryPredicate bp = (ResolvedPredicate.BinaryPredicate) resolved;
        assertThat(bp.signed()).isTrue();
        // -1.50 with scale 2 → unscaled -150 → sign-extended to 8 bytes
        byte[] expected = FilterPredicateResolver.toFixedLenDecimalBytes(
                new BigDecimal("-1.50").setScale(2).unscaledValue(), 8);
        assertThat(bp.value()).isEqualTo(expected);
        // First byte should be 0xFF (negative sign extension)
        assertThat(bp.value()[0]).isEqualTo((byte) 0xFF);
    }

    // ==================== UUID ====================

    @Test
    void resolveUuidToUnsignedBinary() {
        FileSchema schema = schemaWithLogicalType("id", PhysicalType.FIXED_LEN_BYTE_ARRAY, 16,
                new LogicalType.UuidType());
        UUID uuid = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.eq("id", uuid), schema);

        assertThat(resolved).isInstanceOf(ResolvedPredicate.BinaryPredicate.class);
        ResolvedPredicate.BinaryPredicate bp = (ResolvedPredicate.BinaryPredicate) resolved;
        assertThat(bp.signed()).isFalse();
        assertThat(bp.op()).isEqualTo(FilterPredicate.Operator.EQ);
        assertThat(bp.columnIndex()).isEqualTo(0);
    }

    @Test
    void resolveUuidOnNonUuidColumnThrows() {
        FileSchema schema = schemaWithLogicalType("id", PhysicalType.FIXED_LEN_BYTE_ARRAY, 16, null);
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.eq("id", UUID.randomUUID()), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("UuidType");
    }

    // ==================== Combinators ====================

    @Test
    void resolveRecursesIntoAnd() {
        FileSchema schema = schemaWithLogicalType("col", PhysicalType.INT32, new LogicalType.DateType());
        LocalDate d1 = LocalDate.of(2024, 1, 1);
        LocalDate d2 = LocalDate.of(2024, 12, 31);
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.and(FilterPredicate.gtEq("col", d1), FilterPredicate.lt("col", d2)),
                schema);
        assertThat(resolved).isInstanceOf(ResolvedPredicate.And.class);
        ResolvedPredicate.And and = (ResolvedPredicate.And) resolved;
        assertThat(and.children()).hasSize(2);
        assertThat(and.children().get(0)).isInstanceOf(ResolvedPredicate.IntPredicate.class);
        assertThat(and.children().get(1)).isInstanceOf(ResolvedPredicate.IntPredicate.class);
    }

    @Test
    void resolvePhysicalPredicateReturnsResolvedType() {
        FileSchema schema = schemaWithLogicalType("col", PhysicalType.INT32, null);
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.eq("col", 42), schema);
        assertThat(resolved).isInstanceOf(ResolvedPredicate.IntPredicate.class);
        ResolvedPredicate.IntPredicate ip = (ResolvedPredicate.IntPredicate) resolved;
        assertThat(ip.value()).isEqualTo(42);
        assertThat(ip.columnIndex()).isEqualTo(0);
    }

    // ==================== Column resolution ====================

    @Test
    void resolveColumnIndex() {
        FileSchema schema = schemaWithLogicalType("col", PhysicalType.INT32, null);
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.eq("col", 42), schema);
        assertThat(((ResolvedPredicate.IntPredicate) resolved).columnIndex()).isEqualTo(0);
    }

    @Test
    void resolveUnknownColumnThrows() {
        FileSchema schema = schemaWithLogicalType("col", PhysicalType.INT32, null);
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.eq("nonexistent", 42), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not found");
    }

    // ==================== Type validation ====================

    @Test
    void resolveTypeMismatchThrows() {
        FileSchema schema = schemaWithLogicalType("col", PhysicalType.BYTE_ARRAY, null);
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.eq("col", 42), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("incompatible");
    }

    // ==================== IS NULL / IS NOT NULL ====================

    @Test
    void resolveIsNullToColumnIndex() {
        FileSchema schema = schemaWithLogicalType("col", PhysicalType.INT32, null);
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.isNull("col"), schema);

        assertThat(resolved).isInstanceOf(ResolvedPredicate.IsNullPredicate.class);
        assertThat(((ResolvedPredicate.IsNullPredicate) resolved).columnIndex()).isEqualTo(0);
    }

    @Test
    void resolveIsNotNullToColumnIndex() {
        FileSchema schema = schemaWithLogicalType("col", PhysicalType.INT64, null);
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.isNotNull("col"), schema);

        assertThat(resolved).isInstanceOf(ResolvedPredicate.IsNotNullPredicate.class);
        assertThat(((ResolvedPredicate.IsNotNullPredicate) resolved).columnIndex()).isEqualTo(0);
    }

    @Test
    void resolveIsNullWorksOnAnyPhysicalType() {
        // IS NULL / IS NOT NULL should resolve without type validation errors on any column type
        for (PhysicalType type : new PhysicalType[] {
                PhysicalType.INT32, PhysicalType.INT64, PhysicalType.FLOAT,
                PhysicalType.DOUBLE, PhysicalType.BOOLEAN, PhysicalType.BYTE_ARRAY }) {
            FileSchema schema = schemaWithLogicalType("col", type, null);
            ResolvedPredicate isNull = FilterPredicateResolver.resolve(
                    FilterPredicate.isNull("col"), schema);
            ResolvedPredicate isNotNull = FilterPredicateResolver.resolve(
                    FilterPredicate.isNotNull("col"), schema);

            assertThat(isNull).isInstanceOf(ResolvedPredicate.IsNullPredicate.class);
            assertThat(isNotNull).isInstanceOf(ResolvedPredicate.IsNotNullPredicate.class);
        }
    }

    @Test
    void resolveIsNullOnUnknownColumnThrows() {
        FileSchema schema = schemaWithLogicalType("col", PhysicalType.INT32, null);
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.isNull("nonexistent"), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not found");
    }

    // ==================== Geospatial ====================

    @Test
    void resolveIntersectsOnGeometryColumn() {
        FileSchema schema = schemaWithLogicalType("loc", PhysicalType.BYTE_ARRAY,
                new LogicalType.GeometryType("OGC:CRS84"));
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.intersects("loc", -25.0, 35.0, 45.0, 72.0), schema);

        assertThat(resolved).isInstanceOf(ResolvedPredicate.GeospatialPredicate.class);
        ResolvedPredicate.GeospatialPredicate gp = (ResolvedPredicate.GeospatialPredicate) resolved;
        assertThat(gp.columnIndex()).isEqualTo(0);
        assertThat(gp.xmin()).isEqualTo(-25.0);
        assertThat(gp.ymin()).isEqualTo(35.0);
        assertThat(gp.xmax()).isEqualTo(45.0);
        assertThat(gp.ymax()).isEqualTo(72.0);
    }

    @Test
    void resolveIntersectsOnGeographyColumn() {
        FileSchema schema = schemaWithLogicalType("loc", PhysicalType.BYTE_ARRAY,
                new LogicalType.GeographyType("OGC:CRS84", LogicalType.EdgeInterpolationAlgorithm.SPHERICAL));
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.intersects("loc", 0.0, 0.0, 1.0, 1.0), schema);

        assertThat(resolved).isInstanceOf(ResolvedPredicate.GeospatialPredicate.class);
    }

    @Test
    void resolveIntersectsOnNonGeoColumnThrows() {
        FileSchema schema = schemaWithLogicalType("col", PhysicalType.BYTE_ARRAY, new LogicalType.StringType());
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.intersects("col", 0.0, 0.0, 1.0, 1.0), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("GEOMETRY")
                .hasMessageContaining("GEOGRAPHY");
    }

    @Test
    void resolveNotIntersectsThrows() {
        FileSchema schema = schemaWithLogicalType("loc", PhysicalType.BYTE_ARRAY,
                new LogicalType.GeometryType("OGC:CRS84"));
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.not(FilterPredicate.intersects("loc", 0.0, 0.0, 1.0, 1.0)), schema))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Negation");
    }

    // ==================== Helpers ====================

    private static FileSchema schemaWithLogicalType(String columnName, PhysicalType type,
            LogicalType logicalType) {
        return schemaWithLogicalType(columnName, type, null, logicalType);
    }

    private static FileSchema schemaWithLogicalType(String columnName, PhysicalType type,
            Integer typeLength, LogicalType logicalType) {
        SchemaElement root = new SchemaElement("root", null, null, null, 1, null, null, null, null, null);
        SchemaElement col = new SchemaElement(columnName, type, typeLength, RepetitionType.REQUIRED,
                null, null, null, null, null, logicalType);
        return FileSchema.fromSchemaElements(List.of(root, col));
    }
}
