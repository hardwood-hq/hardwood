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
