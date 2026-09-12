/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.internal.predicate.ResolvedPredicate.BinaryPredicate.Comparison;
import dev.hardwood.metadata.ColumnOrder;
import dev.hardwood.metadata.ConvertedType;
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
        FileSchema schema = schemaWithLogicalType("col", PhysicalType.INT32, LogicalType.date());
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
        FileSchema schema = schemaWithLogicalType("col", PhysicalType.INT32, LogicalType.date());
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.eq("col", LocalDate.of(1970, 1, 1)), schema);

        assertThat(((ResolvedPredicate.IntPredicate) resolved).value()).isEqualTo(0);
    }

    @Test
    void resolveDateOnUnannotatedIntColumnThrows() {
        FileSchema schema = schemaWithLogicalType("col", PhysicalType.INT32, null);
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.eq("col", LocalDate.of(2024, 6, 15)), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'col' is not a DateType column (logical type: null)");
    }

    @Test
    void resolveDateOnDifferentlyAnnotatedIntColumnThrows() {
        FileSchema schema = schemaWithLogicalType("col", PhysicalType.INT32,
                new LogicalType.IntType(32, true));
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.gt("col", LocalDate.of(2024, 6, 15)), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'col' is not a DateType column (logical type: INT_32)");
    }

    // ==================== Instant ====================

    @Test
    void resolveInstantMillisToLong() {
        FileSchema schema = schemaWithLogicalType("ts", PhysicalType.INT64,
                LogicalType.timestamp(true, LogicalType.TimeUnit.MILLIS));
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
                LogicalType.timestamp(true, LogicalType.TimeUnit.MICROS));
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
                LogicalType.timestamp(true, LogicalType.TimeUnit.NANOS));
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
        FileSchema schema = schemaWithLogicalType("col", PhysicalType.INT64, LogicalType.date());
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.eq("col", Instant.now()), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'col' does not have a TIMESTAMP logical type");
    }

    // ==================== LocalTime ====================

    @Test
    void resolveTimeMillisToInt() {
        FileSchema schema = schemaWithLogicalType("t", PhysicalType.INT32,
                LogicalType.time(false, LogicalType.TimeUnit.MILLIS));
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
                LogicalType.time(false, LogicalType.TimeUnit.MICROS));
        LocalTime time = LocalTime.of(12, 30, 45, 123_456_000);
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.eq("t", time), schema);
        assertThat(resolved).isInstanceOf(ResolvedPredicate.LongPredicate.class);
        assertThat(((ResolvedPredicate.LongPredicate) resolved).value())
                .isEqualTo(time.toNanoOfDay() / 1_000L);
    }

    @Test
    void resolveTimeOnNonTimeColumnThrows() {
        FileSchema schema = schemaWithLogicalType("col", PhysicalType.INT32, LogicalType.date());
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.eq("col", LocalTime.NOON), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'col' does not have a TIME logical type");
    }

    /// `TIME(MILLIS)` is stored in an `INT32`, so on an `INT64` column the annotation is
    /// dropped and the column filters as the `INT64` it is: by `long`, not by `LocalTime`.
    @Test
    void resolveTimeOnATimeMillisAnnotationOverInt64Throws() {
        FileSchema schema = schemaWithLogicalType("t", PhysicalType.INT64,
                LogicalType.time(true, LogicalType.TimeUnit.MILLIS));
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.eq("t", LocalTime.NOON), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 't' does not have a TIME logical type");
        assertThat(FilterPredicateResolver.resolve(FilterPredicate.eq("t", 7L), schema))
                .isInstanceOf(ResolvedPredicate.LongPredicate.class);
    }

    /// An `INT32` holds nine digits, so a `DECIMAL(12, 2)` annotation on one is dropped and
    /// the column filters as a plain `INT32`.
    @Test
    void resolveDecimalOnADecimalAnnotationBeyondItsCarrierThrows() {
        FileSchema schema = schemaWithLogicalType("amount", PhysicalType.INT32,
                LogicalType.decimal(12, 2));
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.eq("amount", new BigDecimal("1.50")), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'amount' does not have a DECIMAL logical type");
        assertThat(FilterPredicateResolver.resolve(FilterPredicate.eq("amount", 150), schema))
                .isInstanceOf(ResolvedPredicate.IntPredicate.class);
    }

    // ==================== Decimal ====================

    /// A literal is rescaled to the column's scale, so a column holding more scale than the
    /// literal pads it rather than refusing it.
    @Test
    void resolveDecimalPadsALiteralCoarserThanTheColumn() {
        FileSchema schema = schemaWithLogicalType("amount", PhysicalType.INT32,
                LogicalType.decimal(9, 4));
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.gt("amount", new BigDecimal("99.99")), schema);

        // 99.99 restated at scale 4 is 99.9900, unscaled 999900.
        assertThat(((ResolvedPredicate.IntPredicate) resolved).value()).isEqualTo(999900);
    }

    /// Trailing zeros carry no value, so dropping them to reach the column's scale is not
    /// rounding and is allowed.
    @Test
    void resolveDecimalDropsTrailingZerosToReachTheColumnScale() {
        FileSchema schema = schemaWithLogicalType("amount", PhysicalType.INT32,
                LogicalType.decimal(9, 2));
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.gt("amount", new BigDecimal("99.9900")), schema);

        assertThat(((ResolvedPredicate.IntPredicate) resolved).value()).isEqualTo(9999);
    }

    /// A digit the column cannot hold is refused for equality rather than rounded away, so a
    /// predicate never silently answers for a value other than the one asked about.
    @Test
    void resolveDecimalRefusesAnEqualityLiteralFinerThanTheColumn() {
        FileSchema schema = schemaWithLogicalType("amount", PhysicalType.INT32,
                LogicalType.decimal(9, 2));

        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.eq("amount", new BigDecimal("99.999")), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'amount' holds a DECIMAL of scale 2 within the INT32 range; "
                        + "the equality literal 99.999 is not a value it can hold");

        // `notEq` asks the same question of the literal and is refused the same way: it would
        // otherwise hold on every row, for a value the caller cannot have meant.
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.notEq("amount", new BigDecimal("99.999")), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'amount' holds a DECIMAL of scale 2 within the INT32 range; "
                        + "the equality literal 99.999 is not a value it can hold");
    }

    /// An order has an answer for such a literal: the bound moves to the nearest value the column
    /// holds on the side the operator admits, which is exact for every value it can store.
    @Test
    void resolveDecimalMovesAnOrderBoundToTheNearestValueTheColumnHolds() {
        FileSchema schema = schemaWithLogicalType("amount", PhysicalType.INT32,
                LogicalType.decimal(9, 2));

        assertThat(FilterPredicateResolver.resolve(
                FilterPredicate.gt("amount", new BigDecimal("99.999")), schema))
                .isEqualTo(new ResolvedPredicate.IntPredicate(0, FilterPredicate.Operator.GT_EQ, 10_000));
        assertThat(FilterPredicateResolver.resolve(
                FilterPredicate.lt("amount", new BigDecimal("99.999")), schema))
                .isEqualTo(new ResolvedPredicate.IntPredicate(0, FilterPredicate.Operator.LT_EQ, 9_999));
    }

    /// Past the carrier's range there is no such bound, and the predicate matches every non-null
    /// row or none.
    @Test
    void resolveDecimalPastTheCarrierRangeResolvesToAConstant() {
        FileSchema schema = schemaWithLogicalType("amount", PhysicalType.INT32,
                LogicalType.decimal(9, 2));

        assertThat(FilterPredicateResolver.resolve(
                FilterPredicate.gt("amount", new BigDecimal("99999999999.00")), schema))
                .isEqualTo(new ResolvedPredicate.NoRowPredicate(0));
        assertThat(FilterPredicateResolver.resolve(
                FilterPredicate.not(FilterPredicate.gt("amount", new BigDecimal("99999999999.00"))), schema))
                .isEqualTo(new ResolvedPredicate.EveryNonNullRowPredicate(0));
    }

    @Test
    void resolveDecimalInt32() {
        FileSchema schema = schemaWithLogicalType("amount", PhysicalType.INT32,
                LogicalType.decimal(9, 2));
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.gt("amount", new BigDecimal("99.99")), schema);
        assertThat(resolved).isInstanceOf(ResolvedPredicate.IntPredicate.class);
        // 99.99 with scale 2 → unscaled 9999
        assertThat(((ResolvedPredicate.IntPredicate) resolved).value()).isEqualTo(9999);
    }

    @Test
    void resolveDecimalInt64() {
        FileSchema schema = schemaWithLogicalType("amount", PhysicalType.INT64,
                LogicalType.decimal(18, 4));
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.eq("amount", new BigDecimal("123.4567")), schema);
        assertThat(resolved).isInstanceOf(ResolvedPredicate.LongPredicate.class);
        assertThat(((ResolvedPredicate.LongPredicate) resolved).value()).isEqualTo(1234567L);
    }

    @Test
    void resolveDecimalFixedLenByteArray() {
        FileSchema schema = schemaWithLogicalType("amount", PhysicalType.FIXED_LEN_BYTE_ARRAY, 16,
                LogicalType.decimal(30, 2));
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
        FileSchema schema = schemaWithLogicalType("col", PhysicalType.INT32, LogicalType.date());
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.eq("col", new BigDecimal("1.0")), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'col' does not have a DECIMAL logical type");
    }

    @Test
    void resolveNegativeDecimalInt32() {
        FileSchema schema = schemaWithLogicalType("amount", PhysicalType.INT32,
                LogicalType.decimal(9, 2));
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.gt("amount", new BigDecimal("-99.99")), schema);
        assertThat(resolved).isInstanceOf(ResolvedPredicate.IntPredicate.class);
        // -99.99 with scale 2 → unscaled -9999
        assertThat(((ResolvedPredicate.IntPredicate) resolved).value()).isEqualTo(-9999);
    }

    @Test
    void resolveByteLiteralOnFixedLenDecimalComparesSigned() {
        FileSchema schema = schemaWithLogicalType("amount", PhysicalType.FIXED_LEN_BYTE_ARRAY, 8,
                LogicalType.decimal(18, 2));
        byte[] value = new byte[8];
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                new FilterPredicate.BinaryColumnPredicate("amount", FilterPredicate.Operator.GT, value),
                schema);
        assertThat(resolved).isInstanceOf(ResolvedPredicate.BinaryPredicate.class);
        assertThat(((ResolvedPredicate.BinaryPredicate) resolved).signed()).isTrue();
    }

    @Test
    void resolveNegativeDecimalFixedLenByteArray() {
        FileSchema schema = schemaWithLogicalType("amount", PhysicalType.FIXED_LEN_BYTE_ARRAY, 8,
                LogicalType.decimal(18, 2));
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

    @Test
    void resolveDecimalTooWideForFixedLenColumnThrows() {
        FileSchema schema = schemaWithLogicalType("amount", PhysicalType.FIXED_LEN_BYTE_ARRAY, 4,
                LogicalType.decimal(9, 2));
        // 30000000.00 is unscaled 3_000_000_000, which needs five bytes in two's complement
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.eq("amount", new BigDecimal("30000000.00")), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'amount' holds a DECIMAL of scale 2 within 4 bytes; "
                        + "the equality literal 30000000.00 is not a value it can hold");
    }

    // ==================== Byte literal on a fixed-width DECIMAL ====================

    /// A fixed-width `DECIMAL` holds each number in one encoding, sign-extended to the column
    /// width, and the byte-exact shortcuts probe for exactly that encoding. A byte literal of any
    /// length resolves to it.
    @Test
    void resolveNarrowByteLiteralOnFixedLenDecimalSignExtendsToWidth() {
        assertThat(resolveFixedDecimalLiteral(0x7D)).containsExactly(0x00, 0x00, 0x00, 0x7D);
        assertThat(resolveFixedDecimalLiteral(0x83)).containsExactly(0xFF, 0xFF, 0xFF, 0x83);
    }

    @Test
    void resolveWideByteLiteralOnFixedLenDecimalDropsSignExtension() {
        assertThat(resolveFixedDecimalLiteral(0x00, 0x00, 0x00, 0x00, 0x7D)).containsExactly(0x00, 0x00, 0x00, 0x7D);
        assertThat(resolveFixedDecimalLiteral(0xFF, 0xFF, 0xFF, 0xFF, 0x83)).containsExactly(0xFF, 0xFF, 0xFF, 0x83);
    }

    /// `BinaryComparator.compareSigned` reads an empty array as zero.
    @Test
    void resolveEmptyByteLiteralOnFixedLenDecimalAsZero() {
        assertThat(resolveFixedDecimalLiteral()).containsExactly(0x00, 0x00, 0x00, 0x00);
    }

    @Test
    void resolveByteLiteralTooWideForFixedLenDecimalThrows() {
        assertThatThrownBy(() -> resolveFixedDecimalLiteral(0x01, 0x00, 0x00, 0x00, 0x00))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'amount' holds a DECIMAL within 4 bytes; "
                        + "the equality literal 0100000000 is not a value it can hold");
    }

    @Test
    void resolveByteLiteralProbesOnFixedLenDecimalSignExtendToWidth() {
        FileSchema schema = schemaWithLogicalType("amount", PhysicalType.FIXED_LEN_BYTE_ARRAY, 4,
                LogicalType.decimal(9, 2));
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                new FilterPredicate.BinaryInPredicate("amount", new byte[][]{ byteLiteral(0x7D), byteLiteral(0x83) }),
                schema);
        assertThat(resolved).isInstanceOfSatisfying(ResolvedPredicate.BinaryInPredicate.class, p -> {
            assertThat(p.byteExact()).isTrue();
            assertThat(p.values()[0]).containsExactly(0x00, 0x00, 0x00, 0x7D);
            assertThat(p.values()[1]).containsExactly(0xFF, 0xFF, 0xFF, 0x83);
        });
    }

    /// Every probe of a membership test is an equality literal, so the column has to hold each
    /// one; a probe whose value needs more bytes than the column has is refused as a scalar
    /// literal of the same value would be.
    @Test
    void resolveByteLiteralProbeTooWideForFixedLenDecimalThrows() {
        FileSchema schema = schemaWithLogicalType("amount", PhysicalType.FIXED_LEN_BYTE_ARRAY, 4,
                LogicalType.decimal(9, 2));

        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                new FilterPredicate.BinaryInPredicate("amount",
                        new byte[][]{ byteLiteral(0x7D), byteLiteral(0x01, 0x00, 0x00, 0x00, 0x00) }),
                schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'amount' holds a DECIMAL within 4 bytes; "
                        + "the equality literal 0100000000 is not a value it can hold");
    }

    /// The same for a fixed-width column that compares as a byte string, where the width itself
    /// is what the column holds rather than the value the bytes encode.
    @Test
    void resolveByteLiteralProbeOfAnotherWidthOnUnannotatedFixedLenColumnThrows() {
        FileSchema schema = schemaWithLogicalType("code", PhysicalType.FIXED_LEN_BYTE_ARRAY, 4, null);

        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.in("code", byteLiteral(0x61, 0x61, 0x61, 0x61), byteLiteral(0x61, 0x61)), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'code' holds a byte string of 4 bytes; "
                        + "the equality literal 6161 (2 bytes) is not a value it can hold");
    }

    /// An unannotated fixed-width column compares its bytes as a byte string, where a shorter
    /// literal is a different value, not a padded one. It holds only byte strings of its width,
    /// so equality against another width is refused while an order compares as given.
    @Test
    void resolveByteLiteralOfAnotherWidthOnUnannotatedFixedLenColumn() {
        FileSchema schema = schemaWithLogicalType("code", PhysicalType.FIXED_LEN_BYTE_ARRAY, 4, null);

        assertThatThrownBy(() -> FilterPredicateResolver.resolve(FilterPredicate.eq("code", byteLiteral(0x61, 0x61)), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'code' holds a byte string of 4 bytes; "
                        + "the equality literal 6161 (2 bytes) is not a value it can hold");

        assertThat(FilterPredicateResolver.resolve(FilterPredicate.lt("code", byteLiteral(0x61, 0x61)), schema))
                .isInstanceOfSatisfying(ResolvedPredicate.BinaryPredicate.class,
                        p -> assertThat(p.value()).containsExactly('a', 'a'));
    }

    /// Resolves `eq` for `literal` against a `DECIMAL(9, 2)` in a `FIXED_LEN_BYTE_ARRAY(4)` and
    /// returns the bytes the predicate compares.
    private static byte[] resolveFixedDecimalLiteral(int... literal) {
        FileSchema schema = schemaWithLogicalType("amount", PhysicalType.FIXED_LEN_BYTE_ARRAY, 4,
                LogicalType.decimal(9, 2));
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                new FilterPredicate.BinaryColumnPredicate("amount", FilterPredicate.Operator.EQ, byteLiteral(literal)),
                schema);
        assertThat(resolved).isInstanceOf(ResolvedPredicate.BinaryPredicate.class);
        ResolvedPredicate.BinaryPredicate predicate = (ResolvedPredicate.BinaryPredicate) resolved;
        assertThat(predicate.byteExact()).isTrue();
        return predicate.value();
    }

    private static byte[] byteLiteral(int... values) {
        byte[] bytes = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            bytes[i] = (byte) values[i];
        }
        return bytes;
    }

    // ==================== UUID ====================

    @Test
    void resolveUuidToUnsignedBinary() {
        FileSchema schema = schemaWithLogicalType("id", PhysicalType.FIXED_LEN_BYTE_ARRAY, 16,
                LogicalType.uuid());
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
                .hasMessage("Column 'id' is not a UuidType column (logical type: null)");
    }

    // ==================== Combinators ====================

    @Test
    void resolveRecursesIntoAnd() {
        FileSchema schema = schemaWithLogicalType("col", PhysicalType.INT32, LogicalType.date());
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
                .hasMessage("Column 'nonexistent' not found in schema");
    }

    @Test
    void resolveNestedGroupNullPredicate() {
        FileSchema schema = FileSchema.builder("root")
                .struct("company", RepetitionType.OPTIONAL, company -> company
                        .struct("address", RepetitionType.OPTIONAL, address -> address
                                .addColumn("street", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL)))
                .build();

        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.isNull("company.address"), schema);

        assertThat(resolved)
                .isEqualTo(new ResolvedPredicate.IsNullPredicate(0, 2, 3));
    }

    @Test
    void resolveGroupWithRepeatedChildIsReportedAsGroup() {
        // `address` is OPTIONAL and only its child is REPEATED, so a rejection may not claim the
        // column itself is repeated. A comparison predicate is turned away for being a group; a
        // null predicate is answered from the repeated leaf below it, whose definition levels
        // still record whether `address` is there.
        SchemaElement root = new SchemaElement("root", null, null, null, 1, null, null, null, null, null);
        SchemaElement address = new SchemaElement("address", null, null, RepetitionType.OPTIONAL, 1,
                null, null, null, null, null);
        SchemaElement tags = new SchemaElement("tags", PhysicalType.INT32, null, RepetitionType.REPEATED,
                null, null, null, null, null, null);
        FileSchema schema = FileSchema.fromSchemaElements(List.of(root, address, tags));

        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.eq("address", 1), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Filter predicates require a leaf column. Column 'address' is a group.");

        assertThat(FilterPredicateResolver.resolve(FilterPredicate.isNull("address"), schema))
                .isEqualTo(new ResolvedPredicate.IsNullPredicate(0, 1, 2));
    }

    @Test
    void resolveNullPredicateOnAListIsAnsweredFromItsElement() {
        // `optional group tags (LIST) { repeated group list { optional int32 element } }`:
        // the LIST group is at definition level 1, its element at 3.
        FileSchema schema = FileSchema.builder("root")
                .list("tags", RepetitionType.OPTIONAL,
                        element -> element.primitive(PhysicalType.INT32, RepetitionType.OPTIONAL))
                .build();

        assertThat(FilterPredicateResolver.resolve(FilterPredicate.isNull("tags"), schema))
                .isEqualTo(new ResolvedPredicate.IsNullPredicate(0, 1, 3));
    }

    /// A `LIST` or a `MAP` is answered from a leaf below a repeated node, and every repeated node
    /// adds a definition level, so the collection sits strictly below its leaf in every encoding.
    /// That keeps the predicate on the definition level histogram: the leaf is repeated, and an
    /// absent collection and an empty one both count among its nulls.
    @ParameterizedTest(name = "{0}")
    @MethodSource
    void nullPredicateOnACollectionIsAnsweredAsAGroup(String shape, List<SchemaElement> elements,
            int definitionLevel, int leafDefinitionLevel) {
        FileSchema schema = FileSchema.fromSchemaElements(elements);

        assertThat(FilterPredicateResolver.resolve(FilterPredicate.isNull("c"), schema))
                .isEqualTo(new ResolvedPredicate.IsNullPredicate(0, definitionLevel, leafDefinitionLevel));
        assertThat(FilterPredicateResolver.resolve(FilterPredicate.isNotNull("c"), schema))
                .isEqualTo(new ResolvedPredicate.IsNotNullPredicate(0, definitionLevel, leafDefinitionLevel));
    }

    static Stream<Arguments> nullPredicateOnACollectionIsAnsweredAsAGroup() {
        SchemaElement root = SchemaElement.root("root", 1);
        SchemaElement key = SchemaElement.primitive("key", PhysicalType.BYTE_ARRAY, RepetitionType.REQUIRED);
        SchemaElement value = SchemaElement.primitive("value", PhysicalType.INT32, RepetitionType.OPTIONAL);

        return Stream.of(
                Arguments.of("two-level list of a primitive", List.of(root,
                        legacyGroup("c", RepetitionType.OPTIONAL, 1, ConvertedType.LIST),
                        SchemaElement.primitive("element", PhysicalType.INT32, RepetitionType.REPEATED)), 1, 2),
                Arguments.of("two-level list, element group of several fields", List.of(root,
                        legacyGroup("c", RepetitionType.OPTIONAL, 1, ConvertedType.LIST),
                        SchemaElement.group("element", RepetitionType.REPEATED, 2),
                        SchemaElement.primitive("a", PhysicalType.INT32, RepetitionType.REQUIRED),
                        SchemaElement.primitive("b", PhysicalType.INT32, RepetitionType.REQUIRED)), 1, 2),
                Arguments.of("two-level list of lists", List.of(root,
                        legacyGroup("c", RepetitionType.OPTIONAL, 1, ConvertedType.LIST),
                        SchemaElement.group("element", RepetitionType.REPEATED, 1),
                        SchemaElement.primitive("x", PhysicalType.INT32, RepetitionType.REPEATED)), 1, 3),
                Arguments.of("two-level list, element named array", List.of(root,
                        legacyGroup("c", RepetitionType.OPTIONAL, 1, ConvertedType.LIST),
                        SchemaElement.group("array", RepetitionType.REPEATED, 1),
                        SchemaElement.primitive("x", PhysicalType.INT32, RepetitionType.REQUIRED)), 1, 2),
                Arguments.of("two-level list, element named c_tuple", List.of(root,
                        legacyGroup("c", RepetitionType.OPTIONAL, 1, ConvertedType.LIST),
                        SchemaElement.group("c_tuple", RepetitionType.REPEATED, 1),
                        SchemaElement.primitive("x", PhysicalType.INT32, RepetitionType.REQUIRED)), 1, 2),
                Arguments.of("three-level list under other names", List.of(root,
                        legacyGroup("c", RepetitionType.OPTIONAL, 1, ConvertedType.LIST),
                        SchemaElement.group("bag", RepetitionType.REPEATED, 1),
                        SchemaElement.primitive("item", PhysicalType.INT32, RepetitionType.OPTIONAL)), 1, 3),
                // Never absent, so its IS NULL splits the histogram at level 0: no bucket below.
                Arguments.of("required two-level list", List.of(root,
                        legacyGroup("c", RepetitionType.REQUIRED, 1, ConvertedType.LIST),
                        SchemaElement.primitive("element", PhysicalType.INT32, RepetitionType.REPEATED)), 0, 1),
                Arguments.of("map", List.of(root,
                        legacyGroup("c", RepetitionType.OPTIONAL, 1, ConvertedType.MAP),
                        SchemaElement.group("key_value", RepetitionType.REPEATED, 2), key, value), 1, 2),
                Arguments.of("map whose repeated group is marked MAP_KEY_VALUE", List.of(root,
                        legacyGroup("c", RepetitionType.OPTIONAL, 1, ConvertedType.MAP),
                        legacyGroup("map", RepetitionType.REPEATED, 2, ConvertedType.MAP_KEY_VALUE), key, value),
                        1, 2),
                Arguments.of("map marked MAP_KEY_VALUE itself", List.of(root,
                        legacyGroup("c", RepetitionType.OPTIONAL, 1, ConvertedType.MAP_KEY_VALUE),
                        SchemaElement.group("map", RepetitionType.REPEATED, 2), key, value), 1, 2));
    }

    @Test
    void nullPredicateOnAStructOfRequiredFieldsIsAnsweredAsItsLeaf() {
        // `optional group c { required int32 a; }`: nothing between `c` and `a` adds a definition
        // level, so the two share one and the predicate takes the leaf's path. It may: `a` is not
        // repeated, writes one entry per row, and is null exactly where `c` is absent.
        FileSchema schema = FileSchema.fromSchemaElements(List.of(
                SchemaElement.root("root", 1),
                SchemaElement.group("c", RepetitionType.OPTIONAL, 1),
                SchemaElement.primitive("a", PhysicalType.INT32, RepetitionType.REQUIRED)));

        assertThat(FilterPredicateResolver.resolve(FilterPredicate.isNull("c"), schema))
                .isEqualTo(new ResolvedPredicate.IsNullPredicate(0, 1, 1));
    }

    /// A group annotated only by the legacy `converted_type`, as files written before the logical
    /// type union carry `LIST`, `MAP` and `MAP_KEY_VALUE`.
    private static SchemaElement legacyGroup(String name, RepetitionType repetitionType, int numChildren,
            ConvertedType convertedType) {
        return new SchemaElement(name, null, null, repetitionType, numChildren, convertedType,
                null, null, null, null);
    }

    @Test
    void resolveNestedGroupNotNullPredicate() {
        FileSchema schema = FileSchema.builder("root")
                .struct("company", RepetitionType.OPTIONAL, company -> company
                        .struct("address", RepetitionType.OPTIONAL, address -> address
                                .addColumn(
                                        "street",
                                        PhysicalType.BYTE_ARRAY,
                                        RepetitionType.OPTIONAL)))
                .build();

        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.isNotNull("company.address"), schema);

        assertThat(resolved)
                .isEqualTo(new ResolvedPredicate.IsNotNullPredicate(0, 2, 3));
    }

    @Test
    void resolveRepeatedGroupIsReportedAsRepeated() {
        // A REPEATED group carries no LIST or MAP annotation, so it is caught by its own
        // repetition level rather than by the annotation checks.
        SchemaElement root = new SchemaElement("root", null, null, null, 1, null, null, null, null, null);
        SchemaElement addresses = new SchemaElement("addresses", null, null, RepetitionType.REPEATED, 1,
                null, null, null, null, null);
        SchemaElement city = new SchemaElement("city", PhysicalType.BYTE_ARRAY, null,
                RepetitionType.OPTIONAL, null, null, null, null, null, null);
        FileSchema schema = FileSchema.fromSchemaElements(List.of(root, addresses, city));

        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.isNull("addresses"), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Filter predicates do not support repeated columns. Column 'addresses' is "
                         + "repeated.");
    }

    // ==================== Type validation ====================

    @Test
    void resolveTypeMismatchThrows() {
        FileSchema schema = schemaWithLogicalType("col", PhysicalType.BYTE_ARRAY, null);
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.eq("col", 42), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'col' has physical type BYTE_ARRAY; given filter predicate type INT32 "
                         + "is incompatible");
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
                .hasMessage("Column 'nonexistent' not found in schema");
    }

    // ==================== Geospatial ====================

    @Test
    void resolveIntersectsOnGeometryColumn() {
        FileSchema schema = schemaWithLogicalType("loc", PhysicalType.BYTE_ARRAY,
                LogicalType.geometry("OGC:CRS84"));
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
                LogicalType.geography("OGC:CRS84", LogicalType.EdgeInterpolationAlgorithm.SPHERICAL));
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.intersects("loc", 0.0, 0.0, 1.0, 1.0), schema);

        assertThat(resolved).isInstanceOf(ResolvedPredicate.GeospatialPredicate.class);
    }

    @Test
    void resolveIntersectsOnNonGeoColumnThrows() {
        FileSchema schema = schemaWithLogicalType("col", PhysicalType.BYTE_ARRAY, LogicalType.string());
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.intersects("col", 0.0, 0.0, 1.0, 1.0), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'col' is not a GEOMETRY or GEOGRAPHY column")
                ;
    }

    @Test
    void resolveNotIntersectsThrows() {
        FileSchema schema = schemaWithLogicalType("loc", PhysicalType.BYTE_ARRAY,
                LogicalType.geometry("OGC:CRS84"));
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.not(FilterPredicate.intersects("loc", 0.0, 0.0, 1.0, 1.0)), schema))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Negation of spatial intersects predicate is not supported");
    }

    // ==================== Column order propagation (#595) ====================

    @Test
    void floatTypeDefinedOrderMarksPredicateForWidening() {
        FileSchema schema = schemaWithLogicalType("f", PhysicalType.FLOAT, null);
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.eq("f", 1.0f), schema, List.of(ColumnOrder.TYPE_DEFINED_ORDER));
        assertThat(((ResolvedPredicate.FloatPredicate) resolved).ieee754TotalOrder()).isFalse();
    }

    @Test
    void floatIeee754OrderMarksPredicateExact() {
        FileSchema schema = schemaWithLogicalType("f", PhysicalType.FLOAT, null);
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.eq("f", 1.0f), schema, List.of(ColumnOrder.IEEE754_TOTAL_ORDER));
        assertThat(((ResolvedPredicate.FloatPredicate) resolved).ieee754TotalOrder()).isTrue();
    }

    @Test
    void doubleAbsentColumnOrdersDefaultsToTypeDefined() {
        FileSchema schema = schemaWithLogicalType("d", PhysicalType.DOUBLE, null);
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.eq("d", 1.0), schema, List.of());
        assertThat(((ResolvedPredicate.DoublePredicate) resolved).ieee754TotalOrder()).isFalse();
    }

    @Test
    void float16TypeDefinedOrderMarksPredicateForWidening() {
        FileSchema schema = schemaWithLogicalType("h", PhysicalType.FIXED_LEN_BYTE_ARRAY, 2,
                LogicalType.float16());
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.eq("h", 1.0f), schema, List.of(ColumnOrder.TYPE_DEFINED_ORDER));
        assertThat(resolved).isInstanceOf(ResolvedPredicate.Float16Predicate.class);
        assertThat(((ResolvedPredicate.Float16Predicate) resolved).ieee754TotalOrder()).isFalse();
    }

    @Test
    void float16Ieee754OrderMarksPredicateExact() {
        FileSchema schema = schemaWithLogicalType("h", PhysicalType.FIXED_LEN_BYTE_ARRAY, 2,
                LogicalType.float16());
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.eq("h", 1.0f), schema, List.of(ColumnOrder.IEEE754_TOTAL_ORDER));
        assertThat(((ResolvedPredicate.Float16Predicate) resolved).ieee754TotalOrder()).isTrue();
    }

    // ==================== Double IN (#868) ====================

    @Test
    void resolveDoubleInOnDoubleColumn() {
        FileSchema schema = schemaWithLogicalType("d", PhysicalType.DOUBLE, null);
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.in("d", 1.5, 2.5), schema, List.of(ColumnOrder.IEEE754_TOTAL_ORDER));
        assertThat(resolved).isInstanceOf(ResolvedPredicate.DoubleInPredicate.class);
        ResolvedPredicate.DoubleInPredicate dp = (ResolvedPredicate.DoubleInPredicate) resolved;
        assertThat(dp.columnIndex()).isEqualTo(0);
        assertThat(dp.values()).containsExactly(1.5, 2.5);
        assertThat(dp.floatColumn()).isFalse();
        assertThat(dp.ieee754TotalOrder()).isTrue();

        ResolvedPredicate defaultOrder = FilterPredicateResolver.resolve(
                FilterPredicate.in("d", 1.5, 2.5), schema, List.of());
        assertThat(((ResolvedPredicate.DoubleInPredicate) defaultOrder).ieee754TotalOrder()).isFalse();
    }

    @Test
    void resolveDoubleInOnFloatColumn() {
        FileSchema schema = schemaWithLogicalType("f", PhysicalType.FLOAT, null);
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.in("f", 1.5, 2.5), schema, List.of(ColumnOrder.IEEE754_TOTAL_ORDER));
        assertThat(resolved).isInstanceOf(ResolvedPredicate.DoubleInPredicate.class);
        ResolvedPredicate.DoubleInPredicate dp = (ResolvedPredicate.DoubleInPredicate) resolved;
        assertThat(dp.columnIndex()).isEqualTo(0);
        assertThat(dp.values()).containsExactly(1.5, 2.5);
        assertThat(dp.floatColumn()).isTrue();
        assertThat(dp.ieee754TotalOrder()).isTrue();

        ResolvedPredicate defaultOrder = FilterPredicateResolver.resolve(
                FilterPredicate.in("f", 1.5, 2.5), schema, List.of());
        assertThat(((ResolvedPredicate.DoubleInPredicate) defaultOrder).ieee754TotalOrder()).isFalse();
    }

    @Test
    void resolveDoubleInOnFloat16() {
        FileSchema schema = schemaWithLogicalType("h", PhysicalType.FIXED_LEN_BYTE_ARRAY, 2,
                new LogicalType.Float16Type());
        assertThat(FilterPredicateResolver.resolve(FilterPredicate.in("h", 1.5, 2.5), schema))
                .isInstanceOfSatisfying(ResolvedPredicate.Float16InPredicate.class,
                        p -> assertThat(p.values()).containsExactly(1.5, 2.5));
    }

    @Test
    void resolveDoubleInOnIncompatibleColumnThrows() {
        FileSchema intSchema = schemaWithLogicalType("i", PhysicalType.INT32, null);
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.in("i", 1.5, 2.5), intSchema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'i' has physical type INT32; "
                        + "given filter predicate type DOUBLE/FLOAT is incompatible");

        FileSchema stringSchema = schemaWithLogicalType("s", PhysicalType.BYTE_ARRAY, new LogicalType.StringType());
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.in("s", 1.5, 2.5), stringSchema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 's' has physical type BYTE_ARRAY; "
                        + "given filter predicate type DOUBLE/FLOAT is incompatible");

        FileSchema int64Schema = schemaWithLogicalType("l", PhysicalType.INT64, null);
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.in("l", 1.5, 2.5), int64Schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'l' has physical type INT64; "
                        + "given filter predicate type DOUBLE/FLOAT is incompatible");

        FileSchema boolSchema = schemaWithLogicalType("b", PhysicalType.BOOLEAN, null);
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.in("b", 1.5, 2.5), boolSchema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'b' has physical type BOOLEAN; "
                        + "given filter predicate type DOUBLE/FLOAT is incompatible");

        FileSchema flbaSchema = schemaWithLogicalType("flba", PhysicalType.FIXED_LEN_BYTE_ARRAY, 4, null);
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.in("flba", 1.5, 2.5), flbaSchema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'flba' has physical type FIXED_LEN_BYTE_ARRAY; "
                        + "given filter predicate type DOUBLE/FLOAT is incompatible");
    }

    @Test
    void allPredicatesRejectRepeatedColumns() {
        FileSchema schema = schemaWithRepeatedColumn("rep", PhysicalType.INT32, null, null);
        List<FilterPredicate> predicates = List.of(
                FilterPredicate.eq("rep", 1),
                FilterPredicate.eq("rep", 1L),
                FilterPredicate.eq("rep", 1.0f),
                FilterPredicate.eq("rep", 1.0d),
                FilterPredicate.eq("rep", true),
                FilterPredicate.eq("rep", "text"),
                FilterPredicate.eq("rep", LocalDate.of(2026, 1, 1)),
                FilterPredicate.eq("rep", Instant.EPOCH),
                FilterPredicate.eq("rep", LocalTime.NOON),
                FilterPredicate.eq("rep", BigDecimal.ONE),
                FilterPredicate.eq("rep", UUID.randomUUID()),
                FilterPredicate.in("rep", 1, 2),
                FilterPredicate.in("rep", 1L, 2L),
                FilterPredicate.in("rep", 1.0, 2.0),
                FilterPredicate.inStrings("rep", "a", "b"),
                FilterPredicate.isNull("rep"),
                FilterPredicate.isNotNull("rep")
        );

        for (FilterPredicate p : predicates) {
            assertThatThrownBy(() -> FilterPredicateResolver.resolve(p, schema))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Filter predicates do not support repeated columns. "
                            + "Column 'rep' is repeated.");
        }
    }

    @Test
    void typeValidationOnAllPredicates() {
        FileSchema int32Schema = schemaWithLogicalType("c", PhysicalType.INT32, null);
        FileSchema int64Schema = schemaWithLogicalType("c", PhysicalType.INT64, null);

        assertThatThrownBy(() -> FilterPredicateResolver.resolve(FilterPredicate.eq("c", 1L), int32Schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(incompatible("INT32", "INT64"));
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(FilterPredicate.eq("c", 1.0), int32Schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(incompatible("INT32", "DOUBLE"));
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(FilterPredicate.eq("c", true), int32Schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(incompatible("INT32", "BOOLEAN"));
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(FilterPredicate.eq("c", "str"), int32Schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(incompatible("INT32", "BYTE_ARRAY"));
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(FilterPredicate.in("c", 1, 2), int64Schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(incompatible("INT64", "INT32"));
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(FilterPredicate.in("c", 1L, 2L), int32Schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(incompatible("INT32", "INT64"));
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(FilterPredicate.inStrings("c", "a"), int32Schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(incompatible("INT32", "BYTE_ARRAY"));

        FileSchema dateSchemaWrong = schemaWithLogicalType("c", PhysicalType.INT64, new LogicalType.DateType());
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(FilterPredicate.eq("c", LocalDate.of(2026, 1, 1)), dateSchemaWrong))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(incompatible("INT64", "INT32"));

        // A TIMESTAMP annotation on an INT32 column, and a TIME annotation on the width its unit
        // is not stored in, are dropped from the schema, so the column arrives with no logical
        // type and the unit lookup rejects it before the physical type is compared.
        FileSchema tsSchemaWrong = schemaWithLogicalType("c", PhysicalType.INT32,
                new LogicalType.TimestampType(false, LogicalType.TimeUnit.MILLIS));
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(FilterPredicate.eq("c", Instant.EPOCH), tsSchemaWrong))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'c' does not have a TIMESTAMP logical type");

        FileSchema timeMillisWrong = schemaWithLogicalType("c", PhysicalType.INT64,
                new LogicalType.TimeType(false, LogicalType.TimeUnit.MILLIS));
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(FilterPredicate.eq("c", LocalTime.NOON), timeMillisWrong))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'c' does not have a TIME logical type");

        FileSchema timeMicrosWrong = schemaWithLogicalType("c", PhysicalType.INT32,
                new LogicalType.TimeType(false, LogicalType.TimeUnit.MICROS));
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(FilterPredicate.eq("c", LocalTime.NOON), timeMicrosWrong))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'c' does not have a TIME logical type");

        FileSchema uuidWrong = schemaWithLogicalType("c", PhysicalType.INT32,
                new LogicalType.UuidType());
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.eq("c", UUID.randomUUID()), uuidWrong))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(incompatible("INT32", "FIXED_LEN_BYTE_ARRAY"));
    }

    @Test
    void toFixedLenDecimalBytes_zeroSignumPadding() {
        byte[] bytes = FilterPredicateResolver.toFixedLenDecimalBytes(BigInteger.ZERO, 4);
        assertThat(bytes).containsExactly(0, 0, 0, 0);
    }

    // ==================== String literals ====================

    /// A `String` is the literal where `getString` reads the column, and it compares as its
    /// UTF-8 bytes, which is what such a column stores.
    @ParameterizedTest(name = "{0}")
    @MethodSource
    void aStringLiteralResolvesOnATextColumn(String name, FileSchema schema) {
        assertThat(FilterPredicateResolver.resolve(FilterPredicate.eq("c", "hé"), schema))
                .isInstanceOfSatisfying(ResolvedPredicate.BinaryPredicate.class, p -> {
                    assertThat(p.comparison()).isEqualTo(Comparison.BYTE_STRING);
                    assertThat(p.value()).containsExactly(0x68, 0xC3, 0xA9);
                });
        assertThat(FilterPredicateResolver.resolve(FilterPredicate.inStrings("c", "hé"), schema))
                .isInstanceOfSatisfying(ResolvedPredicate.BinaryInPredicate.class, p -> {
                    assertThat(p.comparison()).isEqualTo(Comparison.BYTE_STRING);
                    assertThat(p.values()[0]).containsExactly(0x68, 0xC3, 0xA9);
                });
    }

    static Stream<Arguments> aStringLiteralResolvesOnATextColumn() {
        return Stream.of(
                Arguments.of("STRING", schemaWithLogicalType("c", PhysicalType.BYTE_ARRAY,
                        LogicalType.string())),
                Arguments.of("ENUM", schemaWithLogicalType("c", PhysicalType.BYTE_ARRAY,
                        LogicalType.enumType())),
                Arguments.of("JSON", schemaWithLogicalType("c", PhysicalType.BYTE_ARRAY,
                        LogicalType.json())),
                Arguments.of("unannotated BYTE_ARRAY", schemaWithLogicalType("c",
                        PhysicalType.BYTE_ARRAY, null)));
    }

    /// Every other binary column stores bytes its annotation reads as something else, and a
    /// `String` on one would be taken as the bytes it encodes rather than as the text that was
    /// written. It is refused, naming the literals the column does take.
    @ParameterizedTest(name = "{0}")
    @MethodSource
    void aStringLiteralIsRefusedOnAColumnThatDoesNotHoldText(String name, FileSchema schema,
            String message) {
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(FilterPredicate.eq("c", "a"), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(message);
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.inStrings("c", "a"), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(message);
    }

    static Stream<Arguments> aStringLiteralIsRefusedOnAColumnThatDoesNotHoldText() {
        return Stream.of(
                Arguments.of("DECIMAL over BYTE_ARRAY",
                        schemaWithLogicalType("c", PhysicalType.BYTE_ARRAY, LogicalType.decimal(9, 2)),
                        notText("annotated DECIMAL(9, 2)", "BigDecimal and byte[]")),
                Arguments.of("DECIMAL over FIXED_LEN_BYTE_ARRAY",
                        schemaWithLogicalType("c", PhysicalType.FIXED_LEN_BYTE_ARRAY, 4,
                                LogicalType.decimal(9, 2)),
                        notText("annotated DECIMAL(9, 2)", "BigDecimal and byte[]")),
                Arguments.of("FLOAT16",
                        schemaWithLogicalType("c", PhysicalType.FIXED_LEN_BYTE_ARRAY, 2,
                                LogicalType.float16()),
                        notText("annotated FLOAT16", "float and byte[]")),
                Arguments.of("UUID",
                        schemaWithLogicalType("c", PhysicalType.FIXED_LEN_BYTE_ARRAY, 16,
                                LogicalType.uuid()),
                        notText("annotated UUID", "UUID and byte[]")),
                Arguments.of("INTERVAL",
                        schemaWithLogicalType("c", PhysicalType.FIXED_LEN_BYTE_ARRAY, 12,
                                new LogicalType.IntervalType()),
                        notText("annotated INTERVAL", "byte[]")),
                Arguments.of("BSON",
                        schemaWithLogicalType("c", PhysicalType.BYTE_ARRAY, LogicalType.bson()),
                        notText("annotated BSON", "byte[]")),
                Arguments.of("GEOMETRY",
                        schemaWithLogicalType("c", PhysicalType.BYTE_ARRAY,
                                new LogicalType.GeometryType(null)),
                        notText("annotated GEOMETRY", "byte[]")),
                Arguments.of("GEOGRAPHY",
                        schemaWithLogicalType("c", PhysicalType.BYTE_ARRAY,
                                new LogicalType.GeographyType(null, null)),
                        notText("annotated GEOGRAPHY", "byte[]")),
                Arguments.of("NULL",
                        schemaWithLogicalType("c", PhysicalType.BYTE_ARRAY, new LogicalType.NullType()),
                        notText("annotated NULL", "byte[]")),
                Arguments.of("unannotated FIXED_LEN_BYTE_ARRAY",
                        schemaWithLogicalType("c", PhysicalType.FIXED_LEN_BYTE_ARRAY, 4, null),
                        notText("an unannotated FIXED_LEN_BYTE_ARRAY", "byte[]")));
    }

    /// The same column takes the bytes themselves, which is what its accessor returns for it.
    @Test
    void aByteLiteralResolvesOnAColumnThatDoesNotHoldText() {
        FileSchema schema = schemaWithLogicalType("c", PhysicalType.FIXED_LEN_BYTE_ARRAY, 4, null);

        assertThat(FilterPredicateResolver.resolve(
                FilterPredicate.eq("c", byteLiteral(0x61, 0x61, 0x61, 0x61)), schema))
                .isInstanceOfSatisfying(ResolvedPredicate.BinaryPredicate.class,
                        p -> assertThat(p.value()).containsExactly(0x61, 0x61, 0x61, 0x61));
    }

    // ==================== Helpers ====================

    /// The message a `String` literal raises on a column that does not hold text.
    private static String notText(String description, String literals) {
        return "Column 'c' is " + description + ", which takes " + literals
                + " literals, not a String";
    }

    /// The message `validateType` raises when a column's physical type does not admit the
    /// predicate's value type.
    private static String incompatible(String actualType, String expectedType) {
        return "Column 'c' has physical type " + actualType
                + "; given filter predicate type " + expectedType + " is incompatible";
    }

    private static FileSchema schemaWithLogicalType(String columnName, PhysicalType type,
            LogicalType logicalType) {
        return schemaWithLogicalType(columnName, type, null, logicalType);
    }

    private static FileSchema schemaWithLogicalType(String columnName, PhysicalType type,
            Integer typeLength, LogicalType logicalType) {
        SchemaElement root = SchemaElement.root("root", 1);
        SchemaElement col = new SchemaElement(columnName, type, typeLength, RepetitionType.REQUIRED,
                null, null, null, null, null, logicalType);
        return FileSchema.fromSchemaElements(List.of(root, col));
    }

    private static FileSchema schemaWithRepeatedColumn(String columnName, PhysicalType type,
            Integer typeLength, LogicalType logicalType) {
        SchemaElement root = SchemaElement.root("root", 1);
        SchemaElement col = new SchemaElement(columnName, type, typeLength, RepetitionType.REPEATED,
                null, null, null, null, null, logicalType);
        return FileSchema.fromSchemaElements(List.of(root, col));
    }
}
