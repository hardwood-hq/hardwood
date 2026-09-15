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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.internal.predicate.ResolvedPredicate.BinaryPredicate.Comparison;
import dev.hardwood.metadata.ColumnOrder;
import dev.hardwood.metadata.ConvertedType;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.row.PqInterval;
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
                .hasMessage("Column 'col' is an unannotated INT32, which takes int literals, not a LocalDate");
    }

    @Test
    void resolveDateOnDifferentlyAnnotatedIntColumnThrows() {
        FileSchema schema = schemaWithLogicalType("col", PhysicalType.INT32,
                new LogicalType.IntType(32, true));
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.gt("col", LocalDate.of(2024, 6, 15)), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'col' is annotated INT_32, which takes int literals, not a LocalDate");
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
                .hasMessage("Column 'col' is an unannotated INT64, which takes long literals, not an Instant");
    }

    @Test
    void resolveInstantOnLocalTimestampColumnThrows() {
        FileSchema schema = schemaWithLogicalType("ts", PhysicalType.INT64,
                LogicalType.timestamp(false, LogicalType.TimeUnit.MICROS));
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.gt("ts", Instant.EPOCH), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'ts' is a local-wall-clock TIMESTAMP (isAdjustedToUTC=false),"
                        + " which takes LocalDateTime and long literals, not an Instant");
    }

    @ParameterizedTest
    @EnumSource(value = ConvertedType.class, names = { "TIMESTAMP_MILLIS", "TIMESTAMP_MICROS" })
    void resolveInstantOnLegacyTimestampColumn(ConvertedType convertedType) {
        FileSchema schema = schemaWithConvertedType("ts", PhysicalType.INT64, convertedType);
        Instant instant = Instant.parse("2024-06-15T12:30:00.123Z");
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(FilterPredicate.eq("ts", instant), schema);

        long expected = convertedType == ConvertedType.TIMESTAMP_MILLIS
                ? instant.toEpochMilli()
                : Math.multiplyExact(instant.toEpochMilli(), 1_000L);
        assertThat(((ResolvedPredicate.LongPredicate) resolved).value()).isEqualTo(expected);
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.eq("ts", LocalDateTime.ofInstant(instant, ZoneOffset.UTC)), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'ts' is a UTC-adjusted TIMESTAMP (isAdjustedToUTC=true),"
                        + " which takes Instant and long literals, not a LocalDateTime");
    }

    // ==================== INT96 ====================

    /// The last Julian day an `INT96` stores, as an epoch second at its midnight.
    private static final long LAST_INT96_DAY_EPOCH_SECOND = (Integer.MAX_VALUE - 2_440_588L) * 86_400L;

    @ParameterizedTest
    @EnumSource(FilterPredicate.Operator.class)
    void resolveInstantOnInt96ToItsCanonicalBytes(FilterPredicate.Operator op) {
        FileSchema schema = schemaWithLogicalType("ts", PhysicalType.INT96, null);
        // 2023-11-14 is Julian day 2460263.
        Instant instant = Instant.parse("2023-11-14T12:00:00.000000001Z");

        assertThat(FilterPredicateResolver.resolve(instantPredicate("ts", op, instant), schema))
                .usingRecursiveComparison().withStrictTypeChecking().isEqualTo(new ResolvedPredicate.BinaryPredicate(0, op,
                        int96(43_200_000_000_001L, 2_460_263), Comparison.INT96_INSTANT));
    }

    @ParameterizedTest
    @EnumSource(FilterPredicate.Operator.class)
    void resolveInstantBeforeTheEpochOnInt96(FilterPredicate.Operator op) {
        FileSchema schema = schemaWithLogicalType("ts", PhysicalType.INT96, null);

        assertThat(FilterPredicateResolver.resolve(instantPredicate("ts", op, Instant.ofEpochSecond(-1, 5)), schema))
                .usingRecursiveComparison().withStrictTypeChecking().isEqualTo(new ResolvedPredicate.BinaryPredicate(0, op,
                        int96(86_399_000_000_005L, 2_440_587), Comparison.INT96_INSTANT));
    }

    /// A stored value's nanoseconds may run past one day, so a value can lie beyond the last Julian
    /// day. A literal there, for equality as for order, keeps that day and carries the rest in its
    /// nanoseconds.
    @ParameterizedTest
    @EnumSource(FilterPredicate.Operator.class)
    void resolveInstantPastTheLastInt96DayIntoItsNanoseconds(FilterPredicate.Operator op) {
        FileSchema schema = schemaWithLogicalType("ts", PhysicalType.INT96, null);
        Instant past = Instant.ofEpochSecond(LAST_INT96_DAY_EPOCH_SECOND + 2 * 86_400L + 5);

        assertThat(FilterPredicateResolver.resolve(instantPredicate("ts", op, past), schema))
                .usingRecursiveComparison().withStrictTypeChecking().isEqualTo(new ResolvedPredicate.BinaryPredicate(0, op,
                        int96(2 * 86_400_000_000_000L + 5_000_000_000L, Integer.MAX_VALUE),
                        Comparison.INT96_INSTANT));
    }

    @ParameterizedTest
    @EnumSource(FilterPredicate.Operator.class)
    void resolveInstantBeforeTheFirstInt96DayIntoItsNanoseconds(FilterPredicate.Operator op) {
        FileSchema schema = schemaWithLogicalType("ts", PhysicalType.INT96, null);
        long firstDayEpochSecond = (Integer.MIN_VALUE - 2_440_588L) * 86_400L;
        Instant before = Instant.ofEpochSecond(firstDayEpochSecond - 86_400L + 5);

        assertThat(FilterPredicateResolver.resolve(instantPredicate("ts", op, before), schema))
                .usingRecursiveComparison().withStrictTypeChecking().isEqualTo(new ResolvedPredicate.BinaryPredicate(0, op,
                        int96(-86_400_000_000_000L + 5_000_000_000L, Integer.MIN_VALUE),
                        Comparison.INT96_INSTANT));
    }

    /// The latest and earliest instants an `INT96` encodes are the extreme day with the extreme
    /// nanoseconds; an equality literal one nanosecond beyond either is not a value it can hold.
    @Test
    void resolveEqualityPastEveryInt96ValueThrows() {
        FileSchema schema = schemaWithLogicalType("ts", PhysicalType.INT96, null);
        Instant latest = Instant.ofEpochSecond(LAST_INT96_DAY_EPOCH_SECOND, 0).plusNanos(Long.MAX_VALUE);
        Instant earliest = Instant.ofEpochSecond((Integer.MIN_VALUE - 2_440_588L) * 86_400L, 0)
                .plusNanos(Long.MIN_VALUE);

        assertThat(FilterPredicateResolver.resolve(FilterPredicate.eq("ts", latest), schema))
                .usingRecursiveComparison().withStrictTypeChecking().isEqualTo(new ResolvedPredicate.BinaryPredicate(0,
                        FilterPredicate.Operator.EQ, int96(Long.MAX_VALUE, Integer.MAX_VALUE), Comparison.INT96_INSTANT));
        assertThat(FilterPredicateResolver.resolve(FilterPredicate.eq("ts", earliest), schema))
                .usingRecursiveComparison().withStrictTypeChecking().isEqualTo(new ResolvedPredicate.BinaryPredicate(0,
                        FilterPredicate.Operator.EQ, int96(Long.MIN_VALUE, Integer.MIN_VALUE), Comparison.INT96_INSTANT));

        Instant pastLatest = latest.plusNanos(1);
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(FilterPredicate.notEq("ts", pastLatest), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'ts' holds an instant within the range an INT96 encodes; "
                        + "the equality literal " + pastLatest + " is not a value it can hold");
        Instant beforeEarliest = earliest.minusNanos(1);
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(FilterPredicate.eq("ts", beforeEarliest), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'ts' holds an instant within the range an INT96 encodes; "
                        + "the equality literal " + beforeEarliest + " is not a value it can hold");
    }

    /// Past every instant an `INT96` encodes, an order literal compares against the latest or
    /// earliest one, or matches no row.
    @Test
    void resolveOrderPastEveryInt96ValueToTheExtremeValue() {
        FileSchema schema = schemaWithLogicalType("ts", PhysicalType.INT96, null);

        assertThat(FilterPredicateResolver.resolve(FilterPredicate.lt("ts", Instant.MAX), schema))
                .usingRecursiveComparison().withStrictTypeChecking().isEqualTo(new ResolvedPredicate.BinaryPredicate(0,
                        FilterPredicate.Operator.LT_EQ, int96(Long.MAX_VALUE, Integer.MAX_VALUE), Comparison.INT96_INSTANT));
        assertThat(FilterPredicateResolver.resolve(FilterPredicate.gtEq("ts", Instant.MAX), schema))
                .isEqualTo(new ResolvedPredicate.NoRowPredicate(0));
        assertThat(FilterPredicateResolver.resolve(FilterPredicate.ltEq("ts", Instant.MIN), schema))
                .isEqualTo(new ResolvedPredicate.NoRowPredicate(0));
        assertThat(FilterPredicateResolver.resolve(FilterPredicate.gt("ts", Instant.MIN), schema))
                .usingRecursiveComparison().withStrictTypeChecking().isEqualTo(new ResolvedPredicate.BinaryPredicate(0,
                        FilterPredicate.Operator.GT_EQ, int96(Long.MIN_VALUE, Integer.MIN_VALUE), Comparison.INT96_INSTANT));
    }

    /// A byte literal is the stored bytes, so a non-canonical encoding of an instant is a literal
    /// of its own, compared byte for byte.
    @Test
    void resolveByteLiteralOnInt96ComparesTheStoredBytes() {
        FileSchema schema = schemaWithLogicalType("ts", PhysicalType.INT96, null);
        byte[] nonCanonical = int96(86_400_000_000_000L, 2_460_262);

        assertThat(FilterPredicateResolver.resolve(FilterPredicate.eq("ts", nonCanonical), schema))
                .usingRecursiveComparison().withStrictTypeChecking().isEqualTo(new ResolvedPredicate.BinaryPredicate(0,
                        FilterPredicate.Operator.EQ, nonCanonical, Comparison.STORED_BYTES));
        assertThat(FilterPredicateResolver.resolve(FilterPredicate.notEq("ts", nonCanonical), schema))
                .usingRecursiveComparison().withStrictTypeChecking().isEqualTo(new ResolvedPredicate.BinaryPredicate(0,
                        FilterPredicate.Operator.NOT_EQ, nonCanonical, Comparison.STORED_BYTES));
        assertThat(FilterPredicateResolver.resolve(FilterPredicate.in("ts", nonCanonical), schema))
                .isInstanceOfSatisfying(ResolvedPredicate.BinaryInPredicate.class, p -> {
                    assertThat(p.values()).isDeepEqualTo(new byte[][] { nonCanonical });
                    assertThat(p.comparison()).isEqualTo(Comparison.STORED_BYTES);
                    assertThat(p.byteExact()).isTrue();
                });
    }

    @Test
    void resolveOrderedByteLiteralOnInt96Throws() {
        FileSchema schema = schemaWithLogicalType("ts", PhysicalType.INT96, null);

        assertThatThrownBy(() -> FilterPredicateResolver.resolve(FilterPredicate.gtEq("ts", new byte[12]), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'ts' is a legacy INT96 TIMESTAMP (no isAdjustedToUTC field), whose values do not "
                        + "order as their stored bytes; a byte[] literal takes eq, notEq and in there, and an ordered "
                        + "predicate takes an Instant");
    }

    @Test
    void resolveByteLiteralOfAnotherWidthOnInt96Throws() {
        FileSchema schema = schemaWithLogicalType("ts", PhysicalType.INT96, null);

        assertThatThrownBy(() -> FilterPredicateResolver.resolve(FilterPredicate.eq("ts", new byte[13]), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'ts' is an INT96, whose literal is 12 bytes, not 13");
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.in("ts", new byte[12], new byte[4]), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'ts' is an INT96, whose literal is 12 bytes, not 4");
    }

    @Test
    void resolveLocalDateTimeOnInt96Throws() {
        FileSchema schema = schemaWithLogicalType("ts", PhysicalType.INT96, null);

        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.eq("ts", LocalDateTime.parse("2023-11-14T12:00:00")), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'ts' is a legacy INT96 TIMESTAMP (no isAdjustedToUTC field),"
                        + " which takes Instant and byte[] literals, not a LocalDateTime");
    }

    private static FilterPredicate instantPredicate(String column, FilterPredicate.Operator op, Instant value) {
        return switch (op) {
            case EQ -> FilterPredicate.eq(column, value);
            case NOT_EQ -> FilterPredicate.notEq(column, value);
            case LT -> FilterPredicate.lt(column, value);
            case LT_EQ -> FilterPredicate.ltEq(column, value);
            case GT -> FilterPredicate.gt(column, value);
            case GT_EQ -> FilterPredicate.gtEq(column, value);
        };
    }

    /// Twelve `INT96` bytes: nanoseconds of the day, then the Julian day, both little-endian.
    private static byte[] int96(long nanosOfDay, int julianDay) {
        return ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN).putLong(nanosOfDay).putInt(julianDay).array();
    }

    // ==================== TIMESTAMP over FIXED_LEN_BYTE_ARRAY(12) ====================

    /// An `Instant` resolves to the twelve little-endian bytes of its count of the column's unit,
    /// compared in the signed order of the value.
    @ParameterizedTest
    @EnumSource(FilterPredicate.Operator.class)
    void resolveInstantOnATwelveByteTimestampToItsCount(FilterPredicate.Operator op) {
        FileSchema schema = schemaWithLogicalType("ts", PhysicalType.FIXED_LEN_BYTE_ARRAY, 12,
                LogicalType.timestamp(true, LogicalType.TimeUnit.NANOS));

        assertThat(FilterPredicateResolver.resolve(instantPredicate("ts", op, Instant.parse("1969-12-31T23:59:59Z")),
                schema))
                .usingRecursiveComparison().withStrictTypeChecking().isEqualTo(new ResolvedPredicate.BinaryPredicate(0, op,
                        HexFormat.of().parseHex("003665c4ffffffffffffffff"), Comparison.FIXED_TIMESTAMP));
    }

    /// Past the `INT64` nanosecond range, where the count needs the twelfth byte.
    @Test
    void resolveLocalDateTimeSetOnATwelveByteTimestampPastTheInt64Range() {
        FileSchema schema = schemaWithLogicalType("ts", PhysicalType.FIXED_LEN_BYTE_ARRAY, 12,
                LogicalType.timestamp(false, LogicalType.TimeUnit.NANOS));

        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.in("ts", LocalDateTime.parse("0001-01-01T00:00")), schema);

        assertThat(resolved).isInstanceOf(ResolvedPredicate.BinaryInPredicate.class);
        ResolvedPredicate.BinaryInPredicate in = (ResolvedPredicate.BinaryInPredicate) resolved;
        assertThat(in.comparison()).isEqualTo(Comparison.FIXED_TIMESTAMP);
        assertThat(in.values()).isDeepEqualTo(new byte[][] { HexFormat.of().parseHex("00001a3deb03b2a1fcffffff") });
    }

    /// An `Instant` between two milliseconds is no value of a `MILLIS` column: equality refuses it
    /// and an order moves to the whole millisecond on the side it admits.
    @Test
    void resolveInstantBetweenTwoUnitsOnATwelveByteTimestamp() {
        FileSchema schema = schemaWithLogicalType("ts", PhysicalType.FIXED_LEN_BYTE_ARRAY, 12,
                LogicalType.timestamp(true, LogicalType.TimeUnit.MILLIS));
        Instant between = Instant.parse("1969-12-31T23:59:59.9995Z");

        assertThatThrownBy(() -> FilterPredicateResolver.resolve(FilterPredicate.eq("ts", between), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'ts' holds a whole number of milliseconds; the equality literal "
                        + "1969-12-31T23:59:59.999500Z is not a value it can hold");
        assertThat(FilterPredicateResolver.resolve(FilterPredicate.gt("ts", between), schema))
                .usingRecursiveComparison().withStrictTypeChecking().isEqualTo(new ResolvedPredicate.BinaryPredicate(0,
                        FilterPredicate.Operator.GT_EQ, new byte[12], Comparison.FIXED_TIMESTAMP));
    }

    /// A `byte[]` is the stored bytes, for equality only, and twelve of them.
    @Test
    void resolveBytesOnATwelveByteTimestamp() {
        FileSchema schema = schemaWithLogicalType("ts", PhysicalType.FIXED_LEN_BYTE_ARRAY, 12,
                LogicalType.timestamp(true, LogicalType.TimeUnit.MICROS));

        assertThat(FilterPredicateResolver.resolve(FilterPredicate.notEq("ts", new byte[12]), schema))
                .usingRecursiveComparison().withStrictTypeChecking().isEqualTo(ResolvedPredicate.negate(
                        new ResolvedPredicate.BinaryPredicate(0, FilterPredicate.Operator.EQ, new byte[12],
                                Comparison.FIXED_TIMESTAMP)));
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(FilterPredicate.ltEq("ts", new byte[12]), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'ts' is annotated TIMESTAMP(MICROS, UTC), whose values do not order as their "
                        + "stored bytes; a byte[] literal takes eq, notEq and in there, and an ordered predicate "
                        + "takes an Instant");
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(FilterPredicate.eq("ts", new byte[8]), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'ts' is a FIXED_LEN_BYTE_ARRAY(12) TIMESTAMP, whose literal is 12 bytes, not 8");
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(FilterPredicate.eq("ts", 0L), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'ts' is annotated TIMESTAMP(MICROS, UTC), which takes Instant and byte[] "
                        + "literals, not a long");
    }

    // ==================== LocalDateTime ====================

    static Stream<Arguments> localDateTimeUnits() {
        LocalDateTime wallClock = LocalDateTime.parse("2024-06-15T12:30:00.123456789");
        long epochSecond = wallClock.toEpochSecond(ZoneOffset.UTC);
        return Stream.of(
                Arguments.of(LogicalType.TimeUnit.MILLIS, LocalDateTime.parse("2024-06-15T12:30:00.123"),
                        epochSecond * 1_000L + 123L),
                Arguments.of(LogicalType.TimeUnit.MICROS, LocalDateTime.parse("2024-06-15T12:30:00.123456"),
                        epochSecond * 1_000_000L + 123_456L),
                Arguments.of(LogicalType.TimeUnit.NANOS, wallClock, epochSecond * 1_000_000_000L + 123_456_789L),
                Arguments.of(LogicalType.TimeUnit.MICROS, LocalDateTime.parse("1969-12-31T23:59:59.999999"), -1L));
    }

    @ParameterizedTest
    @MethodSource("localDateTimeUnits")
    void resolveLocalDateTimeToWallClockInColumnUnit(LogicalType.TimeUnit unit, LocalDateTime wallClock,
            long expected) {
        FileSchema schema = schemaWithLogicalType("ts", PhysicalType.INT64, LogicalType.timestamp(false, unit));
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.ltEq("ts", wallClock), schema);

        assertThat(resolved).isEqualTo(new ResolvedPredicate.LongPredicate(0, FilterPredicate.Operator.LT_EQ,
                expected));
    }

    @Test
    void resolveLocalDateTimeOnUtcTimestampColumnThrows() {
        FileSchema schema = schemaWithLogicalType("ts", PhysicalType.INT64,
                LogicalType.timestamp(true, LogicalType.TimeUnit.MILLIS));
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.eq("ts", LocalDateTime.of(2024, 6, 15, 12, 30)), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'ts' is a UTC-adjusted TIMESTAMP (isAdjustedToUTC=true),"
                        + " which takes Instant and long literals, not a LocalDateTime");
    }

    @Test
    void resolveLocalDateTimeOnNonTimestampColumnThrows() {
        FileSchema schema = schemaWithLogicalType("col", PhysicalType.INT64, null);
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.eq("col", LocalDateTime.of(2024, 6, 15, 12, 30)), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'col' is an unannotated INT64, which takes long literals, not a LocalDateTime");
    }

    @Test
    void resolveSubUnitLocalDateTime() {
        FileSchema schema = schemaWithLogicalType("ts", PhysicalType.INT64,
                LogicalType.timestamp(false, LogicalType.TimeUnit.MILLIS));
        LocalDateTime subMilli = LocalDateTime.parse("1970-01-01T00:00:00.0015");

        assertThatThrownBy(() -> FilterPredicateResolver.resolve(FilterPredicate.eq("ts", subMilli), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'ts' holds a whole number of milliseconds within the INT64 range;"
                        + " the equality literal 1970-01-01T00:00:00.001500 is not a value it can hold");
        assertThat(FilterPredicateResolver.resolve(FilterPredicate.lt("ts", subMilli), schema))
                .isEqualTo(new ResolvedPredicate.LongPredicate(0, FilterPredicate.Operator.LT_EQ, 1L));
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
                .hasMessage("Column 'col' is annotated DATE, which takes LocalDate and int literals, not a LocalTime");
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
                .hasMessage("Column 't' is an unannotated INT64, which takes long literals, not a LocalTime");
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
                .hasMessage("Column 'amount' is an unannotated INT32, which takes int literals, not a BigDecimal");
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
        assertThat(bp.comparison()).isEqualTo(Comparison.FIXED_DECIMAL);
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
                .hasMessage("Column 'col' is annotated DATE, which takes LocalDate and int literals, not a BigDecimal");
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
    void resolveOrderedByteLiteralOnDecimalThrows() {
        FileSchema schema = schemaWithLogicalType("amount", PhysicalType.FIXED_LEN_BYTE_ARRAY, 8,
                LogicalType.decimal(18, 2));
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(FilterPredicate.gt("amount", new byte[8]), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'amount' is annotated DECIMAL(18, 2), whose values do not order as their stored "
                        + "bytes; a byte[] literal takes eq, notEq and in there, and an ordered predicate takes a "
                        + "BigDecimal");
    }

    @Test
    void resolveNegativeDecimalFixedLenByteArray() {
        FileSchema schema = schemaWithLogicalType("amount", PhysicalType.FIXED_LEN_BYTE_ARRAY, 8,
                LogicalType.decimal(18, 2));
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.eq("amount", new BigDecimal("-1.50")), schema);
        assertThat(resolved).isInstanceOf(ResolvedPredicate.BinaryPredicate.class);
        ResolvedPredicate.BinaryPredicate bp = (ResolvedPredicate.BinaryPredicate) resolved;
        assertThat(bp.comparison()).isEqualTo(Comparison.FIXED_DECIMAL);
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

    // ==================== Byte literal on a DECIMAL or FLOAT16 ====================

    /// A fixed-width `DECIMAL` holds each number under one encoding, so equality on its stored
    /// bytes is equality on the number, and it takes a literal of exactly the column width.
    @Test
    void resolveByteLiteralOnFixedLenDecimalAsTheStoredBytes() {
        assertThat(resolveFixedDecimalLiteral(0x00, 0x00, 0x00, 0x7D)).containsExactly(0x00, 0x00, 0x00, 0x7D);
        assertThat(resolveFixedDecimalLiteral(0xFF, 0xFF, 0xFF, 0x83)).containsExactly(0xFF, 0xFF, 0xFF, 0x83);
    }

    @Test
    void resolveByteLiteralOfAnotherWidthOnFixedLenDecimalThrows() {
        assertThatThrownBy(() -> resolveFixedDecimalLiteral(0x7D))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'amount' holds a byte string of 4 bytes; "
                        + "the equality literal 7d (1 bytes) is not a value it can hold");
        assertThatThrownBy(() -> resolveFixedDecimalLiteral(0x00, 0x00, 0x00, 0x00, 0x7D))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'amount' holds a byte string of 4 bytes; "
                        + "the equality literal 000000007d (5 bytes) is not a value it can hold");
        assertThatThrownBy(() -> resolveFixedDecimalLiteral())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'amount' holds a byte string of 4 bytes; "
                        + "the equality literal  (0 bytes) is not a value it can hold");
    }

    @Test
    void resolveByteLiteralProbesOnFixedLenDecimalAsTheStoredBytes() {
        FileSchema schema = schemaWithLogicalType("amount", PhysicalType.FIXED_LEN_BYTE_ARRAY, 4,
                LogicalType.decimal(9, 2));
        byte[][] probes = { byteLiteral(0x00, 0x00, 0x00, 0x7D), byteLiteral(0xFF, 0xFF, 0xFF, 0x83) };

        assertThat(FilterPredicateResolver.resolve(FilterPredicate.in("amount", probes), schema))
                .isInstanceOfSatisfying(ResolvedPredicate.BinaryInPredicate.class, p -> {
                    assertThat(p.comparison()).isEqualTo(Comparison.FIXED_DECIMAL);
                    assertThat(p.values()).isDeepEqualTo(probes);
                });
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.in("amount", probes[0], byteLiteral(0x7D)), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'amount' holds a byte string of 4 bytes; "
                        + "the equality literal 7d (1 bytes) is not a value it can hold");
    }

    /// A `BYTE_ARRAY` `DECIMAL` may hold a number under more than one encoding. Byte equality is
    /// the number's equality, which reads the bounds, and the bytes' own, which reads none.
    @Test
    void resolveByteLiteralOnByteArrayDecimalPairsTheValueWithTheStoredBytes() {
        FileSchema schema = schemaWithLogicalType("amount", PhysicalType.BYTE_ARRAY, LogicalType.decimal(18, 2));
        byte[] padded = byteLiteral(0x00, 0x7D);

        assertThat(FilterPredicateResolver.resolve(FilterPredicate.eq("amount", padded), schema))
                .usingRecursiveComparison().withStrictTypeChecking().isEqualTo(new ResolvedPredicate.And(List.of(
                        new ResolvedPredicate.BinaryPredicate(0, FilterPredicate.Operator.EQ, padded,
                                Comparison.VARIABLE_DECIMAL),
                        new ResolvedPredicate.BinaryPredicate(0, FilterPredicate.Operator.EQ, padded,
                                Comparison.STORED_BYTES))));
        assertThat(FilterPredicateResolver.resolve(FilterPredicate.notEq("amount", padded), schema))
                .usingRecursiveComparison().withStrictTypeChecking().isEqualTo(new ResolvedPredicate.Or(List.of(
                        new ResolvedPredicate.BinaryPredicate(0, FilterPredicate.Operator.NOT_EQ, padded,
                                Comparison.VARIABLE_DECIMAL),
                        new ResolvedPredicate.BinaryPredicate(0, FilterPredicate.Operator.NOT_EQ, padded,
                                Comparison.STORED_BYTES))));
        assertThat(FilterPredicateResolver.resolve(FilterPredicate.in("amount", padded), schema))
                .usingRecursiveComparison().withStrictTypeChecking().isEqualTo(new ResolvedPredicate.And(List.of(
                        new ResolvedPredicate.BinaryInPredicate(0, new byte[][] { padded }, Comparison.VARIABLE_DECIMAL),
                        new ResolvedPredicate.BinaryInPredicate(0, new byte[][] { padded }, Comparison.STORED_BYTES))));
    }

    /// Two NaN encodings decode to halves `Float.compare` calls equal, so the half's equality,
    /// which reads the bounds, is paired with the bytes'.
    @Test
    void resolveByteLiteralOnFloat16PairsTheHalfWithTheStoredBytes() {
        FileSchema schema = schemaWithLogicalType("h", PhysicalType.FIXED_LEN_BYTE_ARRAY, 2, LogicalType.float16());
        byte[] signedNaN = byteLiteral(0x00, 0xFE);

        assertThat(FilterPredicateResolver.resolve(FilterPredicate.eq("h", signedNaN), schema))
                .usingRecursiveComparison().withStrictTypeChecking().withComparatorForType(Float::compare, Float.class)
                .isEqualTo(new ResolvedPredicate.And(List.of(
                        new ResolvedPredicate.Float16Predicate(0, FilterPredicate.Operator.EQ, Float.NaN, false),
                        new ResolvedPredicate.BinaryPredicate(0, FilterPredicate.Operator.EQ, signedNaN,
                                Comparison.STORED_BYTES))));
        assertThat(FilterPredicateResolver.resolve(FilterPredicate.in("h", signedNaN), schema))
                .usingRecursiveComparison().withStrictTypeChecking().withComparatorForType(Float::compare, Float.class)
                .isEqualTo(new ResolvedPredicate.And(List.of(
                        new ResolvedPredicate.Float16InPredicate(0, new float[] { Float.NaN }, false),
                        new ResolvedPredicate.BinaryInPredicate(0, new byte[][] { signedNaN }, Comparison.STORED_BYTES))));
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(FilterPredicate.lt("h", signedNaN), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'h' is annotated FLOAT16, whose values do not order as their stored bytes; "
                        + "a byte[] literal takes eq, notEq and in there, and an ordered predicate takes a float");
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(FilterPredicate.eq("h", byteLiteral(0x00)), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'h' is a FLOAT16, whose literal is 2 bytes, not 1");
    }

    /// A fixed-width column that compares as a byte string refuses a probe of another width too.
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
        assertThat(bp.comparison()).isEqualTo(Comparison.BYTE_STRING);
        assertThat(bp.op()).isEqualTo(FilterPredicate.Operator.EQ);
        assertThat(bp.columnIndex()).isEqualTo(0);
    }

    @Test
    void resolveUuidOnNonUuidColumnThrows() {
        FileSchema schema = schemaWithLogicalType("id", PhysicalType.FIXED_LEN_BYTE_ARRAY, 16, null);
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.eq("id", UUID.randomUUID()), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'id' is an unannotated FIXED_LEN_BYTE_ARRAY, which takes byte[] literals, not a UUID");
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
                .hasMessage("Column 'col' is an unannotated BYTE_ARRAY"
                         + ", which takes String and byte[] literals, not an int");
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
                .hasMessage("Column 'col' is annotated STRING; intersects takes a GEOMETRY or GEOGRAPHY column")
                ;
    }

    /// A bounding-box overlap has no inverse, so `not` over one is refused where every other
    /// predicate the rule does not admit is: at reader creation, naming the column.
    @Test
    void resolveNotIntersectsThrows() {
        FileSchema schema = schemaWithLogicalType("loc", PhysicalType.BYTE_ARRAY,
                LogicalType.geometry("OGC:CRS84"));
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.not(FilterPredicate.intersects("loc", 0.0, 0.0, 1.0, 1.0)), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'loc' is tested by intersects, which has no inverse and cannot"
                        + " appear below not");
    }

    /// `not` is lowered by inverting each leaf below it, so an `intersects` anywhere in that
    /// subtree is the one that has no inverse.
    @Test
    void resolveNotOverACompoundHoldingIntersectsThrows() {
        FileSchema schema = FileSchema.fromSchemaElements(List.of(
                SchemaElement.root("root", 2),
                new SchemaElement("loc", PhysicalType.BYTE_ARRAY, null, RepetitionType.OPTIONAL,
                        null, null, null, null, null, LogicalType.geometry("OGC:CRS84")),
                new SchemaElement("id", PhysicalType.INT32, null, RepetitionType.OPTIONAL,
                        null, null, null, null, null, null)));

        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.not(FilterPredicate.and(
                        FilterPredicate.eq("id", 1),
                        FilterPredicate.not(FilterPredicate.intersects("loc", 0.0, 0.0, 1.0, 1.0)))),
                schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'loc' is tested by intersects, which has no inverse and cannot"
                        + " appear below not");
    }

    // ==================== Ordered operators ====================

    /// These values have no order, so comparing them answers in an order nobody wrote: an
    /// `INTERVAL`'s little-endian components sort 256 months below 1 month. The refusal names the
    /// literals the column does take, so a literal of another type is not sent on to equality.
    @ParameterizedTest(name = "{0}")
    @MethodSource
    void anOrderedOperatorIsRefusedOnAColumnWithNoOrder(String name, FileSchema schema,
            FilterPredicate ordered, String annotation, String literals) {
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(ordered, schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'c' is annotated " + annotation + ", which defines no order; it takes "
                        + literals + " literals with eq, notEq and in only");
    }

    static Stream<Arguments> anOrderedOperatorIsRefusedOnAColumnWithNoOrder() {
        FileSchema interval = schemaWithLogicalType("c", PhysicalType.FIXED_LEN_BYTE_ARRAY, 12,
                new LogicalType.IntervalType());
        FileSchema geometry = schemaWithLogicalType("c", PhysicalType.BYTE_ARRAY,
                LogicalType.geometry("OGC:CRS84"));
        FileSchema geography = schemaWithLogicalType("c", PhysicalType.BYTE_ARRAY,
                LogicalType.geography("OGC:CRS84", LogicalType.EdgeInterpolationAlgorithm.SPHERICAL));
        FileSchema nullColumn = schemaWithLogicalType("c", PhysicalType.INT32, new LogicalType.NullType());
        return Stream.of(
                Arguments.of("INTERVAL, byte literal", interval,
                        FilterPredicate.lt("c", new byte[12]), "INTERVAL", "PqInterval and byte[]"),
                Arguments.of("INTERVAL, PqInterval literal", interval,
                        new FilterPredicate.IntervalColumnPredicate("c", FilterPredicate.Operator.GT,
                                new PqInterval(1, 0, 0)), "INTERVAL", "PqInterval and byte[]"),
                Arguments.of("GEOMETRY", geometry, FilterPredicate.gtEq("c", new byte[] { 1 }),
                        "GEOMETRY(OGC:CRS84)", "byte[]"),
                Arguments.of("GEOMETRY, a literal type it does not take", geometry, FilterPredicate.lt("c", 1),
                        "GEOMETRY(OGC:CRS84)", "byte[]"),
                Arguments.of("GEOGRAPHY", geography, FilterPredicate.ltEq("c", new byte[] { 1 }),
                        "GEOGRAPHY(OGC:CRS84, SPHERICAL)", "byte[]"),
                Arguments.of("NULL", nullColumn, FilterPredicate.lt("c", 1), "NULL", "int"),
                Arguments.of("NULL, a literal type it does not take", nullColumn, FilterPredicate.lt("c", 1L),
                        "NULL", "int"));
    }

    /// Equality asks whether a stored value *is* the literal, which the stored bytes answer
    /// whether or not the values order.
    @Test
    void anEqualityLiteralResolvesOnAColumnWithNoOrder() {
        FileSchema schema = schemaWithLogicalType("c", PhysicalType.BYTE_ARRAY,
                LogicalType.geometry("OGC:CRS84"));

        assertThat(FilterPredicateResolver.resolve(FilterPredicate.eq("c", new byte[] { 1 }), schema))
                .isInstanceOfSatisfying(ResolvedPredicate.BinaryPredicate.class, p -> {
                    assertThat(p.op()).isEqualTo(FilterPredicate.Operator.EQ);
                    assertThat(p.value()).containsExactly(1);
                    assertThat(p.comparison()).isEqualTo(Comparison.BYTE_STRING);
                });
        assertThat(FilterPredicateResolver.resolve(FilterPredicate.in("c", new byte[] { 1 }), schema))
                .isInstanceOfSatisfying(ResolvedPredicate.BinaryInPredicate.class, p -> {
                    assertThat(p.values()).hasDimensions(1, 1);
                    assertThat(p.values()[0]).containsExactly(1);
                    assertThat(p.comparison()).isEqualTo(Comparison.BYTE_STRING);
                });
    }

    // ==================== BOOLEAN ====================

    /// `false` orders before `true` and the column holds nothing else, so every ordered operator
    /// is an equality against one of the two or a constant.
    @ParameterizedTest(name = "{0} {1}")
    @MethodSource
    void anOrderedBooleanPredicateResolvesToTheValuesItAdmits(FilterPredicate.Operator op,
            boolean literal, ResolvedPredicate expected) {
        FileSchema schema = schemaWithLogicalType("c", PhysicalType.BOOLEAN, null);

        assertThat(FilterPredicateResolver.resolve(
                new FilterPredicate.BooleanColumnPredicate("c", op, literal), schema))
                .isEqualTo(expected);
    }

    static Stream<Arguments> anOrderedBooleanPredicateResolvesToTheValuesItAdmits() {
        ResolvedPredicate isFalse = new ResolvedPredicate.BooleanPredicate(0,
                FilterPredicate.Operator.EQ, false);
        ResolvedPredicate isTrue = new ResolvedPredicate.BooleanPredicate(0,
                FilterPredicate.Operator.EQ, true);
        ResolvedPredicate none = new ResolvedPredicate.NoRowPredicate(0);
        ResolvedPredicate every = new ResolvedPredicate.EveryNonNullRowPredicate(0);
        return Stream.of(
                Arguments.of(FilterPredicate.Operator.LT, false, none),
                Arguments.of(FilterPredicate.Operator.LT, true, isFalse),
                Arguments.of(FilterPredicate.Operator.LT_EQ, false, isFalse),
                Arguments.of(FilterPredicate.Operator.LT_EQ, true, every),
                Arguments.of(FilterPredicate.Operator.GT, false, isTrue),
                Arguments.of(FilterPredicate.Operator.GT, true, none),
                Arguments.of(FilterPredicate.Operator.GT_EQ, false, every),
                Arguments.of(FilterPredicate.Operator.GT_EQ, true, isTrue));
    }

    /// A constant negates to the other constant, so `not(ltEq(c, true))` keeps out the null rows
    /// the comparison leaves unknown.
    @Test
    void negatingAnOrderedBooleanPredicateKeepsNullsOut() {
        FileSchema schema = schemaWithLogicalType("c", PhysicalType.BOOLEAN, null);

        assertThat(FilterPredicateResolver.resolve(
                FilterPredicate.not(FilterPredicate.ltEq("c", true)), schema))
                .isEqualTo(new ResolvedPredicate.NoRowPredicate(0));
        assertThat(FilterPredicateResolver.resolve(
                FilterPredicate.not(FilterPredicate.gt("c", false)), schema))
                .isEqualTo(new ResolvedPredicate.BooleanPredicate(0, FilterPredicate.Operator.NOT_EQ,
                        true));
    }

    // ==================== PqInterval ====================

    @Test
    void resolveIntervalToItsTwelveStoredBytes() {
        FileSchema schema = schemaWithLogicalType("c", PhysicalType.FIXED_LEN_BYTE_ARRAY, 12,
                new LogicalType.IntervalType());

        assertThat(FilterPredicateResolver.resolve(
                FilterPredicate.eq("c", new PqInterval(1, 2, 0x0102_0304L)), schema))
                .isInstanceOfSatisfying(ResolvedPredicate.BinaryPredicate.class, p -> {
                    assertThat(p.op()).isEqualTo(FilterPredicate.Operator.EQ);
                    assertThat(p.value())
                            .containsExactly(1, 0, 0, 0, 2, 0, 0, 0, 0x04, 0x03, 0x02, 0x01);
                    assertThat(p.comparison()).isEqualTo(Comparison.BYTE_STRING);
                });
    }

    /// Each component is stored as an unsigned 32-bit value, so one outside that range is a value
    /// the column cannot hold.
    @ParameterizedTest(name = "{0}")
    @MethodSource
    void anIntervalComponentOutsideTheUnsignedRangeThrows(String name, PqInterval literal) {
        FileSchema schema = schemaWithLogicalType("c", PhysicalType.FIXED_LEN_BYTE_ARRAY, 12,
                new LogicalType.IntervalType());

        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.eq("c", literal), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'c' holds an interval whose months, days and milliseconds are"
                        + " each within [0, 4294967295]; the equality literal " + literal
                        + " is not a value it can hold");
    }

    static Stream<Arguments> anIntervalComponentOutsideTheUnsignedRangeThrows() {
        return Stream.of(
                Arguments.of("negative months", new PqInterval(-1, 0, 0)),
                Arguments.of("days above 2^32 - 1", new PqInterval(0, 4_294_967_296L, 0)),
                Arguments.of("milliseconds above 2^32 - 1", new PqInterval(0, 0, 4_294_967_296L)));
    }

    @Test
    void resolveIntervalOnANonIntervalColumnThrows() {
        FileSchema schema = schemaWithLogicalType("c", PhysicalType.FIXED_LEN_BYTE_ARRAY, 12, null);

        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.eq("c", new PqInterval(1, 0, 0)), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'c' is an unannotated FIXED_LEN_BYTE_ARRAY, which takes byte[] literals, not a PqInterval");
    }

    // ==================== VARIANT ====================

    /// The `metadata` and `value` leaves of a `VARIANT` group hold the encoded variant, which
    /// `getVariant` reads off the group. No literal stands for one, so they take null tests only.
    @Test
    void aPredicateOnALeafBelowAVariantGroupThrows() {
        FileSchema schema = variantSchema();

        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.eq("v.value", new byte[] { 1 }), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'v.value' is a leaf of the VARIANT group 'v', which holds an"
                        + " encoded variant; it takes isNull and isNotNull predicates only");
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.in("v.metadata", new byte[] { 1 }), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'v.metadata' is a leaf of the VARIANT group 'v', which holds an"
                        + " encoded variant; it takes isNull and isNotNull predicates only");
    }

    @Test
    void aNullPredicateOnALeafBelowAVariantGroupResolves() {
        FileSchema schema = variantSchema();

        assertThat(FilterPredicateResolver.resolve(FilterPredicate.isNotNull("v.value"), schema))
                .isEqualTo(new ResolvedPredicate.IsNotNullPredicate(1, 2, 2));
        // Answered from `metadata`, the one leaf below the group that is required.
        assertThat(FilterPredicateResolver.resolve(FilterPredicate.isNull("v"), schema))
                .isEqualTo(new ResolvedPredicate.IsNullPredicate(0, 1, 1));
    }

    /// A shredded variant's `typed_value` fields sit deeper than the group's own two leaves, and
    /// they hold the shredding rather than the variant: a row whose payload stayed in `value` has
    /// them null. The group above the leaf is named however far down it is.
    @Test
    void aPredicateOnAShreddedVariantFieldThrows() {
        FileSchema schema = shreddedVariantSchema();

        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.gt("v.typed_value.age", 30), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'v.typed_value.age' is a leaf of the VARIANT group 'v', which"
                        + " holds an encoded variant; it takes isNull and isNotNull predicates only");
    }

    /// `optional group v (VARIANT) { required binary metadata; optional binary value; }`
    private static FileSchema variantSchema() {
        return FileSchema.fromSchemaElements(List.of(
                SchemaElement.root("root", 1),
                new SchemaElement("v", null, null, RepetitionType.OPTIONAL, 2, null, null, null,
                        null, LogicalType.variant(1)),
                new SchemaElement("metadata", PhysicalType.BYTE_ARRAY, null, RepetitionType.REQUIRED,
                        null, null, null, null, null, null),
                new SchemaElement("value", PhysicalType.BYTE_ARRAY, null, RepetitionType.OPTIONAL,
                        null, null, null, null, null, null)));
    }

    /// `optional group v (VARIANT) { required binary metadata; optional binary value;
    /// optional group typed_value { optional int32 age; } }`
    private static FileSchema shreddedVariantSchema() {
        return FileSchema.fromSchemaElements(List.of(
                SchemaElement.root("root", 1),
                new SchemaElement("v", null, null, RepetitionType.OPTIONAL, 3, null, null, null,
                        null, LogicalType.variant(1)),
                new SchemaElement("metadata", PhysicalType.BYTE_ARRAY, null, RepetitionType.REQUIRED,
                        null, null, null, null, null, null),
                new SchemaElement("value", PhysicalType.BYTE_ARRAY, null, RepetitionType.OPTIONAL,
                        null, null, null, null, null, null),
                new SchemaElement("typed_value", null, null, RepetitionType.OPTIONAL, 1, null, null,
                        null, null, null),
                new SchemaElement("age", PhysicalType.INT32, null, RepetitionType.OPTIONAL,
                        null, null, null, null, null, null)));
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

    // ==================== Set forms ====================

    @Test
    void resolveDoubleInOnDoubleColumn() {
        FileSchema schema = schemaWithLogicalType("d", PhysicalType.DOUBLE, null);
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.in("d", 1.5, 2.5), schema, List.of(ColumnOrder.IEEE754_TOTAL_ORDER));
        assertThat(resolved).isInstanceOf(ResolvedPredicate.DoubleInPredicate.class);
        ResolvedPredicate.DoubleInPredicate dp = (ResolvedPredicate.DoubleInPredicate) resolved;
        assertThat(dp.columnIndex()).isEqualTo(0);
        assertThat(dp.values()).containsExactly(1.5, 2.5);
        assertThat(dp.ieee754TotalOrder()).isTrue();

        ResolvedPredicate defaultOrder = FilterPredicateResolver.resolve(
                FilterPredicate.in("d", 1.5, 2.5), schema, List.of());
        assertThat(((ResolvedPredicate.DoubleInPredicate) defaultOrder).ieee754TotalOrder()).isFalse();
    }

    @Test
    void resolveFloatInOnFloatColumn() {
        FileSchema schema = schemaWithLogicalType("f", PhysicalType.FLOAT, null);
        ResolvedPredicate resolved = FilterPredicateResolver.resolve(
                FilterPredicate.in("f", 0.1f, Float.NaN), schema, List.of(ColumnOrder.IEEE754_TOTAL_ORDER));
        assertThat(resolved).isInstanceOfSatisfying(ResolvedPredicate.FloatInPredicate.class, p -> {
            assertThat(p.values()).containsExactly(0.1f, Float.NaN);
            assertThat(p.ieee754TotalOrder()).isTrue();
        });
    }

    @Test
    void resolveFloatInOnFloat16() {
        FileSchema schema = schemaWithLogicalType("h", PhysicalType.FIXED_LEN_BYTE_ARRAY, 2,
                new LogicalType.Float16Type());
        assertThat(FilterPredicateResolver.resolve(FilterPredicate.in("h", 1.5f, Float.NaN), schema))
                .isInstanceOfSatisfying(ResolvedPredicate.Float16InPredicate.class,
                        p -> assertThat(p.values()).containsExactly(1.5f, Float.NaN));

        assertThatThrownBy(() -> FilterPredicateResolver.resolve(FilterPredicate.in("h", 1.5f, 0.1f), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'h' holds a value an IEEE half represents; "
                        + "the equality literal 0.1 is not a value it can hold");
    }

    @Test
    void floatAndDoubleSetsTakeTheirOwnWidthOnly() {
        FileSchema floatSchema = schemaWithLogicalType("c", PhysicalType.FLOAT, null);
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(FilterPredicate.in("c", 1.5, 2.5), floatSchema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(notTaken("an unannotated FLOAT", "float", "a double"));

        FileSchema doubleSchema = schemaWithLogicalType("c", PhysicalType.DOUBLE, null);
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(FilterPredicate.in("c", 1.5f), doubleSchema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(notTaken("an unannotated DOUBLE", "double", "a float"));

        FileSchema float16Schema = schemaWithLogicalType("c", PhysicalType.FIXED_LEN_BYTE_ARRAY, 2,
                new LogicalType.Float16Type());
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(FilterPredicate.in("c", 1.5), float16Schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(notTaken("annotated FLOAT16", "float and byte[]", "a double"));

        FileSchema intSchema = schemaWithLogicalType("c", PhysicalType.INT32, null);
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(FilterPredicate.in("c", 1.5), intSchema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(notTaken("an unannotated INT32", "int", "a double"));
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(FilterPredicate.in("c", 1.5f), intSchema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(notTaken("an unannotated INT32", "int", "a float"));
    }

    @Test
    void resolveDateIn() {
        FileSchema schema = schemaWithLogicalType("d", PhysicalType.INT32, new LogicalType.DateType());
        assertThat(FilterPredicateResolver.resolve(
                FilterPredicate.in("d", LocalDate.ofEpochDay(19000), LocalDate.ofEpochDay(-3)), schema))
                .isInstanceOfSatisfying(ResolvedPredicate.IntInPredicate.class,
                        p -> assertThat(p.values()).containsExactly(19000, -3));

        FileSchema plain = schemaWithLogicalType("d", PhysicalType.INT32, null);
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(FilterPredicate.in("d", LocalDate.EPOCH), plain))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'd' is an unannotated INT32, which takes int literals, not a LocalDate");
    }

    @Test
    void resolveInstantIn() {
        FileSchema micros = schemaWithLogicalType("ts", PhysicalType.INT64,
                new LogicalType.TimestampType(true, LogicalType.TimeUnit.MICROS));
        assertThat(FilterPredicateResolver.resolve(
                FilterPredicate.in("ts", Instant.ofEpochSecond(1, 2_000), Instant.ofEpochSecond(-1)), micros))
                .isInstanceOfSatisfying(ResolvedPredicate.LongInPredicate.class,
                        p -> assertThat(p.values()).containsExactly(1_000_002L, -1_000_000L));

        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.in("ts", Instant.EPOCH, Instant.ofEpochSecond(0, 1)), micros))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'ts' holds a whole number of microseconds within the INT64 range; "
                        + "the equality literal 1970-01-01T00:00:00.000000001Z is not a value it can hold");

        FileSchema local = schemaWithLogicalType("ts", PhysicalType.INT64,
                new LogicalType.TimestampType(false, LogicalType.TimeUnit.MICROS));
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(FilterPredicate.in("ts", Instant.EPOCH), local))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'ts' is a local-wall-clock TIMESTAMP (isAdjustedToUTC=false),"
                        + " which takes LocalDateTime and long literals, not an Instant");
    }

    @Test
    void resolveInstantInOnInt96() {
        FileSchema schema = schemaWithLogicalType("ts", PhysicalType.INT96, null);
        Instant instant = Instant.ofEpochSecond(1_700_000_000L, 5);
        ResolvedPredicate eq = FilterPredicateResolver.resolve(FilterPredicate.eq("ts", instant), schema);
        assertThat(FilterPredicateResolver.resolve(FilterPredicate.in("ts", instant), schema))
                .isInstanceOfSatisfying(ResolvedPredicate.BinaryInPredicate.class, p -> {
                    assertThat(p.values()).isDeepEqualTo(new byte[][] { ((ResolvedPredicate.BinaryPredicate) eq).value() });
                    assertThat(p.comparison()).isEqualTo(Comparison.INT96_INSTANT);
                });

        assertThatThrownBy(() -> FilterPredicateResolver.resolve(FilterPredicate.in("ts", Instant.MAX), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'ts' holds an instant within the range an INT96 encodes; "
                        + "the equality literal +1000000000-12-31T23:59:59.999999999Z is not a value it can hold");
    }

    @Test
    void resolveLocalDateTimeIn() {
        FileSchema millis = schemaWithLogicalType("ts", PhysicalType.INT64,
                new LogicalType.TimestampType(false, LogicalType.TimeUnit.MILLIS));
        assertThat(FilterPredicateResolver.resolve(
                FilterPredicate.in("ts", LocalDateTime.ofEpochSecond(2, 3_000_000, ZoneOffset.UTC)), millis))
                .isInstanceOfSatisfying(ResolvedPredicate.LongInPredicate.class,
                        p -> assertThat(p.values()).containsExactly(2_003L));

        FileSchema utc = schemaWithLogicalType("ts", PhysicalType.INT64,
                new LogicalType.TimestampType(true, LogicalType.TimeUnit.MILLIS));
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.in("ts", LocalDateTime.of(2026, 1, 1, 0, 0)), utc))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'ts' is a UTC-adjusted TIMESTAMP (isAdjustedToUTC=true),"
                        + " which takes Instant and long literals, not a LocalDateTime");

        FileSchema int96 = schemaWithLogicalType("ts", PhysicalType.INT96, null);
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.in("ts", LocalDateTime.of(2026, 1, 1, 0, 0)), int96))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'ts' is a legacy INT96 TIMESTAMP (no isAdjustedToUTC field),"
                        + " which takes Instant and byte[] literals, not a LocalDateTime");
    }

    @Test
    void resolveTimeIn() {
        FileSchema millis = schemaWithLogicalType("t", PhysicalType.INT32,
                new LogicalType.TimeType(false, LogicalType.TimeUnit.MILLIS));
        assertThat(FilterPredicateResolver.resolve(FilterPredicate.in("t", LocalTime.of(0, 0, 1)), millis))
                .isInstanceOfSatisfying(ResolvedPredicate.IntInPredicate.class,
                        p -> assertThat(p.values()).containsExactly(1_000));

        FileSchema nanos = schemaWithLogicalType("t", PhysicalType.INT64,
                new LogicalType.TimeType(false, LogicalType.TimeUnit.NANOS));
        assertThat(FilterPredicateResolver.resolve(FilterPredicate.in("t", LocalTime.ofNanoOfDay(7)), nanos))
                .isInstanceOfSatisfying(ResolvedPredicate.LongInPredicate.class,
                        p -> assertThat(p.values()).containsExactly(7L));

        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.in("t", LocalTime.ofNanoOfDay(1)), millis))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 't' holds a whole number of milliseconds; "
                        + "the equality literal 00:00:00.000000001 is not a value it can hold");
    }

    @Test
    void resolveDecimalInOnEveryCarrier() {
        LogicalType.DecimalType decimal = new LogicalType.DecimalType(9, 2);
        BigDecimal[] probes = { new BigDecimal("1.25"), new BigDecimal("-3") };

        assertThat(FilterPredicateResolver.resolve(FilterPredicate.in("c", probes),
                schemaWithLogicalType("c", PhysicalType.INT32, decimal)))
                .isInstanceOfSatisfying(ResolvedPredicate.IntInPredicate.class,
                        p -> assertThat(p.values()).containsExactly(125, -300));
        assertThat(FilterPredicateResolver.resolve(FilterPredicate.in("c", probes),
                schemaWithLogicalType("c", PhysicalType.INT64, decimal)))
                .isInstanceOfSatisfying(ResolvedPredicate.LongInPredicate.class,
                        p -> assertThat(p.values()).containsExactly(125L, -300L));
        assertThat(FilterPredicateResolver.resolve(FilterPredicate.in("c", probes),
                schemaWithLogicalType("c", PhysicalType.FIXED_LEN_BYTE_ARRAY, 4, decimal)))
                .isInstanceOfSatisfying(ResolvedPredicate.BinaryInPredicate.class, p -> {
                    assertThat(p.values()).isDeepEqualTo(new byte[][] { { 0, 0, 0, 125 },
                            { (byte) 0xFF, (byte) 0xFF, (byte) 0xFE, (byte) 0xD4 } });
                    assertThat(p.comparison()).isEqualTo(Comparison.FIXED_DECIMAL);
                });
        assertThat(FilterPredicateResolver.resolve(FilterPredicate.in("c", probes),
                schemaWithLogicalType("c", PhysicalType.BYTE_ARRAY, decimal)))
                .isInstanceOfSatisfying(ResolvedPredicate.BinaryInPredicate.class, p -> {
                    assertThat(p.values()).isDeepEqualTo(new byte[][] { { 125 }, { (byte) 0xFE, (byte) 0xD4 } });
                    assertThat(p.comparison()).isEqualTo(Comparison.VARIABLE_DECIMAL);
                });

        assertThatThrownBy(() -> FilterPredicateResolver.resolve(FilterPredicate.in("c", new BigDecimal("1.255")),
                schemaWithLogicalType("c", PhysicalType.BYTE_ARRAY, decimal)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'c' holds a DECIMAL of scale 2; "
                        + "the equality literal 1.255 is not a value it can hold");
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(FilterPredicate.in("c", new BigDecimal("1E+10")),
                schemaWithLogicalType("c", PhysicalType.FIXED_LEN_BYTE_ARRAY, 4, decimal)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'c' holds a DECIMAL of scale 2 within 4 bytes; "
                        + "the equality literal 10000000000 is not a value it can hold");
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(FilterPredicate.in("c", BigDecimal.ONE),
                schemaWithLogicalType("c", PhysicalType.INT32, null)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'c' is an unannotated INT32, which takes int literals, not a BigDecimal");
    }

    @Test
    void resolveUuidIn() {
        FileSchema schema = schemaWithLogicalType("u", PhysicalType.FIXED_LEN_BYTE_ARRAY, 16, new LogicalType.UuidType());
        UUID uuid = new UUID(0x0102030405060708L, 0x090A0B0C0D0E0F10L);
        assertThat(FilterPredicateResolver.resolve(FilterPredicate.in("u", uuid), schema))
                .isInstanceOfSatisfying(ResolvedPredicate.BinaryInPredicate.class, p -> {
                    assertThat(p.values()).isDeepEqualTo(new byte[][] {
                            { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 } });
                    assertThat(p.comparison()).isEqualTo(Comparison.BYTE_STRING);
                });

        FileSchema plain = schemaWithLogicalType("u", PhysicalType.FIXED_LEN_BYTE_ARRAY, 16, null);
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(FilterPredicate.in("u", uuid), plain))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'u' is an unannotated FIXED_LEN_BYTE_ARRAY, which takes byte[] literals, not a UUID");
    }

    @Test
    void resolveIntervalIn() {
        FileSchema schema = schemaWithLogicalType("iv", PhysicalType.FIXED_LEN_BYTE_ARRAY, 12,
                new LogicalType.IntervalType());
        assertThat(FilterPredicateResolver.resolve(FilterPredicate.in("iv", new PqInterval(1, 2, 3)), schema))
                .isInstanceOfSatisfying(ResolvedPredicate.BinaryInPredicate.class, p -> {
                    assertThat(p.values()).isDeepEqualTo(new byte[][] { { 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0 } });
                    assertThat(p.comparison()).isEqualTo(Comparison.BYTE_STRING);
                });

        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.in("iv", new PqInterval(1, 2, 3), new PqInterval(0, 0, 4294967296L)), schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Column 'iv' holds an interval whose months, days and milliseconds are each within "
                        + "[0, 4294967295]; the equality literal PqInterval[months=0, days=0, milliseconds=4294967296]"
                        + " is not a value it can hold");
    }

    @Test
    void negatedSetFormsBecomeConjunctionsOfNotEq() {
        FileSchema floatSchema = schemaWithLogicalType("c", PhysicalType.FLOAT, null);
        assertThat(FilterPredicateResolver.resolve(FilterPredicate.not(FilterPredicate.in("c", 0.5f, Float.NaN)),
                floatSchema, List.of(ColumnOrder.IEEE754_TOTAL_ORDER)))
                .isEqualTo(new ResolvedPredicate.And(List.of(
                        new ResolvedPredicate.FloatPredicate(0, FilterPredicate.Operator.NOT_EQ, 0.5f, true),
                        new ResolvedPredicate.FloatPredicate(0, FilterPredicate.Operator.NOT_EQ, Float.NaN, true))));

        FileSchema doubleSchema = schemaWithLogicalType("c", PhysicalType.DOUBLE, null);
        assertThat(FilterPredicateResolver.resolve(FilterPredicate.not(FilterPredicate.in("c", 0.5, Double.NaN)),
                doubleSchema, List.of(ColumnOrder.IEEE754_TOTAL_ORDER)))
                .isEqualTo(new ResolvedPredicate.And(List.of(
                        new ResolvedPredicate.DoublePredicate(0, FilterPredicate.Operator.NOT_EQ, 0.5, true),
                        new ResolvedPredicate.DoublePredicate(0, FilterPredicate.Operator.NOT_EQ, Double.NaN, true))));

        FileSchema float16Schema = schemaWithLogicalType("c", PhysicalType.FIXED_LEN_BYTE_ARRAY, 2,
                new LogicalType.Float16Type());
        assertThat(FilterPredicateResolver.resolve(FilterPredicate.not(FilterPredicate.in("c", 0.5f)), float16Schema))
                .isEqualTo(new ResolvedPredicate.And(List.of(
                        new ResolvedPredicate.Float16Predicate(0, FilterPredicate.Operator.NOT_EQ, 0.5f, false))));

        FileSchema dateSchema = schemaWithLogicalType("c", PhysicalType.INT32, new LogicalType.DateType());
        assertThat(FilterPredicateResolver.resolve(
                FilterPredicate.not(FilterPredicate.in("c", LocalDate.ofEpochDay(4))), dateSchema))
                .isEqualTo(new ResolvedPredicate.And(List.of(
                        new ResolvedPredicate.IntPredicate(0, FilterPredicate.Operator.NOT_EQ, 4))));
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
                FilterPredicate.in("rep", 1.0f, 2.0f),
                FilterPredicate.in("rep", 1.0, 2.0),
                FilterPredicate.in("rep", "a", "b"),
                FilterPredicate.in("rep", LocalDate.of(2026, 1, 1)),
                FilterPredicate.in("rep", Instant.EPOCH),
                FilterPredicate.in("rep", LocalDateTime.of(2026, 1, 1, 0, 0)),
                FilterPredicate.in("rep", LocalTime.NOON),
                FilterPredicate.in("rep", BigDecimal.ONE),
                FilterPredicate.in("rep", UUID.randomUUID()),
                FilterPredicate.in("rep", new PqInterval(0, 0, 0)),
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
                .hasMessage(notTaken("an unannotated INT32", "int", "a long"));
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(FilterPredicate.eq("c", 1.0), int32Schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(notTaken("an unannotated INT32", "int", "a double"));
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(FilterPredicate.eq("c", true), int32Schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(notTaken("an unannotated INT32", "int", "a boolean"));
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(FilterPredicate.eq("c", "str"), int32Schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(notTaken("an unannotated INT32", "int", "a String"));
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(FilterPredicate.in("c", 1, 2), int64Schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(notTaken("an unannotated INT64", "long", "an int"));
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(FilterPredicate.in("c", 1L, 2L), int32Schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(notTaken("an unannotated INT32", "int", "a long"));
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(FilterPredicate.in("c", "a"), int32Schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(notTaken("an unannotated INT32", "int", "a String"));

        FileSchema dateSchemaWrong = schemaWithLogicalType("c", PhysicalType.INT64, new LogicalType.DateType());
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(FilterPredicate.eq("c", LocalDate.of(2026, 1, 1)), dateSchemaWrong))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(notTaken("an unannotated INT64", "long", "a LocalDate"));

        // A TIMESTAMP annotation on an INT32 column, and a TIME annotation on the width its unit
        // is not stored in, are dropped from the schema, so the column arrives with no logical
        // type and the unit lookup rejects it before the physical type is compared.
        FileSchema tsSchemaWrong = schemaWithLogicalType("c", PhysicalType.INT32,
                new LogicalType.TimestampType(false, LogicalType.TimeUnit.MILLIS));
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(FilterPredicate.eq("c", Instant.EPOCH), tsSchemaWrong))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(notTaken("an unannotated INT32", "int", "an Instant"));

        FileSchema timeMillisWrong = schemaWithLogicalType("c", PhysicalType.INT64,
                new LogicalType.TimeType(false, LogicalType.TimeUnit.MILLIS));
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(FilterPredicate.eq("c", LocalTime.NOON), timeMillisWrong))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(notTaken("an unannotated INT64", "long", "a LocalTime"));

        FileSchema timeMicrosWrong = schemaWithLogicalType("c", PhysicalType.INT32,
                new LogicalType.TimeType(false, LogicalType.TimeUnit.MICROS));
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(FilterPredicate.eq("c", LocalTime.NOON), timeMicrosWrong))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(notTaken("an unannotated INT32", "int", "a LocalTime"));

        FileSchema uuidWrong = schemaWithLogicalType("c", PhysicalType.INT32,
                new LogicalType.UuidType());
        assertThatThrownBy(() -> FilterPredicateResolver.resolve(
                FilterPredicate.eq("c", UUID.randomUUID()), uuidWrong))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(notTaken("an unannotated INT32", "int", "a UUID"));
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
        assertThat(FilterPredicateResolver.resolve(FilterPredicate.in("c", "hé"), schema))
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
                FilterPredicate.in("c", "a"), schema))
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
                        notText("annotated INTERVAL", "PqInterval and byte[]")),
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
        return notTaken(description, literals, "a String");
    }

    /// The message a literal of a type column `c` does not take raises, naming what the column
    /// is, the literals it takes and the literal given.
    private static String notTaken(String description, String literals, String literal) {
        return "Column 'c' is " + description + ", which takes " + literals + " literals, not " + literal;
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

    private static FileSchema schemaWithConvertedType(String columnName, PhysicalType type,
            ConvertedType convertedType) {
        SchemaElement root = SchemaElement.root("root", 1);
        SchemaElement col = new SchemaElement(columnName, type, null, RepetitionType.REQUIRED,
                null, convertedType, null, null, null, null);
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
