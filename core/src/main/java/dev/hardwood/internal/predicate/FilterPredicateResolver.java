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
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

import dev.hardwood.internal.conversion.Flba12Timestamps;
import dev.hardwood.internal.conversion.LogicalTypeConverter;
import dev.hardwood.internal.predicate.ResolvedPredicate.BinaryPredicate.Comparison;
import dev.hardwood.internal.reader.TimestampAccessorKind;
import dev.hardwood.internal.schema.FixedWidthValidator;
import dev.hardwood.internal.schema.SchemaPathResolver;
import dev.hardwood.internal.schema.TextColumns;
import dev.hardwood.metadata.ColumnOrder;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.FilterPredicate.And;
import dev.hardwood.reader.FilterPredicate.BinaryColumnPredicate;
import dev.hardwood.reader.FilterPredicate.BinaryInPredicate;
import dev.hardwood.reader.FilterPredicate.BooleanColumnPredicate;
import dev.hardwood.reader.FilterPredicate.DateColumnPredicate;
import dev.hardwood.reader.FilterPredicate.DecimalColumnPredicate;
import dev.hardwood.reader.FilterPredicate.DoubleColumnPredicate;
import dev.hardwood.reader.FilterPredicate.DoubleInPredicate;
import dev.hardwood.reader.FilterPredicate.FloatColumnPredicate;
import dev.hardwood.reader.FilterPredicate.InstantColumnPredicate;
import dev.hardwood.reader.FilterPredicate.IntColumnPredicate;
import dev.hardwood.reader.FilterPredicate.IntInPredicate;
import dev.hardwood.reader.FilterPredicate.IntersectsPredicate;
import dev.hardwood.reader.FilterPredicate.IntervalColumnPredicate;
import dev.hardwood.reader.FilterPredicate.LocalDateTimeColumnPredicate;
import dev.hardwood.reader.FilterPredicate.LongColumnPredicate;
import dev.hardwood.reader.FilterPredicate.LongInPredicate;
import dev.hardwood.reader.FilterPredicate.Not;
import dev.hardwood.reader.FilterPredicate.Operator;
import dev.hardwood.reader.FilterPredicate.Or;
import dev.hardwood.reader.FilterPredicate.TimeColumnPredicate;
import dev.hardwood.row.PqInterval;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;

/// Resolves user-facing [FilterPredicate] into the internal [ResolvedPredicate] tree.
///
/// This resolution runs once per reader creation, performing:
/// - Logical-to-physical value conversion (Date to epoch days, Instant to long, etc.)
/// - Column name to column index resolution
/// - Physical type validation
///
/// After resolution, evaluators work exclusively with [ResolvedPredicate] and never
/// need to repeat column lookups or type checks.
public class FilterPredicateResolver {

    /// A `FLOAT16` value is two little-endian bytes of an IEEE half.
    private static final int FLOAT16_BYTES = 2;

    /// An `INTERVAL` value is three unsigned little-endian 32-bit components.
    private static final int INTERVAL_BYTES = 12;

    /// The largest value one of those components holds.
    private static final long UNSIGNED_INT_MAX = 0xFFFF_FFFFL;

    private static final BigInteger INT32_MIN = BigInteger.valueOf(Integer.MIN_VALUE);
    private static final BigInteger INT32_MAX = BigInteger.valueOf(Integer.MAX_VALUE);
    private static final BigInteger INT64_MIN = BigInteger.valueOf(Long.MIN_VALUE);
    private static final BigInteger INT64_MAX = BigInteger.valueOf(Long.MAX_VALUE);

    /// The counts a `FIXED_LEN_BYTE_ARRAY(12)` `TIMESTAMP` holds.
    private static final CarriedLiteral.Range FIXED_TIMESTAMP_RANGE = CarriedLiteral.Range.ofBytes(Flba12Timestamps.WIDTH);

    private static final BigInteger NANOS_PER_MILLI = BigInteger.valueOf(1_000_000L);
    private static final BigInteger NANOS_PER_MICRO = BigInteger.valueOf(1_000L);
    private static final BigInteger NANOS_PER_SECOND = BigInteger.valueOf(1_000_000_000L);
    private static final BigInteger NANOS_PER_DAY = BigInteger.valueOf(86_400_000_000_000L);

    /// The earliest and latest instants an `INT96` encodes, in nanoseconds since Julian day 0:
    /// the extreme day with the extreme nanoseconds of the day, which the format does not bound
    /// by one day.
    private static final BigInteger INT96_MIN = INT32_MIN.multiply(NANOS_PER_DAY).add(INT64_MIN);
    private static final BigInteger INT96_MAX = INT32_MAX.multiply(NANOS_PER_DAY).add(INT64_MAX);

    private static final HexFormat HEX = HexFormat.of();

    private static final String EPOCH_DAY_HOLDS = "an epoch day within the INT32 range";
    private static final String INT96_HOLDS = "an instant within the range an INT96 encodes";

    /// The leaf a measured literal resolves into, given an operator and a value the column holds.
    @FunctionalInterface
    private interface CarriedLeaf {
        ResolvedPredicate of(FilterPredicate.Operator op, BigInteger value);
    }

    /// Resolves a [FilterPredicate] tree without column-order information. Float/double leaves are
    /// treated as type-defined, so statistics pruning widens `±0` bounds (the conservative default).
    ///
    /// @param predicate the user-facing predicate tree
    /// @param schema the file schema for column resolution and type validation
    /// @return a fully resolved predicate tree ready for evaluation
    public static ResolvedPredicate resolve(FilterPredicate predicate, FileSchema schema) {
        return resolve(predicate, schema, List.of());
    }

    /// Resolves a [FilterPredicate] tree into a [ResolvedPredicate] tree.
    ///
    /// @param predicate the user-facing predicate tree
    /// @param schema the file schema for column resolution and type validation
    /// @param columnOrders the file's decoded `column_orders` (empty if absent), used to mark
    ///        float/double leaves that use the IEEE 754 total order so statistics pruning can skip
    ///        the `±0` widening for them
    /// @return a fully resolved predicate tree ready for evaluation
    public static ResolvedPredicate resolve(FilterPredicate predicate, FileSchema schema,
            List<ColumnOrder> columnOrders) {
        return switch (predicate) {
            case DateColumnPredicate p -> {
                ColumnSchema cs = dateColumn(p.column(), leafColumn(p.column(), schema, p.op()));
                yield carried(p.column(), cs.columnIndex(), p.op(), epochDay(p.value()), EPOCH_DAY_HOLDS,
                        p.value().toString(),
                        (op, day) -> new ResolvedPredicate.IntPredicate(cs.columnIndex(), op, day.intValueExact()));
            }
            case FilterPredicate.DateInPredicate p -> {
                ColumnSchema cs = dateColumn(p.column(), leafColumn(p.column(), schema));
                yield new ResolvedPredicate.IntInPredicate(cs.columnIndex(), intsOf(
                        held(p.column(), p.values(), FilterPredicateResolver::epochDay, EPOCH_DAY_HOLDS,
                                LocalDate::toString)));
            }
            case InstantColumnPredicate p -> {
                ColumnSchema cs = leafColumn(p.column(), schema, p.op());
                if (isLegacyInt96(cs)) {
                    yield int96Instant(p.column(), cs, p.op(), p.value());
                }
                LogicalType.TimeUnit unit = timestampUnit(p.column(), cs, true);
                yield timestamp(p.column(), cs, p.op(), unit, nanosSinceEpoch(p.value()), p.value().toString());
            }
            case FilterPredicate.InstantInPredicate p -> {
                ColumnSchema cs = leafColumn(p.column(), schema);
                if (isLegacyInt96(cs)) {
                    yield new ResolvedPredicate.BinaryInPredicate(cs.columnIndex(), bytesOf(
                            held(p.column(), p.values(), FilterPredicateResolver::int96Instant, INT96_HOLDS,
                                    Instant::toString),
                            FilterPredicateResolver::int96Bytes), Comparison.INT96_INSTANT);
                }
                LogicalType.TimeUnit unit = timestampUnit(p.column(), cs, true);
                yield timestampIn(p.column(), cs, unit, p.values(), FilterPredicateResolver::nanosSinceEpoch,
                        Instant::toString);
            }
            case LocalDateTimeColumnPredicate p -> {
                ColumnSchema cs = leafColumn(p.column(), schema, p.op());
                LogicalType.TimeUnit unit = timestampUnit(p.column(), cs, false);
                yield timestamp(p.column(), cs, p.op(), unit, wallClockNanos(p.value()), p.value().toString());
            }
            case FilterPredicate.LocalDateTimeInPredicate p -> {
                ColumnSchema cs = leafColumn(p.column(), schema);
                LogicalType.TimeUnit unit = timestampUnit(p.column(), cs, false);
                yield timestampIn(p.column(), cs, unit, p.values(), FilterPredicateResolver::wallClockNanos,
                        LocalDateTime::toString);
            }
            case TimeColumnPredicate p -> {
                ColumnSchema cs = leafColumn(p.column(), schema, p.op());
                LogicalType.TimeUnit unit = timeUnit(p.column(), cs);
                CarriedLiteral literal = timeOfDay(p.value(), unit);
                if (unit == LogicalType.TimeUnit.MILLIS) {
                    yield carried(p.column(), cs.columnIndex(), p.op(), literal, timeHolds(unit), p.value().toString(),
                            (op, value) -> new ResolvedPredicate.IntPredicate(cs.columnIndex(), op,
                                    value.intValueExact()));
                }
                yield carried(p.column(), cs.columnIndex(), p.op(), literal, timeHolds(unit), p.value().toString(),
                        (op, value) -> new ResolvedPredicate.LongPredicate(cs.columnIndex(), op,
                                value.longValueExact()));
            }
            case FilterPredicate.TimeInPredicate p -> {
                ColumnSchema cs = leafColumn(p.column(), schema);
                LogicalType.TimeUnit unit = timeUnit(p.column(), cs);
                BigInteger[] probes = held(p.column(), p.values(), value -> timeOfDay(value, unit), timeHolds(unit),
                        LocalTime::toString);
                yield unit == LogicalType.TimeUnit.MILLIS
                        ? new ResolvedPredicate.IntInPredicate(cs.columnIndex(), intsOf(probes))
                        : new ResolvedPredicate.LongInPredicate(cs.columnIndex(), longsOf(probes));
            }
            case FilterPredicate.DecimalInPredicate p -> {
                ColumnSchema cs = leafColumn(p.column(), schema);
                yield decimalIn(p.column(), cs, p.values());
            }
            case DecimalColumnPredicate p -> {
                ColumnSchema cs = leafColumn(p.column(), schema, p.op());
                LogicalType.DecimalType dt = getDecimalType(p.column(), cs);
                BigInteger floor = unscaled(p.value(), dt, RoundingMode.FLOOR);
                BigInteger ceiling = unscaled(p.value(), dt, RoundingMode.CEILING);
                String scale = scaleHolds(dt);
                PhysicalType physicalType = cs.type();
                if (physicalType == PhysicalType.INT32) {
                    yield carried(p.column(), cs.columnIndex(), p.op(),
                            atScale(p.value(), dt, INT32_MIN, INT32_MAX),
                            scale + " within the INT32 range", p.value().toPlainString(),
                            (op, value) -> new ResolvedPredicate.IntPredicate(cs.columnIndex(), op,
                                    value.intValueExact()));
                }
                else if (physicalType == PhysicalType.INT64) {
                    yield carried(p.column(), cs.columnIndex(), p.op(),
                            atScale(p.value(), dt, INT64_MIN, INT64_MAX),
                            scale + " within the INT64 range", p.value().toPlainString(),
                            (op, value) -> new ResolvedPredicate.LongPredicate(cs.columnIndex(), op,
                                    value.longValueExact()));
                }
                else if (physicalType == PhysicalType.FIXED_LEN_BYTE_ARRAY) {
                    yield fixedDecimal(p.column(), cs, p.op(), floor, ceiling, scale,
                            p.value().toPlainString());
                }
                else {
                    validateType(p.column(), PhysicalType.BYTE_ARRAY, cs, "a BigDecimal");
                    // A BYTE_ARRAY decimal should store each value in the fewest bytes that hold
                    // it — `should`, not `must`, so a writer may pad and two byte strings of
                    // different lengths may be the same number. The literal takes the minimal
                    // form, which is what a conforming writer produces, and VARIABLE_DECIMAL
                    // keeps equality from ever riding on that encoding. Any unscaled value fits,
                    // so only the scale can put a literal beyond the column.
                    yield carried(p.column(), cs.columnIndex(), p.op(),
                            CarriedLiteral.unbounded(floor, ceiling), scale, p.value().toPlainString(),
                            (op, value) -> new ResolvedPredicate.BinaryPredicate(cs.columnIndex(), op,
                                    value.toByteArray(), Comparison.VARIABLE_DECIMAL));
                }
            }
            case IntColumnPredicate p -> {
                ColumnSchema cs = leafColumn(p.column(), schema, p.op());
                validateType(p.column(), PhysicalType.INT32, cs, "an int");
                yield ordersUnsigned(cs)
                        ? new ResolvedPredicate.UnsignedIntPredicate(cs.columnIndex(), p.op(), p.value())
                        : new ResolvedPredicate.IntPredicate(cs.columnIndex(), p.op(), p.value());
            }
            case LongColumnPredicate p -> {
                ColumnSchema cs = leafColumn(p.column(), schema, p.op());
                validateType(p.column(), PhysicalType.INT64, cs, "a long");
                yield ordersUnsigned(cs)
                        ? new ResolvedPredicate.UnsignedLongPredicate(cs.columnIndex(), p.op(), p.value())
                        : new ResolvedPredicate.LongPredicate(cs.columnIndex(), p.op(), p.value());
            }
            case FilterPredicate.FloatInPredicate p -> {
                ColumnSchema cs = leafColumn(p.column(), schema);
                if (cs.type() == PhysicalType.FIXED_LEN_BYTE_ARRAY
                        && cs.logicalType() instanceof LogicalType.Float16Type) {
                    for (float value : p.values()) {
                        rejectUnholdableHalf(p.column(), Operator.EQ, value);
                    }
                    yield new ResolvedPredicate.Float16InPredicate(cs.columnIndex(), p.values(),
                            isIeee754TotalOrder(cs.columnIndex(), columnOrders));
                }
                validateType(p.column(), PhysicalType.FLOAT, cs, "a float");
                yield new ResolvedPredicate.FloatInPredicate(cs.columnIndex(), p.values(),
                        isIeee754TotalOrder(cs.columnIndex(), columnOrders));
            }
            case FloatColumnPredicate p -> {
                ColumnSchema cs = leafColumn(p.column(), schema, p.op());
                if (cs.type() == PhysicalType.FIXED_LEN_BYTE_ARRAY
                        && cs.logicalType() instanceof LogicalType.Float16Type) {
                    rejectUnholdableHalf(p.column(), p.op(), p.value());
                    yield new ResolvedPredicate.Float16Predicate(cs.columnIndex(), p.op(), p.value(),
                            isIeee754TotalOrder(cs.columnIndex(), columnOrders));
                }
                validateType(p.column(), PhysicalType.FLOAT, cs, "a float");
                yield new ResolvedPredicate.FloatPredicate(cs.columnIndex(), p.op(), p.value(),
                        isIeee754TotalOrder(cs.columnIndex(), columnOrders));
            }
            case DoubleColumnPredicate p -> {
                ColumnSchema cs = leafColumn(p.column(), schema, p.op());
                validateType(p.column(), PhysicalType.DOUBLE, cs, "a double");
                yield new ResolvedPredicate.DoublePredicate(cs.columnIndex(), p.op(), p.value(),
                        isIeee754TotalOrder(cs.columnIndex(), columnOrders));
            }
            case BooleanColumnPredicate p -> {
                ColumnSchema cs = leafColumn(p.column(), schema, p.op());
                validateType(p.column(), PhysicalType.BOOLEAN, cs, "a boolean");
                yield booleanLeaf(cs.columnIndex(), p.op(), p.value());
            }
            case BinaryColumnPredicate p -> {
                ColumnSchema cs = byteColumn(p.column(), leafColumn(p.column(), schema, p.op()));
                String orderingLiteral = orderingLiteral(cs);
                if (orderingLiteral == null) {
                    rejectUnholdableWidth(p.column(), cs, p.op(), p.value());
                    yield new ResolvedPredicate.BinaryPredicate(cs.columnIndex(), p.op(), p.value(),
                            Comparison.BYTE_STRING);
                }
                if (!isEquality(p.op())) {
                    throw notByteOrdered(p.column(), cs, orderingLiteral);
                }
                ResolvedPredicate equal = storedBytesEqual(p.column(), cs, p.value(), columnOrders);
                yield p.op() == Operator.EQ ? equal : ResolvedPredicate.negate(equal);
            }
            case FilterPredicate.StringColumnPredicate p -> {
                ColumnSchema cs = leafColumn(p.column(), schema, p.op());
                validateType(p.column(), PhysicalType.BYTE_ARRAY, cs, "a String");
                requireTextColumn(p.column(), cs);
                yield new ResolvedPredicate.BinaryPredicate(cs.columnIndex(), p.op(),
                        p.value().getBytes(StandardCharsets.UTF_8), Comparison.BYTE_STRING);
            }
            case FilterPredicate.StringInPredicate p -> {
                ColumnSchema cs = leafColumn(p.column(), schema);
                validateType(p.column(), PhysicalType.BYTE_ARRAY, cs, "a String");
                requireTextColumn(p.column(), cs);
                byte[][] probes = new byte[p.values().length][];
                for (int i = 0; i < probes.length; i++) {
                    probes[i] = p.values()[i].getBytes(StandardCharsets.UTF_8);
                }
                yield new ResolvedPredicate.BinaryInPredicate(cs.columnIndex(), probes,
                        Comparison.BYTE_STRING);
            }
            case FilterPredicate.UUIDColumnPredicate p -> {
                ColumnSchema cs = annotatedFixedWidth(p.column(), LogicalType.UuidType.class, "a UUID",
                        leafColumn(p.column(), schema, p.op()));
                yield new ResolvedPredicate.BinaryPredicate(cs.columnIndex(), p.op(), p.value(),
                        Comparison.BYTE_STRING);
            }
            case FilterPredicate.UUIDInPredicate p -> {
                ColumnSchema cs = annotatedFixedWidth(p.column(), LogicalType.UuidType.class, "a UUID",
                        leafColumn(p.column(), schema));
                byte[][] probes = new byte[p.values().size()][];
                for (int i = 0; i < probes.length; i++) {
                    probes[i] = uuidBytes(p.values().get(i));
                }
                yield new ResolvedPredicate.BinaryInPredicate(cs.columnIndex(), probes, Comparison.BYTE_STRING);
            }
            case IntervalColumnPredicate p -> {
                ColumnSchema cs = annotatedFixedWidth(p.column(), LogicalType.IntervalType.class, "a PqInterval",
                        leafColumn(p.column(), schema, p.op()));
                yield new ResolvedPredicate.BinaryPredicate(cs.columnIndex(), p.op(),
                        intervalBytes(p.column(), p.value()), Comparison.BYTE_STRING);
            }
            case FilterPredicate.IntervalInPredicate p -> {
                ColumnSchema cs = annotatedFixedWidth(p.column(), LogicalType.IntervalType.class, "a PqInterval",
                        leafColumn(p.column(), schema));
                byte[][] probes = new byte[p.values().size()][];
                for (int i = 0; i < probes.length; i++) {
                    probes[i] = intervalBytes(p.column(), p.values().get(i));
                }
                yield new ResolvedPredicate.BinaryInPredicate(cs.columnIndex(), probes, Comparison.BYTE_STRING);
            }
            case IntInPredicate p -> {
                ColumnSchema cs = leafColumn(p.column(), schema);
                validateType(p.column(), PhysicalType.INT32, cs, "an int");
                yield ordersUnsigned(cs)
                        ? new ResolvedPredicate.UnsignedIntInPredicate(cs.columnIndex(), p.values())
                        : new ResolvedPredicate.IntInPredicate(cs.columnIndex(), p.values());
            }
            case LongInPredicate p -> {
                ColumnSchema cs = leafColumn(p.column(), schema);
                validateType(p.column(), PhysicalType.INT64, cs, "a long");
                yield ordersUnsigned(cs)
                        ? new ResolvedPredicate.UnsignedLongInPredicate(cs.columnIndex(), p.values())
                        : new ResolvedPredicate.LongInPredicate(cs.columnIndex(), p.values());
            }
            case BinaryInPredicate p -> {
                ColumnSchema cs = byteColumn(p.column(), leafColumn(p.column(), schema));
                if (orderingLiteral(cs) == null) {
                    for (byte[] value : p.values()) {
                        rejectUnholdableWidth(p.column(), cs, Operator.EQ, value);
                    }
                    yield new ResolvedPredicate.BinaryInPredicate(cs.columnIndex(), p.values(),
                            Comparison.BYTE_STRING);
                }
                yield storedBytesMember(p.column(), cs, p.values(), columnOrders);
            }
            case DoubleInPredicate p -> {
                ColumnSchema cs = leafColumn(p.column(), schema);
                validateType(p.column(), PhysicalType.DOUBLE, cs, "a double");
                yield new ResolvedPredicate.DoubleInPredicate(cs.columnIndex(), p.values(),
                        isIeee754TotalOrder(cs.columnIndex(), columnOrders));
            }
            case FilterPredicate.IsNullPredicate p -> {
                NullTarget target = resolveNullTarget(p.column(), schema);
                yield new ResolvedPredicate.IsNullPredicate(target.columnIndex(), target.definitionLevel(),
                        target.leafDefinitionLevel());
            }
            case FilterPredicate.IsNotNullPredicate p -> {
                NullTarget target = resolveNullTarget(p.column(), schema);
                yield new ResolvedPredicate.IsNotNullPredicate(target.columnIndex(), target.definitionLevel(),
                        target.leafDefinitionLevel());
            }
            case And a -> new ResolvedPredicate.And(a.filters().stream()
                    .map(f -> resolve(f, schema, columnOrders))
                    .toList());
            case Or o -> new ResolvedPredicate.Or(o.filters().stream()
                    .map(f -> resolve(f, schema, columnOrders))
                    .toList());
            case Not n -> {
                ResolvedPredicate resolvedDelegate = resolve(n.delegate(), schema, columnOrders);
                rejectNegatedIntersects(n.delegate());
                yield ResolvedPredicate.negate(resolvedDelegate);
            }
            case IntersectsPredicate p -> {
                ColumnSchema cs = leafColumn(p.column(), schema);
                if (!(cs.logicalType() instanceof LogicalType.GeometryType) &&
                        !(cs.logicalType() instanceof LogicalType.GeographyType)) {
                    throw new IllegalArgumentException("Column '" + p.column() + "' is "
                            + ColumnLiterals.describe(cs) + "; intersects takes a GEOMETRY or GEOGRAPHY column");
                }
                yield new ResolvedPredicate.GeospatialPredicate(cs.columnIndex(),
                        p.xmin(), p.ymin(), p.xmax(), p.ymax());
            }
        };
    }

    /// `true` when the leaf at `columnIndex` declares the IEEE 754 total order, whose signed-zero
    /// statistics are exact. Any other case — type-defined order, an absent (empty) `column_orders`,
    /// an out-of-range index, or an unrecognized order — yields `false`, so pruning widens `±0`.
    private static boolean isIeee754TotalOrder(int columnIndex, List<ColumnOrder> columnOrders) {
        return columnIndex < columnOrders.size()
                && columnOrders.get(columnIndex) == ColumnOrder.IEEE754_TOTAL_ORDER;
    }

    // ==================== Column resolution ====================

    /// The leaf column a null predicate is answered from, the definition level at or above which
    /// the node the user named is present, and that leaf's own maximum definition level. The last
    /// two are equal when the user named the leaf itself.
    private record NullTarget(int columnIndex, int definitionLevel, int leafDefinitionLevel) {
    }

    private static NullTarget resolveNullTarget(String columnName, FileSchema schema) {
        SchemaNode node = resolveNode(columnName, schema);

        if (node instanceof SchemaNode.GroupNode group) {
            rejectRepeatedGroup(columnName, group);

            SchemaNode.PrimitiveNode leaf = leafToAnswerFrom(group);

            if (leaf == null) {
                throw new IllegalArgumentException(
                        "Null predicates on a group are answered from a leaf column below it. "
                                + "Column '" + columnName + "' is a group with no leaf columns.");
            }

            return new NullTarget(leaf.columnIndex(), group.maxDefinitionLevel(),
                    leaf.maxDefinitionLevel());
        }

        ColumnSchema cs = schema.getColumn(columnName);
        rejectRepeated(columnName, cs);

        return new NullTarget(cs.columnIndex(), cs.maxDefinitionLevel(), cs.maxDefinitionLevel());
    }

    /// The walk of `columnName` over the schema, which answers what the name denotes directly, so
    /// a caller branches on the node it gets back rather than on whether a leaf lookup threw.
    ///
    /// @throws IllegalArgumentException if the name reaches no node in the schema
    private static SchemaPathResolver.Resolution resolvePath(String columnName, FileSchema schema) {
        SchemaPathResolver.Resolution resolution = SchemaPathResolver.resolve(schema, columnName);

        if (resolution.node() == null) {
            throw new IllegalArgumentException("Column '" + columnName + "' not found in schema");
        }

        return resolution;
    }

    /// The schema node `columnName` denotes, whether leaf or group.
    private static SchemaNode resolveNode(String columnName, FileSchema schema) {
        return resolvePath(columnName, schema).node();
    }

    /// Rejects a group that occurs more than once per row, which is a group below a repeated path.
    /// It holds many values per row, so a single answer per row is not defined.
    ///
    /// A `LIST` or a `MAP` is not itself repeated — the group below it is — so a null predicate on
    /// one asks a question with a single answer per row: whether the list or map is present at all.
    private static void rejectRepeatedGroup(String columnName, SchemaNode.GroupNode group) {
        if (group.maxRepetitionLevel() > 0) {
            throw repeatedColumnRejected(columnName);
        }
    }

    /// The leaf column below `group` whose definition levels a null predicate on the group reads.
    ///
    /// Any leaf below the group answers whether the group is present: a row where it is absent
    /// writes one entry below the group's definition level into every leaf under it. A
    /// non-repeated leaf is preferred because it writes exactly one entry per row either way,
    /// which is what lets a row group be proven to match throughout rather than only to be
    /// undecided. Below a `LIST` or a `MAP` there is no such leaf, and the first one serves.
    ///
    /// @return the leaf to answer from, or `null` if the group holds no leaf columns at all
    private static SchemaNode.PrimitiveNode leafToAnswerFrom(SchemaNode.GroupNode group) {
        SchemaNode.PrimitiveNode firstLeaf = null;

        for (SchemaNode child : group.children()) {
            SchemaNode.PrimitiveNode candidate = child instanceof SchemaNode.GroupNode childGroup
                    ? leafToAnswerFrom(childGroup)
                    : (SchemaNode.PrimitiveNode) child;

            if (candidate == null) {
                continue;
            }
            if (candidate.maxRepetitionLevel() == 0) {
                return candidate;
            }
            if (firstLeaf == null) {
                firstLeaf = candidate;
            }
        }

        return firstLeaf;
    }

    /// The leaf column a comparison, set or spatial predicate names, checked against what such a
    /// predicate can be answered on at all.
    ///
    /// Only a leaf carries a predicate. A name denoting a group — a struct, a `LIST`, or a `MAP` —
    /// is rejected rather than resolved to one of the group's leaves, which would answer a
    /// different question than the one that was asked. A leaf below a repeated path holds many
    /// values per row, and one below a `VARIANT` group holds an encoded payload no accessor reads
    /// as a value; both take `isNull` / `isNotNull` only, which [#resolveNullTarget] answers
    /// without coming here.
    ///
    /// @return the resolved column schema
    /// @throws IllegalArgumentException if the column is not found, or is one no predicate of this
    ///         shape reaches
    private static ColumnSchema leafColumn(String columnName, FileSchema schema) {
        SchemaPathResolver.Resolution resolution = resolvePath(columnName, schema);

        if (resolution.node() instanceof SchemaNode.GroupNode group) {
            rejectRepeatedGroup(columnName, group);
            throw new IllegalArgumentException(
                    "Filter predicates require a leaf column. "
                            + "Column '" + columnName + "' is a group.");
        }
        if (resolution.variantAncestor() != null) {
            throw new IllegalArgumentException("Column '" + columnName + "' is a leaf of the VARIANT "
                    + "group '" + resolution.variantAncestor() + "', which holds an encoded variant; "
                    + "it takes isNull and isNotNull predicates only");
        }

        ColumnSchema columnSchema = schema.getColumn(columnName);
        rejectRepeated(columnName, columnSchema);

        return columnSchema;
    }

    /// The leaf column a predicate comparing with `op` names, refusing an ordered operator on a
    /// column whose type puts its values in no order.
    private static ColumnSchema leafColumn(String columnName, FileSchema schema, Operator op) {
        ColumnSchema columnSchema = leafColumn(columnName, schema);
        requireOrder(columnName, columnSchema, op);
        return columnSchema;
    }

    /// Refuses `lt`, `ltEq`, `gt` and `gtEq` on a column whose annotation defines no order.
    ///
    /// parquet-format defines a sort order for most annotations and none for `INTERVAL`,
    /// `GEOMETRY`, `GEOGRAPHY` and `NULL`: an interval's months, days and milliseconds have no
    /// fixed conversion between them, and a geometry is a shape. Comparing their stored bytes
    /// would answer in an order nobody defined — for an `INTERVAL`, one that sorts 256 months
    /// below 1 month, since the components are little-endian. Equality and the set form stay:
    /// those ask whether a stored value *is* the literal, which the bytes answer.
    private static void requireOrder(String columnName, ColumnSchema columnSchema, Operator op) {
        if (isEquality(op) || BoundsReadability.namesAnOrder(columnSchema.logicalType())) {
            return;
        }
        throw new IllegalArgumentException("Column '" + columnName + "' is annotated "
                + columnSchema.logicalType() + ", whose values parquet-format puts in no order; "
                + "it takes equality and set membership only");
    }

    /// Refuses `not` over a predicate holding an `intersects`.
    ///
    /// `intersects` asks whether a bounding box overlaps the one a unit records, which has no
    /// inverse: the rows a box does not cover are not a box. `not` is lowered by inverting each
    /// leaf, so the whole tree below it is walked rather than the leaf alone.
    private static void rejectNegatedIntersects(FilterPredicate predicate) {
        switch (predicate) {
            case IntersectsPredicate p -> throw new IllegalArgumentException("Column '" + p.column()
                    + "' is tested by intersects, which has no inverse and cannot appear below not");
            case And a -> a.filters().forEach(FilterPredicateResolver::rejectNegatedIntersects);
            case Or o -> o.filters().forEach(FilterPredicateResolver::rejectNegatedIntersects);
            case Not n -> rejectNegatedIntersects(n.delegate());
            default -> {
            }
        }
    }

    // ==================== Type validation ====================

    private static void rejectRepeated(String columnName, ColumnSchema columnSchema) {
        if (columnSchema.maxRepetitionLevel() > 0) {
            throw repeatedColumnRejected(columnName);
        }
    }

    private static IllegalArgumentException repeatedColumnRejected(String columnName) {
        return new IllegalArgumentException(
                "Filter predicates do not support repeated columns. "
                        + "Column '" + columnName + "' is repeated.");
    }

    /// Refuses `literal`, such as `an int`, on a column whose physical type does not store it.
    private static void validateType(String columnName, PhysicalType expectedType,
            ColumnSchema columnSchema, String literal) {
        PhysicalType actualType = columnSchema.type();
        if (actualType != expectedType && !isBinaryCompatible(actualType, expectedType)) {
            throw ColumnLiterals.notTaken(columnName, columnSchema, literal);
        }
    }

    /// Whether the column's values order by unsigned magnitude, which an `INT(bitWidth,
    /// isSigned = false)` annotation says and nothing else does.
    ///
    /// The narrower unsigned widths never actually diverge — an `INT(8, false)` holds `0..255`,
    /// which orders the same either way — but they take the unsigned form too, so the annotation
    /// alone decides and no width is a special case.
    private static boolean ordersUnsigned(ColumnSchema columnSchema) {
        return columnSchema.logicalType() instanceof LogicalType.IntType intType && !intType.isSigned();
    }

    // ==================== Byte literals ====================

    /// A column a `byte[]` literal reaches: `INT96`, `BYTE_ARRAY` or `FIXED_LEN_BYTE_ARRAY`,
    /// whatever it is annotated.
    private static ColumnSchema byteColumn(String columnName, ColumnSchema columnSchema) {
        if (columnSchema.type() != PhysicalType.INT96) {
            validateType(columnName, PhysicalType.BYTE_ARRAY, columnSchema, "a byte[]");
        }
        return columnSchema;
    }

    /// The literal that orders a byte column whose values do not order as their stored bytes, as
    /// a refusal names it, or `null` for a column whose values do.
    ///
    /// A `DECIMAL` orders by the number its bytes encode, a `FLOAT16` by the half, and an `INT96`
    /// and a `FIXED_LEN_BYTE_ARRAY(12)` `TIMESTAMP` by the instant or wall clock. On those a `byte[]` literal takes equality and membership only, which the
    /// bytes answer whatever annotation the reader recognises; the typed literal carries the
    /// order.
    ///
    /// The switch is exhaustive rather than a list of exceptions, so an annotation added later
    /// has to state whether its values order as their bytes.
    private static String orderingLiteral(ColumnSchema columnSchema) {
        if (columnSchema.type() == PhysicalType.INT96) {
            return "an Instant";
        }
        LogicalType logicalType = columnSchema.logicalType();
        if (logicalType == null) {
            return null;
        }
        return switch (logicalType) {
            case LogicalType.DecimalType ignored -> "a BigDecimal";
            case LogicalType.Float16Type ignored -> "a float";
            case LogicalType.StringType ignored -> null;
            case LogicalType.EnumType ignored -> null;
            case LogicalType.JsonType ignored -> null;
            case LogicalType.BsonType ignored -> null;
            case LogicalType.UuidType ignored -> null;
            // No order at all, which [#requireOrder] refuses before this is reached.
            case LogicalType.IntervalType ignored -> null;
            case LogicalType.GeometryType ignored -> null;
            case LogicalType.GeographyType ignored -> null;
            case LogicalType.NullType ignored -> null;
            // Not a leaf annotation, or dropped off a binary physical type when the schema is read.
            case LogicalType.VariantType ignored -> null;
            case LogicalType.ListType ignored -> null;
            case LogicalType.MapType ignored -> null;
            case LogicalType.IntType ignored -> null;
            case LogicalType.DateType ignored -> null;
            case LogicalType.TimeType ignored -> null;
            // Only a FIXED_LEN_BYTE_ARRAY(12) timestamp reaches here: byteColumn refuses an INT64.
            case LogicalType.TimestampType timestamp -> timestamp.isAdjustedToUTC() ? "an Instant" : "a LocalDateTime";
        };
    }

    private static IllegalArgumentException notByteOrdered(String columnName, ColumnSchema columnSchema,
            String orderingLiteral) {
        String description = columnSchema.type() == PhysicalType.INT96
                ? TimestampAccessorKind.describeLegacyInt96()
                : "annotated " + columnSchema.logicalType();
        return new IllegalArgumentException("Column '" + columnName + "' is " + description
                + ", whose values do not order as their stored bytes; a byte[] literal takes eq, notEq and in "
                + "there, and an ordered predicate takes " + orderingLiteral);
    }

    /// Whether a row stores exactly `value`, on a column whose values do not order as their bytes.
    ///
    /// Those bytes denote one value of the column, so a row storing them holds that value: the
    /// comparison of the value reads the column's bounds, in the order they are written in, and
    /// the comparison of the bytes, [Comparison#STORED_BYTES], reads none. A `FIXED_LEN_BYTE_ARRAY(12)`
    /// `TIMESTAMP` and a fixed-width `DECIMAL` hold each value under one encoding, so there the value
    /// alone decides.
    private static ResolvedPredicate storedBytesEqual(String columnName, ColumnSchema columnSchema, byte[] value,
            List<ColumnOrder> columnOrders) {
        int columnIndex = columnSchema.columnIndex();
        ResolvedPredicate bytes = new ResolvedPredicate.BinaryPredicate(columnIndex, Operator.EQ, value,
                Comparison.STORED_BYTES);
        if (columnSchema.type() == PhysicalType.INT96) {
            requireInt96Width(columnName, value);
            return bytes;
        }
        if (columnSchema.logicalType() instanceof LogicalType.TimestampType) {
            requireFixedTimestampWidth(columnName, value);
            return new ResolvedPredicate.BinaryPredicate(columnIndex, Operator.EQ, value, Comparison.FIXED_TIMESTAMP);
        }
        if (columnSchema.logicalType() instanceof LogicalType.Float16Type) {
            return new ResolvedPredicate.And(List.of(new ResolvedPredicate.Float16Predicate(columnIndex, Operator.EQ,
                    float16ToFloat(columnName, value), isIeee754TotalOrder(columnIndex, columnOrders)), bytes));
        }
        if (columnSchema.type() == PhysicalType.FIXED_LEN_BYTE_ARRAY) {
            rejectUnholdableWidth(columnName, columnSchema, Operator.EQ, value);
            return new ResolvedPredicate.BinaryPredicate(columnIndex, Operator.EQ, value, Comparison.FIXED_DECIMAL);
        }
        return new ResolvedPredicate.And(List.of(new ResolvedPredicate.BinaryPredicate(columnIndex, Operator.EQ,
                value, Comparison.VARIABLE_DECIMAL), bytes));
    }

    /// Whether a row stores exactly one of `values`, as [#storedBytesEqual] decides it for each.
    private static ResolvedPredicate storedBytesMember(String columnName, ColumnSchema columnSchema, byte[][] values,
            List<ColumnOrder> columnOrders) {
        int columnIndex = columnSchema.columnIndex();
        ResolvedPredicate bytes = new ResolvedPredicate.BinaryInPredicate(columnIndex, values,
                Comparison.STORED_BYTES);
        if (columnSchema.type() == PhysicalType.INT96) {
            for (byte[] value : values) {
                requireInt96Width(columnName, value);
            }
            return bytes;
        }
        if (columnSchema.logicalType() instanceof LogicalType.TimestampType) {
            for (byte[] value : values) {
                requireFixedTimestampWidth(columnName, value);
            }
            return new ResolvedPredicate.BinaryInPredicate(columnIndex, values, Comparison.FIXED_TIMESTAMP);
        }
        if (columnSchema.logicalType() instanceof LogicalType.Float16Type) {
            return new ResolvedPredicate.And(List.of(new ResolvedPredicate.Float16InPredicate(columnIndex,
                    float16Probes(columnName, values), isIeee754TotalOrder(columnIndex, columnOrders)), bytes));
        }
        if (columnSchema.type() == PhysicalType.FIXED_LEN_BYTE_ARRAY) {
            for (byte[] value : values) {
                rejectUnholdableWidth(columnName, columnSchema, Operator.EQ, value);
            }
            return new ResolvedPredicate.BinaryInPredicate(columnIndex, values, Comparison.FIXED_DECIMAL);
        }
        return new ResolvedPredicate.And(List.of(new ResolvedPredicate.BinaryInPredicate(columnIndex, values,
                Comparison.VARIABLE_DECIMAL), bytes));
    }

    /// Refuses a `String` literal on a column that does not hold text.
    ///
    /// A `String` is the literal exactly where `getString` reads the column, which
    /// [TextColumns] decides for both: a `STRING`, an `ENUM`, a `JSON` and an unannotated
    /// `BYTE_ARRAY`, whose stored bytes are the literal's UTF-8 encoding. Every other binary
    /// column stores bytes its annotation reads as something else, which a caller writes as a
    /// `byte[]` or as the annotation's own literal type.
    private static void requireTextColumn(String columnName, ColumnSchema columnSchema) {
        if (!TextColumns.holdsText(columnSchema.type(), columnSchema.logicalType())) {
            throw ColumnLiterals.notTaken(columnName, columnSchema, "a String");
        }
    }

    /// A `BOOLEAN` predicate, as one of the four answers a two-valued column has.
    ///
    /// `false` orders before `true` and a column holds nothing besides the two, so every ordered
    /// operator is an equality against one of them or a constant: `ltEq(true)` admits every value
    /// the column holds, `lt(false)` none. Reducing them here is what "Literals the column cannot
    /// hold" does on any other carrier, and it leaves every evaluator — statistics, dictionary,
    /// batch and record — the one boolean comparison it already answers.
    private static ResolvedPredicate booleanLeaf(int columnIndex, Operator op, boolean value) {
        return switch (op) {
            case EQ, NOT_EQ -> new ResolvedPredicate.BooleanPredicate(columnIndex, op, value);
            case LT -> value
                    ? booleanEquals(columnIndex, false)
                    : new ResolvedPredicate.NoRowPredicate(columnIndex);
            case LT_EQ -> value
                    ? new ResolvedPredicate.EveryNonNullRowPredicate(columnIndex)
                    : booleanEquals(columnIndex, false);
            case GT -> value
                    ? new ResolvedPredicate.NoRowPredicate(columnIndex)
                    : booleanEquals(columnIndex, true);
            case GT_EQ -> value
                    ? booleanEquals(columnIndex, true)
                    : new ResolvedPredicate.EveryNonNullRowPredicate(columnIndex);
        };
    }

    private static ResolvedPredicate booleanEquals(int columnIndex, boolean value) {
        return new ResolvedPredicate.BooleanPredicate(columnIndex, Operator.EQ, value);
    }

    /// The twelve bytes an `INTERVAL` column stores for `value`: its months, its days and its
    /// milliseconds, each an unsigned 32-bit little-endian integer.
    private static byte[] intervalBytes(String columnName, PqInterval value) {
        byte[] bytes = new byte[INTERVAL_BYTES];
        ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
                .putInt(unsignedComponent(columnName, value, value.months()))
                .putInt(unsignedComponent(columnName, value, value.days()))
                .putInt(unsignedComponent(columnName, value, value.milliseconds()));
        return bytes;
    }

    /// One component of an interval literal, as the four bytes the column stores it in. The cast
    /// takes the low 32 bits of a value measured against the unsigned range just above, which is
    /// the bit pattern the column holds.
    private static int unsignedComponent(String columnName, PqInterval literal, long component) {
        if (component < 0 || component > UNSIGNED_INT_MAX) {
            throw cannotHold(columnName, "an interval whose months, days and milliseconds are each "
                    + "within [0, " + UNSIGNED_INT_MAX + "]", literal.toString());
        }
        return (int) component;
    }

    /// The half two little-endian bytes of a `FLOAT16` literal encode, which the column's bounds
    /// are written in the order of.
    private static float float16ToFloat(String columnName, byte[] value) {
        if (value.length != FLOAT16_BYTES) {
            throw new IllegalArgumentException(
                    "Column '" + columnName + "' is a FLOAT16, whose literal is "
                            + FLOAT16_BYTES + " bytes, not " + value.length);
        }
        return Float.float16ToFloat((short) ((value[1] & 0xFF) << 8 | value[0] & 0xFF));
    }

    /// Refuses a byte literal on an `INT96` column that is not the twelve bytes of a value.
    private static void requireInt96Width(String columnName, byte[] value) {
        requireLiteralWidth(columnName, "an INT96", LogicalTypeConverter.INT96_BYTES, value);
    }

    /// Refuses a byte literal on a `FIXED_LEN_BYTE_ARRAY(12)` `TIMESTAMP` column that is not the
    /// twelve bytes of a value.
    private static void requireFixedTimestampWidth(String columnName, byte[] value) {
        requireLiteralWidth(columnName, "a FIXED_LEN_BYTE_ARRAY(12) TIMESTAMP", Flba12Timestamps.WIDTH, value);
    }

    /// Refuses a byte literal of another width than the `width` bytes that encode a value of the
    /// column, which `column` names.
    private static void requireLiteralWidth(String columnName, String column, int width, byte[] value) {
        if (value.length != width) {
            throw new IllegalArgumentException("Column '" + columnName + "' is " + column + ", whose literal is "
                    + width + " bytes, not " + value.length);
        }
    }

    /// The two-byte probes of a `FLOAT16` membership test, as the halves they encode. See
    /// [#float16ToFloat].
    private static float[] float16Probes(String columnName, byte[][] values) {
        float[] probes = new float[values.length];
        for (int i = 0; i < values.length; i++) {
            probes[i] = float16ToFloat(columnName, values[i]);
        }
        return probes;
    }

    /// Refuses `literal` on a column not carrying the annotation that literal is the value of.
    private static void validateLogicalType(String columnName,
            Class<? extends LogicalType> expectedLogicalType,
            ColumnSchema columnSchema, String literal) {
        if (!expectedLogicalType.isInstance(columnSchema.logicalType())) {
            throw ColumnLiterals.notTaken(columnName, columnSchema, literal);
        }
    }

    private static boolean isBinaryCompatible(PhysicalType actual, PhysicalType expected) {
        return (actual == PhysicalType.BYTE_ARRAY || actual == PhysicalType.FIXED_LEN_BYTE_ARRAY)
                && (expected == PhysicalType.BYTE_ARRAY || expected == PhysicalType.FIXED_LEN_BYTE_ARRAY);
    }

    // ==================== Value conversion helpers ====================

    /// The unit of a `TIMESTAMP` column of the kind the literal denotes: an [Instant] a UTC-adjusted
    /// one, a [LocalDateTime] a local wall clock. A legacy `INT96` timestamp, which an [Instant]
    /// reaches before this, takes no [LocalDateTime].
    private static LogicalType.TimeUnit timestampUnit(String columnName, ColumnSchema columnSchema,
            boolean literalIsInstant) {
        if (!literalIsInstant && isLegacyInt96(columnSchema)) {
            throw new IllegalArgumentException("Column '" + columnName + "' is "
                    + TimestampAccessorKind.describeLegacyInt96()
                    + ", which takes Instant and byte[] literals, not a LocalDateTime");
        }
        String literal = literalIsInstant ? "an Instant" : "a LocalDateTime";
        if (!(columnSchema.logicalType() instanceof LogicalType.TimestampType timestampType)) {
            throw ColumnLiterals.notTaken(columnName, columnSchema, literal);
        }
        if (timestampType.isAdjustedToUTC() != literalIsInstant) {
            throw new IllegalArgumentException("Column '" + columnName + "' is "
                    + TimestampAccessorKind.describe(timestampType.isAdjustedToUTC()) + ", which takes "
                    + ColumnLiterals.taken(columnSchema) + " literals, not " + literal);
        }
        // FileSchema keeps a TIMESTAMP on no other carrier.
        if (columnSchema.type() != PhysicalType.FIXED_LEN_BYTE_ARRAY) {
            validateType(columnName, PhysicalType.INT64, columnSchema, literal);
        }
        return timestampType.unit();
    }

    /// The unit of a `TIME` column, which stores `MILLIS` in an `INT32` and the finer units in an
    /// `INT64`.
    private static LogicalType.TimeUnit timeUnit(String columnName, ColumnSchema columnSchema) {
        if (!(columnSchema.logicalType() instanceof LogicalType.TimeType timeType)) {
            throw ColumnLiterals.notTaken(columnName, columnSchema, "a LocalTime");
        }
        validateType(columnName,
                timeType.unit() == LogicalType.TimeUnit.MILLIS ? PhysicalType.INT32 : PhysicalType.INT64,
                columnSchema, "a LocalTime");
        return timeType.unit();
    }

    /// A `DATE` column, which stores epoch days in an `INT32`.
    private static ColumnSchema dateColumn(String columnName, ColumnSchema columnSchema) {
        validateType(columnName, PhysicalType.INT32, columnSchema, "a LocalDate");
        validateLogicalType(columnName, LogicalType.DateType.class, columnSchema, "a LocalDate");
        return columnSchema;
    }

    /// A `FIXED_LEN_BYTE_ARRAY` column carrying `annotation`, as a `UUID` or `INTERVAL` literal
    /// requires.
    private static ColumnSchema annotatedFixedWidth(String columnName, Class<? extends LogicalType> annotation,
            String literal, ColumnSchema columnSchema) {
        validateType(columnName, PhysicalType.FIXED_LEN_BYTE_ARRAY, columnSchema, literal);
        validateLogicalType(columnName, annotation, columnSchema, literal);
        return columnSchema;
    }

    /// The sixteen big-endian bytes a `UUID` column stores for `value`.
    private static byte[] uuidBytes(UUID value) {
        return ByteBuffer.allocate(16)
                .putLong(value.getMostSignificantBits())
                .putLong(value.getLeastSignificantBits())
                .array();
    }

    private static LogicalType.DecimalType getDecimalType(String columnName, ColumnSchema columnSchema) {
        if (columnSchema.logicalType() instanceof LogicalType.DecimalType decimalType) {
            return decimalType;
        }
        throw ColumnLiterals.notTaken(columnName, columnSchema, "a BigDecimal");
    }

    /// Converts an unscaled [BigInteger] to a fixed-length big-endian two's complement byte array,
    /// matching the Parquet `FIXED_LEN_BYTE_ARRAY` encoding for decimals. The output is
    /// sign-extended (0x00 for positive, 0xFF for negative) to fill the fixed length.
    ///
    /// The value has already been measured against the width by the caller.
    static byte[] toFixedLenDecimalBytes(BigInteger unscaled, int typeLength) {
        byte[] minimal = unscaled.toByteArray();
        if (minimal.length == typeLength) {
            return minimal;
        }
        if (minimal.length > typeLength) {
            throw new IllegalStateException("An unscaled value of " + minimal.length
                    + " bytes reached the encoding for a FIXED_LEN_BYTE_ARRAY(" + typeLength + ") DECIMAL");
        }
        byte[] padded = new byte[typeLength];
        byte fill = (byte) (unscaled.signum() < 0 ? 0xFF : 0x00);
        int offset = typeLength - minimal.length;
        for (int i = 0; i < offset; i++) {
            padded[i] = fill;
        }
        System.arraycopy(minimal, 0, padded, offset, minimal.length);
        return padded;
    }

    // ==================== Literals the column cannot hold ====================

    /// A predicate whose literal has been measured against the column's carrier.
    ///
    /// Where the column holds the literal, the predicate is what was asked for. Where it does
    /// not, equality asks whether a stored value *is* a value the column cannot store, which no
    /// row can answer yes and every row answers no, and which is in practice a mistake — so it is
    /// refused. An order asks instead where the literal sits among the column's values, which it
    /// has an answer to: the predicate moves to the nearest value the column holds on the side
    /// the operator admits, and where the column holds nothing on that side at all, no row can
    /// match.
    private static ResolvedPredicate carried(String columnName, int columnIndex,
            FilterPredicate.Operator op, CarriedLiteral carried, String holds, String literal,
            CarriedLeaf leaf) {
        if (carried.exact()) {
            return leaf.of(op, carried.below());
        }
        return switch (op) {
            case EQ, NOT_EQ -> throw cannotHold(columnName, holds, literal);
            case LT, LT_EQ -> carried.below() == null
                    ? new ResolvedPredicate.NoRowPredicate(columnIndex)
                    : leaf.of(FilterPredicate.Operator.LT_EQ, carried.below());
            case GT, GT_EQ -> carried.above() == null
                    ? new ResolvedPredicate.NoRowPredicate(columnIndex)
                    : leaf.of(FilterPredicate.Operator.GT_EQ, carried.above());
        };
    }

    private static IllegalArgumentException cannotHold(String columnName, String holds, String literal) {
        return new IllegalArgumentException("Column '" + columnName + "' holds " + holds
                + "; the equality literal " + literal + " is not a value it can hold");
    }

    /// A predicate on a `FIXED_LEN_BYTE_ARRAY` `DECIMAL`, whose unscaled value is sign-extended to
    /// the column width. The width is what bounds the literal.
    private static ResolvedPredicate fixedDecimal(String columnName, ColumnSchema columnSchema,
            FilterPredicate.Operator op, BigInteger floor, BigInteger ceiling, String scale,
            String literal) {
        int width = FixedWidthValidator.requireWidth(null, columnSchema);
        CarriedLiteral.Range range = CarriedLiteral.Range.ofBytes(width);
        return carried(columnName, columnSchema.columnIndex(), op,
                CarriedLiteral.within(floor, ceiling, range.min(), range.max()),
                scale + " within " + width + " bytes", literal,
                (resolvedOp, value) -> new ResolvedPredicate.BinaryPredicate(columnSchema.columnIndex(),
                        resolvedOp, toFixedLenDecimalBytes(value, width), Comparison.FIXED_DECIMAL));
    }

    /// Refuses an equality literal of a width a fixed-width column cannot store. An order literal
    /// of another width compares as given: the comparison is exact on it either way.
    private static void rejectUnholdableWidth(String columnName, ColumnSchema columnSchema,
            FilterPredicate.Operator op, byte[] literal) {
        if (columnSchema.type() != PhysicalType.FIXED_LEN_BYTE_ARRAY || !isEquality(op)) {
            return;
        }
        int width = FixedWidthValidator.requireWidth(null, columnSchema);
        if (literal.length == width) {
            return;
        }
        throw cannotHold(columnName, "a byte string of " + width + " bytes",
                HEX.formatHex(literal) + " (" + literal.length + " bytes)");
    }

    /// Refuses an equality literal no IEEE half represents. An order literal compares as given
    /// against the halves the column stores, which is exact.
    private static void rejectUnholdableHalf(String columnName, FilterPredicate.Operator op, float literal) {
        if (!isEquality(op) || Float.isNaN(literal)
                || Float.float16ToFloat(Float.floatToFloat16(literal)) == literal) {
            return;
        }
        throw cannotHold(columnName, "a value an IEEE half represents", Float.toString(literal));
    }

    /// Whether the operator asks whether a stored value *is* the literal, rather than where it
    /// sits in the column's order.
    private static boolean isEquality(FilterPredicate.Operator op) {
        return op == FilterPredicate.Operator.EQ || op == FilterPredicate.Operator.NOT_EQ;
    }

    /// Whether the column is the legacy `INT96` timestamp, which `getTimestamp` reads as an
    /// [Instant]. An `INT96` carrying an annotation is read as that annotation instead.
    private static boolean isLegacyInt96(ColumnSchema columnSchema) {
        return columnSchema.type() == PhysicalType.INT96 && columnSchema.logicalType() == null;
    }

    /// An [Instant] literal on an `INT96` column, measured in nanoseconds since Julian day 0
    /// against the instants the column's twelve bytes encode.
    ///
    /// A stored value's nanoseconds of the day are not bounded by one day, so the column holds
    /// instants past its first and last Julian day, up to what an `INT64` of nanoseconds reaches
    /// from there. That is the range [#carried] measures against, for equality and order alike.
    private static ResolvedPredicate int96Instant(String columnName, ColumnSchema cs, Operator op,
            Instant value) {
        return carried(columnName, cs.columnIndex(), op, int96Instant(value), INT96_HOLDS, value.toString(),
                (resolvedOp, instant) -> new ResolvedPredicate.BinaryPredicate(cs.columnIndex(), resolvedOp,
                        int96Bytes(instant), Comparison.INT96_INSTANT));
    }

    /// `value` in nanoseconds since Julian day 0, measured against [#INT96_MIN] and [#INT96_MAX].
    private static CarriedLiteral int96Instant(Instant value) {
        BigInteger nanos = nanosSinceEpoch(value).add(
                BigInteger.valueOf(LogicalTypeConverter.JULIAN_EPOCH_OFFSET_DAYS).multiply(NANOS_PER_DAY));
        return CarriedLiteral.within(nanos, nanos, INT96_MIN, INT96_MAX);
    }

    /// The twelve bytes of an instant, `nanos` since Julian day 0, within [#INT96_MIN] and
    /// [#INT96_MAX]: its nanoseconds of the day, then its Julian day. A day past the `INT32`
    /// range keeps the extreme day and carries the days beyond it in the nanoseconds, which
    /// [BinaryComparator#compareInt96] reads as the same instant.
    private static byte[] int96Bytes(BigInteger nanos) {
        BigInteger[] dayAndRemainder = nanos.divideAndRemainder(NANOS_PER_DAY);
        BigInteger floorDay = dayAndRemainder[1].signum() < 0
                ? dayAndRemainder[0].subtract(BigInteger.ONE)
                : dayAndRemainder[0];
        int day = Math.clamp(floorDay.longValueExact(), Integer.MIN_VALUE, Integer.MAX_VALUE);
        long nanosOfDay = nanos.subtract(BigInteger.valueOf(day).multiply(NANOS_PER_DAY)).longValueExact();
        return ByteBuffer.allocate(LogicalTypeConverter.INT96_BYTES).order(ByteOrder.LITTLE_ENDIAN)
                .putLong(nanosOfDay)
                .putInt(day)
                .array();
    }

    /// A timestamp literal, `nanos` since the epoch, on a column counting `unit` in an `INT64` or in
    /// a `FIXED_LEN_BYTE_ARRAY(12)`.
    private static ResolvedPredicate timestamp(String columnName, ColumnSchema cs, Operator op,
            LogicalType.TimeUnit unit, BigInteger nanos, String shown) {
        if (cs.type() == PhysicalType.FIXED_LEN_BYTE_ARRAY) {
            return carried(columnName, cs.columnIndex(), op, inUnit(nanos, unit, FIXED_TIMESTAMP_RANGE.min(),
                    FIXED_TIMESTAMP_RANGE.max()), timeHolds(unit), shown,
                    (resolvedOp, value) -> new ResolvedPredicate.BinaryPredicate(cs.columnIndex(), resolvedOp,
                            fixedTimestampBytes(value), Comparison.FIXED_TIMESTAMP));
        }
        return carried(columnName, cs.columnIndex(), op, inUnit(nanos, unit, INT64_MIN, INT64_MAX),
                timestampHolds(unit), shown,
                (resolvedOp, value) -> new ResolvedPredicate.LongPredicate(cs.columnIndex(), resolvedOp,
                        value.longValueExact()));
    }

    /// A timestamp set, each probe `nanos` of a value since the epoch, measured as a [#timestamp]
    /// equality literal is.
    private static <T> ResolvedPredicate timestampIn(String columnName, ColumnSchema cs, LogicalType.TimeUnit unit,
            List<T> values, Function<T, BigInteger> nanos, Function<T, String> shown) {
        if (cs.type() == PhysicalType.FIXED_LEN_BYTE_ARRAY) {
            return new ResolvedPredicate.BinaryInPredicate(cs.columnIndex(), bytesOf(held(columnName, values,
                    value -> inUnit(nanos.apply(value), unit, FIXED_TIMESTAMP_RANGE.min(), FIXED_TIMESTAMP_RANGE.max()),
                    timeHolds(unit), shown), FilterPredicateResolver::fixedTimestampBytes), Comparison.FIXED_TIMESTAMP);
        }
        return new ResolvedPredicate.LongInPredicate(cs.columnIndex(), longsOf(held(columnName, values,
                value -> inUnit(nanos.apply(value), unit, INT64_MIN, INT64_MAX), timestampHolds(unit), shown)));
    }

    /// The twelve bytes a `FIXED_LEN_BYTE_ARRAY(12)` `TIMESTAMP` stores for `count` units: its
    /// two's complement, least significant byte first. The count has already been measured against
    /// [#FIXED_TIMESTAMP_RANGE].
    private static byte[] fixedTimestampBytes(BigInteger count) {
        byte[] bigEndian = toFixedLenDecimalBytes(count, Flba12Timestamps.WIDTH);
        byte[] littleEndian = new byte[bigEndian.length];
        for (int i = 0; i < bigEndian.length; i++) {
            littleEndian[i] = bigEndian[bigEndian.length - 1 - i];
        }
        return littleEndian;
    }

    private static String timestampHolds(LogicalType.TimeUnit unit) {
        return timeHolds(unit) + " within the INT64 range";
    }

    /// A time of day measured in the column's unit, narrowed to the carrier that unit is stored in.
    private static CarriedLiteral timeOfDay(LocalTime value, LogicalType.TimeUnit unit) {
        BigInteger nanoOfDay = BigInteger.valueOf(value.toNanoOfDay());
        return unit == LogicalType.TimeUnit.MILLIS
                ? inUnit(nanoOfDay, unit, INT32_MIN, INT32_MAX)
                : inUnit(nanoOfDay, unit, INT64_MIN, INT64_MAX);
    }

    private static String timeHolds(LogicalType.TimeUnit unit) {
        return "a whole number of " + unitName(unit);
    }

    /// The epoch day of a date, narrowed to the `INT32` a `DATE` is stored in.
    private static CarriedLiteral epochDay(LocalDate value) {
        BigInteger epochDay = BigInteger.valueOf(value.toEpochDay());
        return CarriedLiteral.within(epochDay, epochDay, INT32_MIN, INT32_MAX);
    }

    private static String scaleHolds(LogicalType.DecimalType decimalType) {
        return "a DECIMAL of scale " + decimalType.scale();
    }

    /// A `BigDecimal` set on a `DECIMAL` column, each probe measured as a [DecimalColumnPredicate]
    /// equality literal is.
    private static ResolvedPredicate decimalIn(String columnName, ColumnSchema cs, List<BigDecimal> values) {
        LogicalType.DecimalType dt = getDecimalType(columnName, cs);
        String scale = scaleHolds(dt);
        return switch (cs.type()) {
            case INT32 -> new ResolvedPredicate.IntInPredicate(cs.columnIndex(), intsOf(held(columnName, values,
                    value -> atScale(value, dt, INT32_MIN, INT32_MAX), scale + " within the INT32 range",
                    BigDecimal::toPlainString)));
            case INT64 -> new ResolvedPredicate.LongInPredicate(cs.columnIndex(), longsOf(held(columnName, values,
                    value -> atScale(value, dt, INT64_MIN, INT64_MAX), scale + " within the INT64 range",
                    BigDecimal::toPlainString)));
            case FIXED_LEN_BYTE_ARRAY -> {
                int width = FixedWidthValidator.requireWidth(null, cs);
                CarriedLiteral.Range range = CarriedLiteral.Range.ofBytes(width);
                yield new ResolvedPredicate.BinaryInPredicate(cs.columnIndex(), bytesOf(held(columnName, values,
                        value -> atScale(value, dt, range.min(), range.max()), scale + " within " + width + " bytes",
                        BigDecimal::toPlainString), unscaled -> toFixedLenDecimalBytes(unscaled, width)),
                        Comparison.FIXED_DECIMAL);
            }
            default -> {
                validateType(columnName, PhysicalType.BYTE_ARRAY, cs, "a BigDecimal");
                yield new ResolvedPredicate.BinaryInPredicate(cs.columnIndex(), bytesOf(held(columnName, values,
                        value -> CarriedLiteral.unbounded(unscaled(value, dt, RoundingMode.FLOOR),
                                unscaled(value, dt, RoundingMode.CEILING)),
                        scale, BigDecimal::toPlainString), BigInteger::toByteArray), Comparison.VARIABLE_DECIMAL);
            }
        };
    }

    /// `value` at the column's scale, narrowed to `[min, max]`.
    private static CarriedLiteral atScale(BigDecimal value, LogicalType.DecimalType decimalType,
            BigInteger min, BigInteger max) {
        return CarriedLiteral.within(unscaled(value, decimalType, RoundingMode.FLOOR),
                unscaled(value, decimalType, RoundingMode.CEILING), min, max);
    }

    private static BigInteger unscaled(BigDecimal value, LogicalType.DecimalType decimalType, RoundingMode rounding) {
        return value.setScale(decimalType.scale(), rounding).unscaledValue();
    }

    /// The carrier values the probes of a set form name, each measured by `measure`. Every probe
    /// is an equality literal, so one the column cannot hold is refused as [#carried] refuses it.
    private static <T> BigInteger[] held(String columnName, List<T> values, Function<T, CarriedLiteral> measure,
            String holds, Function<T, String> shown) {
        BigInteger[] held = new BigInteger[values.size()];
        for (int i = 0; i < held.length; i++) {
            T value = values.get(i);
            CarriedLiteral carried = measure.apply(value);
            if (!carried.exact()) {
                throw cannotHold(columnName, holds, shown.apply(value));
            }
            held[i] = carried.below();
        }
        return held;
    }

    private static int[] intsOf(BigInteger[] values) {
        int[] ints = new int[values.length];
        for (int i = 0; i < ints.length; i++) {
            ints[i] = values[i].intValueExact();
        }
        return ints;
    }

    private static long[] longsOf(BigInteger[] values) {
        long[] longs = new long[values.length];
        for (int i = 0; i < longs.length; i++) {
            longs[i] = values[i].longValueExact();
        }
        return longs;
    }

    private static byte[][] bytesOf(BigInteger[] values, Function<BigInteger, byte[]> encode) {
        byte[][] bytes = new byte[values.length][];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = encode.apply(values[i]);
        }
        return bytes;
    }

    /// A wall clock as nanoseconds since the epoch: a local timestamp stores its wall clock as
    /// though it were a UTC instant.
    private static BigInteger wallClockNanos(LocalDateTime value) {
        return nanosSinceEpoch(value.toInstant(ZoneOffset.UTC));
    }

    /// `nanos` measured in `unit`, narrowed to `[min, max]`.
    private static CarriedLiteral inUnit(BigInteger nanos, LogicalType.TimeUnit unit,
            BigInteger min, BigInteger max) {
        BigInteger[] quotientAndRemainder = nanos.divideAndRemainder(nanosPerUnit(unit));
        BigInteger floor = quotientAndRemainder[1].signum() < 0
                ? quotientAndRemainder[0].subtract(BigInteger.ONE)
                : quotientAndRemainder[0];
        BigInteger ceiling = quotientAndRemainder[1].signum() == 0 ? floor : floor.add(BigInteger.ONE);
        return CarriedLiteral.within(floor, ceiling, min, max);
    }

    private static BigInteger nanosPerUnit(LogicalType.TimeUnit unit) {
        return switch (unit) {
            case MILLIS -> NANOS_PER_MILLI;
            case MICROS -> NANOS_PER_MICRO;
            case NANOS -> BigInteger.ONE;
        };
    }

    private static String unitName(LogicalType.TimeUnit unit) {
        return switch (unit) {
            case MILLIS -> "milliseconds";
            case MICROS -> "microseconds";
            case NANOS -> "nanoseconds";
        };
    }

    /// `value` as a count of nanoseconds since the epoch, which no `long` holds for every
    /// [Instant].
    private static BigInteger nanosSinceEpoch(Instant value) {
        return BigInteger.valueOf(value.getEpochSecond()).multiply(NANOS_PER_SECOND)
                .add(BigInteger.valueOf(value.getNano()));
    }
}
