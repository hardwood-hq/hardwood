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
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;

import dev.hardwood.internal.predicate.ResolvedPredicate.BinaryPredicate.Comparison;
import dev.hardwood.internal.schema.FixedWidthValidator;
import dev.hardwood.internal.schema.SchemaPathResolver;
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
import dev.hardwood.reader.FilterPredicate.LongColumnPredicate;
import dev.hardwood.reader.FilterPredicate.LongInPredicate;
import dev.hardwood.reader.FilterPredicate.Not;
import dev.hardwood.reader.FilterPredicate.Or;
import dev.hardwood.reader.FilterPredicate.TimeColumnPredicate;
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

    private static final BigInteger INT32_MIN = BigInteger.valueOf(Integer.MIN_VALUE);
    private static final BigInteger INT32_MAX = BigInteger.valueOf(Integer.MAX_VALUE);
    private static final BigInteger INT64_MIN = BigInteger.valueOf(Long.MIN_VALUE);
    private static final BigInteger INT64_MAX = BigInteger.valueOf(Long.MAX_VALUE);

    private static final BigInteger NANOS_PER_MILLI = BigInteger.valueOf(1_000_000L);
    private static final BigInteger NANOS_PER_MICRO = BigInteger.valueOf(1_000L);
    private static final BigInteger NANOS_PER_SECOND = BigInteger.valueOf(1_000_000_000L);

    private static final HexFormat HEX = HexFormat.of();

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
                ColumnSchema cs = resolveColumn(p.column(), schema);
                rejectRepeated(p.column(), cs);
                validateType(p.column(), PhysicalType.INT32, cs);
                validateLogicalType(p.column(), LogicalType.DateType.class, cs);
                BigInteger epochDay = BigInteger.valueOf(p.value().toEpochDay());
                yield carried(p.column(), cs.columnIndex(), p.op(),
                        CarriedLiteral.within(epochDay, epochDay, INT32_MIN, INT32_MAX),
                        "an epoch day within the INT32 range", p.value().toString(),
                        (op, day) -> new ResolvedPredicate.IntPredicate(cs.columnIndex(), op, day.intValueExact()));
            }
            case InstantColumnPredicate p -> {
                ColumnSchema cs = resolveColumn(p.column(), schema);
                rejectRepeated(p.column(), cs);
                LogicalType.TimeUnit unit = getTimestampUnit(p.column(), cs);
                validateType(p.column(), PhysicalType.INT64, cs);
                yield carried(p.column(), cs.columnIndex(), p.op(),
                        inUnit(nanosSinceEpoch(p.value()), unit, INT64_MIN, INT64_MAX),
                        "a whole number of " + unitName(unit) + " within the INT64 range",
                        p.value().toString(),
                        (op, value) -> new ResolvedPredicate.LongPredicate(cs.columnIndex(), op,
                                value.longValueExact()));
            }
            case TimeColumnPredicate p -> {
                ColumnSchema cs = resolveColumn(p.column(), schema);
                rejectRepeated(p.column(), cs);
                LogicalType.TimeUnit unit = getTimeUnit(p.column(), cs);
                BigInteger nanoOfDay = BigInteger.valueOf(p.value().toNanoOfDay());
                String holds = "a whole number of " + unitName(unit);
                if (unit == LogicalType.TimeUnit.MILLIS) {
                    validateType(p.column(), PhysicalType.INT32, cs);
                    yield carried(p.column(), cs.columnIndex(), p.op(),
                            inUnit(nanoOfDay, unit, INT32_MIN, INT32_MAX), holds, p.value().toString(),
                            (op, value) -> new ResolvedPredicate.IntPredicate(cs.columnIndex(), op,
                                    value.intValueExact()));
                }
                validateType(p.column(), PhysicalType.INT64, cs);
                yield carried(p.column(), cs.columnIndex(), p.op(),
                        inUnit(nanoOfDay, unit, INT64_MIN, INT64_MAX), holds, p.value().toString(),
                        (op, value) -> new ResolvedPredicate.LongPredicate(cs.columnIndex(), op,
                                value.longValueExact()));
            }
            case DecimalColumnPredicate p -> {
                ColumnSchema cs = resolveColumn(p.column(), schema);
                rejectRepeated(p.column(), cs);
                LogicalType.DecimalType dt = getDecimalType(p.column(), cs);
                BigInteger floor = p.value().setScale(dt.scale(), RoundingMode.FLOOR).unscaledValue();
                BigInteger ceiling = p.value().setScale(dt.scale(), RoundingMode.CEILING).unscaledValue();
                String scale = "a DECIMAL of scale " + dt.scale();
                PhysicalType physicalType = cs.type();
                if (physicalType == PhysicalType.INT32) {
                    yield carried(p.column(), cs.columnIndex(), p.op(),
                            CarriedLiteral.within(floor, ceiling, INT32_MIN, INT32_MAX),
                            scale + " within the INT32 range", p.value().toPlainString(),
                            (op, value) -> new ResolvedPredicate.IntPredicate(cs.columnIndex(), op,
                                    value.intValueExact()));
                }
                else if (physicalType == PhysicalType.INT64) {
                    yield carried(p.column(), cs.columnIndex(), p.op(),
                            CarriedLiteral.within(floor, ceiling, INT64_MIN, INT64_MAX),
                            scale + " within the INT64 range", p.value().toPlainString(),
                            (op, value) -> new ResolvedPredicate.LongPredicate(cs.columnIndex(), op,
                                    value.longValueExact()));
                }
                else if (physicalType == PhysicalType.FIXED_LEN_BYTE_ARRAY) {
                    yield fixedDecimal(p.column(), cs, p.op(), floor, ceiling, scale,
                            p.value().toPlainString());
                }
                else {
                    validateType(p.column(), PhysicalType.BYTE_ARRAY, cs);
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
                ColumnSchema cs = resolveColumn(p.column(), schema);
                rejectRepeated(p.column(), cs);
                validateType(p.column(), PhysicalType.INT32, cs);
                yield ordersUnsigned(cs)
                        ? new ResolvedPredicate.UnsignedIntPredicate(cs.columnIndex(), p.op(), p.value())
                        : new ResolvedPredicate.IntPredicate(cs.columnIndex(), p.op(), p.value());
            }
            case LongColumnPredicate p -> {
                ColumnSchema cs = resolveColumn(p.column(), schema);
                rejectRepeated(p.column(), cs);
                validateType(p.column(), PhysicalType.INT64, cs);
                yield ordersUnsigned(cs)
                        ? new ResolvedPredicate.UnsignedLongPredicate(cs.columnIndex(), p.op(), p.value())
                        : new ResolvedPredicate.LongPredicate(cs.columnIndex(), p.op(), p.value());
            }
            case FloatColumnPredicate p -> {
                ColumnSchema cs = resolveColumn(p.column(), schema);
                rejectRepeated(p.column(), cs);
                if (cs.type() == PhysicalType.FIXED_LEN_BYTE_ARRAY
                        && cs.logicalType() instanceof LogicalType.Float16Type) {
                    rejectUnholdableHalf(p.column(), p.op(), p.value());
                    yield new ResolvedPredicate.Float16Predicate(cs.columnIndex(), p.op(), p.value(),
                            isIeee754TotalOrder(cs.columnIndex(), columnOrders));
                }
                validateType(p.column(), PhysicalType.FLOAT, cs);
                yield new ResolvedPredicate.FloatPredicate(cs.columnIndex(), p.op(), p.value(),
                        isIeee754TotalOrder(cs.columnIndex(), columnOrders));
            }
            case DoubleColumnPredicate p -> {
                ColumnSchema cs = resolveColumn(p.column(), schema);
                rejectRepeated(p.column(), cs);
                validateType(p.column(), PhysicalType.DOUBLE, cs);
                yield new ResolvedPredicate.DoublePredicate(cs.columnIndex(), p.op(), p.value(),
                        isIeee754TotalOrder(cs.columnIndex(), columnOrders));
            }
            case BooleanColumnPredicate p -> {
                ColumnSchema cs = resolveColumn(p.column(), schema);
                rejectRepeated(p.column(), cs);
                validateType(p.column(), PhysicalType.BOOLEAN, cs);
                yield new ResolvedPredicate.BooleanPredicate(cs.columnIndex(), p.op(), p.value());
            }
            case BinaryColumnPredicate p -> {
                ColumnSchema cs = resolveColumn(p.column(), schema);
                rejectRepeated(p.column(), cs);
                validateType(p.column(), PhysicalType.BYTE_ARRAY, cs);
                if (cs.logicalType() instanceof LogicalType.Float16Type) {
                    yield new ResolvedPredicate.Float16Predicate(cs.columnIndex(), p.op(),
                            float16ToFloat(p.column(), p.value()),
                            isIeee754TotalOrder(cs.columnIndex(), columnOrders));
                }
                Comparison comparison = byteComparison(cs);
                if (comparison == Comparison.FIXED_DECIMAL) {
                    BigInteger unscaled = unscaledOf(p.value());
                    yield fixedDecimal(p.column(), cs, p.op(), unscaled, unscaled,
                            "a DECIMAL", HEX.formatHex(p.value()));
                }
                rejectUnholdableWidth(p.column(), cs, p.op(), p.value());
                yield new ResolvedPredicate.BinaryPredicate(cs.columnIndex(), p.op(), p.value(), comparison);
            }
            case FilterPredicate.UUIDColumnPredicate p -> {
                ColumnSchema cs = resolveColumn(p.column(), schema);
                rejectRepeated(p.column(), cs);
                validateType(p.column(), PhysicalType.FIXED_LEN_BYTE_ARRAY, cs);
                validateLogicalType(p.column(), LogicalType.UuidType.class, cs);
                yield new ResolvedPredicate.BinaryPredicate(cs.columnIndex(), p.op(), p.value(),
                        Comparison.BYTE_STRING);
            }
            case IntInPredicate p -> {
                ColumnSchema cs = resolveColumn(p.column(), schema);
                rejectRepeated(p.column(), cs);
                validateType(p.column(), PhysicalType.INT32, cs);
                yield ordersUnsigned(cs)
                        ? new ResolvedPredicate.UnsignedIntInPredicate(cs.columnIndex(), p.values())
                        : new ResolvedPredicate.IntInPredicate(cs.columnIndex(), p.values());
            }
            case LongInPredicate p -> {
                ColumnSchema cs = resolveColumn(p.column(), schema);
                rejectRepeated(p.column(), cs);
                validateType(p.column(), PhysicalType.INT64, cs);
                yield ordersUnsigned(cs)
                        ? new ResolvedPredicate.UnsignedLongInPredicate(cs.columnIndex(), p.values())
                        : new ResolvedPredicate.LongInPredicate(cs.columnIndex(), p.values());
            }
            case BinaryInPredicate p -> {
                ColumnSchema cs = resolveColumn(p.column(), schema);
                rejectRepeated(p.column(), cs);
                validateType(p.column(), PhysicalType.BYTE_ARRAY, cs);
                if (cs.logicalType() instanceof LogicalType.Float16Type) {
                    yield new ResolvedPredicate.Float16InPredicate(cs.columnIndex(),
                            float16Probes(p.column(), p.values()),
                            isIeee754TotalOrder(cs.columnIndex(), columnOrders));
                }
                Comparison comparison = byteComparison(cs);
                yield new ResolvedPredicate.BinaryInPredicate(cs.columnIndex(),
                        probeBytes(p.column(), p.values(), comparison, cs), comparison);
            }
            case DoubleInPredicate p -> {
                ColumnSchema cs = resolveColumn(p.column(), schema);
                rejectRepeated(p.column(), cs);
                if (cs.type() == PhysicalType.FIXED_LEN_BYTE_ARRAY
                        && cs.logicalType() instanceof LogicalType.Float16Type) {
                    yield new ResolvedPredicate.Float16InPredicate(cs.columnIndex(), p.values(),
                            isIeee754TotalOrder(cs.columnIndex(), columnOrders));
                }
                if (cs.type() == PhysicalType.DOUBLE) {
                    yield new ResolvedPredicate.DoubleInPredicate(cs.columnIndex(), p.values(), false,
                            isIeee754TotalOrder(cs.columnIndex(), columnOrders));
                }
                if (cs.type() == PhysicalType.FLOAT) {
                    yield new ResolvedPredicate.DoubleInPredicate(cs.columnIndex(), p.values(), true,
                            isIeee754TotalOrder(cs.columnIndex(), columnOrders));
                }
                throw new IllegalArgumentException(
                        "Column '" + p.column() + "' has physical type " + cs.type()
                                + "; given filter predicate type DOUBLE/FLOAT is incompatible");
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
                yield ResolvedPredicate.negate(resolvedDelegate);
            }
            case IntersectsPredicate p -> {
                ColumnSchema cs = resolveColumn(p.column(), schema);
                if (!(cs.logicalType() instanceof LogicalType.GeometryType) &&
                        !(cs.logicalType() instanceof LogicalType.GeographyType)) {
                    throw new IllegalArgumentException(
                            "Column '" + p.column() + "' is not a GEOMETRY or GEOGRAPHY column");
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

    /// The schema node `columnName` denotes, whether leaf or group.
    ///
    /// The walk answers what the name denotes directly, so a caller branches on the node it gets
    /// back rather than on whether a leaf lookup threw.
    ///
    /// @throws IllegalArgumentException if the name reaches no node in the schema
    private static SchemaNode resolveNode(String columnName, FileSchema schema) {
        SchemaNode node = SchemaPathResolver.resolve(schema, columnName).node();

        if (node == null) {
            throw new IllegalArgumentException("Column '" + columnName + "' not found in schema");
        }

        return node;
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

    /// Resolves a column name to its [ColumnSchema].
    ///
    /// Only leaf columns can carry a predicate. A name denoting a group — a struct, a `LIST`, or a
    /// `MAP` — is rejected rather than resolved to one of the group's leaves, which would answer a
    /// different question than the one that was asked.
    ///
    /// @return the resolved column schema
    /// @throws IllegalArgumentException if the column is not found, or names a group rather than a
    ///         leaf column
    private static ColumnSchema resolveColumn(String columnName, FileSchema schema) {
        SchemaNode node = resolveNode(columnName, schema);

        if (node instanceof SchemaNode.GroupNode group) {
            rejectRepeatedGroup(columnName, group);
            throw new IllegalArgumentException(
                    "Filter predicates require a leaf column. "
                            + "Column '" + columnName + "' is a group.");
        }

        return schema.getColumn(columnName);
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

    private static void validateType(String columnName, PhysicalType expectedType,
            ColumnSchema columnSchema) {
        PhysicalType actualType = columnSchema.type();
        if (actualType != expectedType && !isBinaryCompatible(actualType, expectedType)) {
            throw new IllegalArgumentException(
                    "Column '" + columnName + "' has physical type " + actualType
                            + "; given filter predicate type " + expectedType + " is incompatible");
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

    /// The bytes each probe of a binary membership test compares as on this column.
    ///
    /// Every probe is an equality literal, so the column has to hold each of them: a fixed-width
    /// `DECIMAL` brings the probe to the column's own encoding and refuses a value wider than it,
    /// and a fixed-width column of any other kind refuses a probe of another width.
    private static byte[][] probeBytes(String columnName, byte[][] probes, Comparison comparison,
            ColumnSchema columnSchema) {
        byte[][] resolved = new byte[probes.length][];
        for (int i = 0; i < probes.length; i++) {
            if (comparison == Comparison.FIXED_DECIMAL) {
                resolved[i] = fixedDecimalBytes(columnName, columnSchema, unscaledOf(probes[i]),
                        HEX.formatHex(probes[i]));
            }
            else {
                rejectUnholdableWidth(columnName, columnSchema, FilterPredicate.Operator.EQ, probes[i]);
                resolved[i] = probes[i];
            }
        }
        return resolved;
    }

    /// The number a binary `DECIMAL` literal stands for. An empty literal is zero, as
    /// [BinaryComparator#compareSigned] reads it.
    private static BigInteger unscaledOf(byte[] literal) {
        return literal.length == 0 ? BigInteger.ZERO : new BigInteger(literal);
    }

    /// The order a binary literal compares in on this column.
    ///
    /// Either binary physical type reaches here whatever it is annotated, since [#validateType]
    /// bridges `BYTE_ARRAY` and `FIXED_LEN_BYTE_ARRAY`. Most annotations order as the bytes
    /// themselves. A `DECIMAL` orders by the value they stand for, and its statistics are
    /// written that way, so the literal takes the column's own comparison rather than a
    /// byte-string one — the same comparison a `BigDecimal` literal resolves to, and the one
    /// parquet-java applies through `BINARY_AS_SIGNED_INTEGER`.
    ///
    /// The switch is exhaustive rather than a list of exceptions, so an annotation added later
    /// has to name its order instead of inheriting the byte-string one by default.
    private static Comparison byteComparison(ColumnSchema columnSchema) {
        LogicalType logicalType = columnSchema.logicalType();
        if (logicalType == null) {
            return Comparison.BYTE_STRING;
        }
        return switch (logicalType) {
            // Padded to the column's width, so one number has exactly one encoding here.
            case LogicalType.DecimalType ignored ->
                    columnSchema.type() == PhysicalType.FIXED_LEN_BYTE_ARRAY
                            ? Comparison.FIXED_DECIMAL
                            : Comparison.VARIABLE_DECIMAL;
            case LogicalType.StringType ignored -> Comparison.BYTE_STRING;
            case LogicalType.EnumType ignored -> Comparison.BYTE_STRING;
            case LogicalType.JsonType ignored -> Comparison.BYTE_STRING;
            case LogicalType.BsonType ignored -> Comparison.BYTE_STRING;
            case LogicalType.UuidType ignored -> Comparison.BYTE_STRING;
            case LogicalType.IntervalType ignored -> Comparison.BYTE_STRING;
            case LogicalType.GeometryType ignored -> Comparison.BYTE_STRING;
            case LogicalType.GeographyType ignored -> Comparison.BYTE_STRING;
            case LogicalType.VariantType ignored -> Comparison.BYTE_STRING;
            case LogicalType.NullType ignored -> Comparison.BYTE_STRING;
            case LogicalType.ListType ignored -> Comparison.BYTE_STRING;
            case LogicalType.MapType ignored -> Comparison.BYTE_STRING;
            // Handled before this is reached, or rejected by [#validateType] as a non-binary
            // physical type.
            case LogicalType.Float16Type ignored -> Comparison.BYTE_STRING;
            case LogicalType.IntType ignored -> Comparison.BYTE_STRING;
            case LogicalType.DateType ignored -> Comparison.BYTE_STRING;
            case LogicalType.TimeType ignored -> Comparison.BYTE_STRING;
            case LogicalType.TimestampType ignored -> Comparison.BYTE_STRING;
        };
    }

    /// The two bytes of a `FLOAT16` literal, as the value they encode.
    ///
    /// A `FLOAT16` is two little-endian bytes of an IEEE half. Comparing them as a byte string
    /// would order the low mantissa byte first, which is not the column's order and not the
    /// order its statistics are written in, so the literal is decoded and compared numerically —
    /// as parquet-java does through `BINARY_AS_FLOAT16`. A literal that is a `NaN` payload
    /// therefore matches any stored `NaN`, the same widening a `float` literal already carries.
    private static float float16ToFloat(String columnName, byte[] value) {
        if (value.length != FLOAT16_BYTES) {
            throw new IllegalArgumentException(
                    "Column '" + columnName + "' is a FLOAT16, whose literal is "
                            + FLOAT16_BYTES + " bytes, not " + value.length);
        }
        return Float.float16ToFloat((short) ((value[1] & 0xFF) << 8 | value[0] & 0xFF));
    }

    /// The two-byte probes of a `FLOAT16` membership test, as the halves they encode. See
    /// [#float16ToFloat].
    private static double[] float16Probes(String columnName, byte[][] values) {
        double[] probes = new double[values.length];
        for (int i = 0; i < values.length; i++) {
            probes[i] = float16ToFloat(columnName, values[i]);
        }
        return probes;
    }

    private static void validateLogicalType(String columnName,
            Class<? extends LogicalType> expectedLogicalType,
            ColumnSchema columnSchema) {
        LogicalType logicalType = columnSchema.logicalType();

        if (!expectedLogicalType.isInstance(logicalType)) {
            throw new IllegalArgumentException(
                    "Column '" + columnName + "' is not a " + expectedLogicalType.getSimpleName()
                            + " column (logical type: " + logicalType + ")");
        }
    }

    private static boolean isBinaryCompatible(PhysicalType actual, PhysicalType expected) {
        return (actual == PhysicalType.BYTE_ARRAY || actual == PhysicalType.FIXED_LEN_BYTE_ARRAY)
                && (expected == PhysicalType.BYTE_ARRAY || expected == PhysicalType.FIXED_LEN_BYTE_ARRAY);
    }

    // ==================== Value conversion helpers ====================

    private static LogicalType.TimeUnit getTimestampUnit(String columnName, ColumnSchema columnSchema) {
        if (columnSchema.logicalType() instanceof LogicalType.TimestampType timestampType) {
            return timestampType.unit();
        }
        throw new IllegalArgumentException(
                "Column '" + columnName + "' does not have a TIMESTAMP logical type");
    }

    private static LogicalType.TimeUnit getTimeUnit(String columnName, ColumnSchema columnSchema) {
        if (columnSchema.logicalType() instanceof LogicalType.TimeType timeType) {
            return timeType.unit();
        }
        throw new IllegalArgumentException(
                "Column '" + columnName + "' does not have a TIME logical type");
    }

    private static LogicalType.DecimalType getDecimalType(String columnName, ColumnSchema columnSchema) {
        if (columnSchema.logicalType() instanceof LogicalType.DecimalType decimalType) {
            return decimalType;
        }
        throw new IllegalArgumentException(
                "Column '" + columnName + "' does not have a DECIMAL logical type");
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

    /// `unscaled` in the column's own encoding, for a probe that has to be a value the column
    /// holds.
    private static byte[] fixedDecimalBytes(String columnName, ColumnSchema columnSchema,
            BigInteger unscaled, String literal) {
        int width = FixedWidthValidator.requireWidth(null, columnSchema);
        if (!CarriedLiteral.Range.ofBytes(width).holds(unscaled)) {
            throw cannotHold(columnName, "a DECIMAL within " + width + " bytes", literal);
        }
        return toFixedLenDecimalBytes(unscaled, width);
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
