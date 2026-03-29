/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalTime;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.FilterPredicate.And;
import dev.hardwood.reader.FilterPredicate.DateColumnPredicate;
import dev.hardwood.reader.FilterPredicate.DecimalColumnPredicate;
import dev.hardwood.reader.FilterPredicate.InstantColumnPredicate;
import dev.hardwood.reader.FilterPredicate.IntColumnPredicate;
import dev.hardwood.reader.FilterPredicate.LongColumnPredicate;
import dev.hardwood.reader.FilterPredicate.Not;
import dev.hardwood.reader.FilterPredicate.Or;
import dev.hardwood.reader.FilterPredicate.TimeColumnPredicate;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

/// Resolves logical-type predicates (Date, Instant, Time, Decimal) into their physical
/// equivalents (Int, Long, Binary) using the file schema.
///
/// This resolution must happen once before the predicate is passed to any evaluator
/// ([RowGroupFilterEvaluator], [PageFilterEvaluator], [RecordFilterEvaluator]),
/// so that evaluators only need to handle physical predicate types.
public class FilterPredicateResolver {

    /// Resolves all logical-type predicates in the tree to physical-type predicates.
    /// Recurses into [And], [Or], and [Not] combinators. Physical predicates pass through unchanged.
    ///
    /// @param predicate the predicate tree to resolve
    /// @param schema the file schema providing logical type information
    /// @return a predicate tree containing only physical-type predicates
    public static FilterPredicate resolve(FilterPredicate predicate, FileSchema schema) {
        return switch (predicate) {
            case DateColumnPredicate p -> new IntColumnPredicate(p.column(), p.op(),
                    Math.toIntExact(p.value().toEpochDay()));
            case InstantColumnPredicate p -> {
                ColumnSchema cs = schema.getColumn(p.column());
                LogicalType.TimeUnit unit = getTimestampUnit(p.column(), cs);
                yield new LongColumnPredicate(p.column(), p.op(), instantToLong(p.value(), unit));
            }
            case TimeColumnPredicate p -> {
                ColumnSchema cs = schema.getColumn(p.column());
                LogicalType.TimeUnit unit = getTimeUnit(p.column(), cs);
                long value = localTimeToLong(p.value(), unit);
                if (unit == LogicalType.TimeUnit.MILLIS) {
                    yield new IntColumnPredicate(p.column(), p.op(), Math.toIntExact(value));
                }
                yield new LongColumnPredicate(p.column(), p.op(), value);
            }
            case DecimalColumnPredicate p -> {
                ColumnSchema cs = schema.getColumn(p.column());
                LogicalType.DecimalType dt = getDecimalType(p.column(), cs);
                BigDecimal scaled = p.value().setScale(dt.scale());
                PhysicalType physicalType = cs.type();
                if (physicalType == PhysicalType.INT32) {
                    yield new IntColumnPredicate(p.column(), p.op(),
                            scaled.unscaledValue().intValueExact());
                }
                else if (physicalType == PhysicalType.INT64) {
                    yield new LongColumnPredicate(p.column(), p.op(),
                            scaled.unscaledValue().longValueExact());
                }
                else {
                    yield new FilterPredicate.SignedBinaryColumnPredicate(p.column(), p.op(),
                            toFixedLenDecimalBytes(scaled.unscaledValue(), cs.typeLength()));
                }
            }
            case And a -> new And(a.filters().stream()
                    .map(f -> resolve(f, schema))
                    .toList());
            case Or o -> new Or(o.filters().stream()
                    .map(f -> resolve(f, schema))
                    .toList());
            case Not n -> new Not(resolve(n.delegate(), schema));
            default -> predicate;
        };
    }

    /// Returns an [IllegalStateException] for an unresolved logical-type predicate.
    /// Used by evaluators in their switch branches for logical-type predicates that
    /// should have been resolved before evaluation.
    public static IllegalStateException unresolvedPredicate(FilterPredicate predicate) {
        return new IllegalStateException(
                predicate.getClass().getSimpleName()
                        + " must be resolved via FilterPredicateResolver.resolve() before evaluation");
    }

    static long instantToLong(Instant value, LogicalType.TimeUnit unit) {
        return switch (unit) {
            case MILLIS -> value.toEpochMilli();
            case MICROS -> Math.addExact(
                    Math.multiplyExact(value.getEpochSecond(), 1_000_000L),
                    value.getNano() / 1_000L);
            case NANOS -> Math.addExact(
                    Math.multiplyExact(value.getEpochSecond(), 1_000_000_000L),
                    value.getNano());
        };
    }

    static long localTimeToLong(LocalTime value, LogicalType.TimeUnit unit) {
        return switch (unit) {
            case MILLIS -> value.toNanoOfDay() / 1_000_000L;
            case MICROS -> value.toNanoOfDay() / 1_000L;
            case NANOS -> value.toNanoOfDay();
        };
    }

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
    static byte[] toFixedLenDecimalBytes(BigInteger unscaled, int typeLength) {
        byte[] minimal = unscaled.toByteArray();
        if (minimal.length == typeLength) {
            return minimal;
        }
        if (minimal.length > typeLength) {
            throw new ArithmeticException(
                    "Decimal value requires " + minimal.length + " bytes but column has typeLength " + typeLength);
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
}
