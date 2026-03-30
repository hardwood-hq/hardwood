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
import java.time.LocalTime;

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
import dev.hardwood.reader.FilterPredicate.FloatColumnPredicate;
import dev.hardwood.reader.FilterPredicate.InstantColumnPredicate;
import dev.hardwood.reader.FilterPredicate.IntColumnPredicate;
import dev.hardwood.reader.FilterPredicate.IntInPredicate;
import dev.hardwood.reader.FilterPredicate.LongColumnPredicate;
import dev.hardwood.reader.FilterPredicate.LongInPredicate;
import dev.hardwood.reader.FilterPredicate.Not;
import dev.hardwood.reader.FilterPredicate.Or;
import dev.hardwood.reader.FilterPredicate.TimeColumnPredicate;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

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

    /// Resolves a [FilterPredicate] tree into a [ResolvedPredicate] tree.
    ///
    /// @param predicate the user-facing predicate tree
    /// @param schema the file schema for column resolution and type validation
    /// @return a fully resolved predicate tree ready for evaluation
    public static ResolvedPredicate resolve(FilterPredicate predicate, FileSchema schema) {
        return switch (predicate) {
            case DateColumnPredicate p -> {
                ColumnSchema cs = resolveColumn(p.column(), schema);
                validateType(p.column(), PhysicalType.INT32, cs);
                yield new ResolvedPredicate.IntPredicate(cs.columnIndex(), p.op(),
                        Math.toIntExact(p.value().toEpochDay()));
            }
            case InstantColumnPredicate p -> {
                ColumnSchema cs = resolveColumn(p.column(), schema);
                LogicalType.TimeUnit unit = getTimestampUnit(p.column(), cs);
                validateType(p.column(), PhysicalType.INT64, cs);
                yield new ResolvedPredicate.LongPredicate(cs.columnIndex(), p.op(),
                        instantToLong(p.value(), unit));
            }
            case TimeColumnPredicate p -> {
                ColumnSchema cs = resolveColumn(p.column(), schema);
                LogicalType.TimeUnit unit = getTimeUnit(p.column(), cs);
                long value = localTimeToLong(p.value(), unit);
                if (unit == LogicalType.TimeUnit.MILLIS) {
                    validateType(p.column(), PhysicalType.INT32, cs);
                    yield new ResolvedPredicate.IntPredicate(cs.columnIndex(), p.op(),
                            Math.toIntExact(value));
                }
                validateType(p.column(), PhysicalType.INT64, cs);
                yield new ResolvedPredicate.LongPredicate(cs.columnIndex(), p.op(), value);
            }
            case DecimalColumnPredicate p -> {
                ColumnSchema cs = resolveColumn(p.column(), schema);
                LogicalType.DecimalType dt = getDecimalType(p.column(), cs);
                // setScale without RoundingMode throws ArithmeticException if rounding is needed,
                // which is the correct behavior: the predicate value must match the column's scale exactly
                BigDecimal scaled = p.value().setScale(dt.scale());
                PhysicalType physicalType = cs.type();
                if (physicalType == PhysicalType.INT32) {
                    yield new ResolvedPredicate.IntPredicate(cs.columnIndex(), p.op(),
                            scaled.unscaledValue().intValueExact());
                }
                else if (physicalType == PhysicalType.INT64) {
                    yield new ResolvedPredicate.LongPredicate(cs.columnIndex(), p.op(),
                            scaled.unscaledValue().longValueExact());
                }
                else {
                    yield new ResolvedPredicate.BinaryPredicate(cs.columnIndex(), p.op(),
                            toFixedLenDecimalBytes(scaled.unscaledValue(), cs.typeLength()),
                            true);
                }
            }
            case IntColumnPredicate p -> {
                ColumnSchema cs = resolveColumn(p.column(), schema);
                validateType(p.column(), PhysicalType.INT32, cs);
                yield new ResolvedPredicate.IntPredicate(cs.columnIndex(), p.op(), p.value());
            }
            case LongColumnPredicate p -> {
                ColumnSchema cs = resolveColumn(p.column(), schema);
                validateType(p.column(), PhysicalType.INT64, cs);
                yield new ResolvedPredicate.LongPredicate(cs.columnIndex(), p.op(), p.value());
            }
            case FloatColumnPredicate p -> {
                ColumnSchema cs = resolveColumn(p.column(), schema);
                validateType(p.column(), PhysicalType.FLOAT, cs);
                yield new ResolvedPredicate.FloatPredicate(cs.columnIndex(), p.op(), p.value());
            }
            case DoubleColumnPredicate p -> {
                ColumnSchema cs = resolveColumn(p.column(), schema);
                validateType(p.column(), PhysicalType.DOUBLE, cs);
                yield new ResolvedPredicate.DoublePredicate(cs.columnIndex(), p.op(), p.value());
            }
            case BooleanColumnPredicate p -> {
                ColumnSchema cs = resolveColumn(p.column(), schema);
                validateType(p.column(), PhysicalType.BOOLEAN, cs);
                yield new ResolvedPredicate.BooleanPredicate(cs.columnIndex(), p.op(), p.value());
            }
            case BinaryColumnPredicate p -> {
                ColumnSchema cs = resolveColumn(p.column(), schema);
                validateType(p.column(), PhysicalType.BYTE_ARRAY, cs);
                yield new ResolvedPredicate.BinaryPredicate(cs.columnIndex(), p.op(), p.value(), false);
            }
            case FilterPredicate.SignedBinaryColumnPredicate p -> {
                ColumnSchema cs = resolveColumn(p.column(), schema);
                validateType(p.column(), PhysicalType.FIXED_LEN_BYTE_ARRAY, cs);
                yield new ResolvedPredicate.BinaryPredicate(cs.columnIndex(), p.op(), p.value(), true);
            }
            case IntInPredicate p -> {
                ColumnSchema cs = resolveColumn(p.column(), schema);
                validateType(p.column(), PhysicalType.INT32, cs);
                yield new ResolvedPredicate.IntInPredicate(cs.columnIndex(), p.values());
            }
            case LongInPredicate p -> {
                ColumnSchema cs = resolveColumn(p.column(), schema);
                validateType(p.column(), PhysicalType.INT64, cs);
                yield new ResolvedPredicate.LongInPredicate(cs.columnIndex(), p.values());
            }
            case BinaryInPredicate p -> {
                ColumnSchema cs = resolveColumn(p.column(), schema);
                validateType(p.column(), PhysicalType.BYTE_ARRAY, cs);
                yield new ResolvedPredicate.BinaryInPredicate(cs.columnIndex(), p.values());
            }
            case FilterPredicate.IsNullPredicate p -> {
                ColumnSchema cs = resolveColumn(p.column(), schema);
                yield new ResolvedPredicate.IsNullPredicate(cs.columnIndex());
            }
            case FilterPredicate.IsNotNullPredicate p -> {
                ColumnSchema cs = resolveColumn(p.column(), schema);
                yield new ResolvedPredicate.IsNotNullPredicate(cs.columnIndex());
            }
            case And a -> new ResolvedPredicate.And(a.filters().stream()
                    .map(f -> resolve(f, schema))
                    .toList());
            case Or o -> new ResolvedPredicate.Or(o.filters().stream()
                    .map(f -> resolve(f, schema))
                    .toList());
            case Not n -> {
                ResolvedPredicate resolvedDelegate = resolve(n.delegate(), schema);
                yield ResolvedPredicate.negate(resolvedDelegate);
            }
        };
    }

    // ==================== Column resolution ====================

    /// Resolves a column name to its [ColumnSchema]. Tries exact name/path lookup first,
    /// then falls back to matching by top-level field name (for nested/repeated columns).
    ///
    /// @return the resolved column schema
    /// @throws IllegalArgumentException if the column is not found
    private static ColumnSchema resolveColumn(String columnName, FileSchema schema) {
        try {
            return schema.getColumn(columnName);
        }
        catch (IllegalArgumentException e) {
            // Fall back to matching by top-level field name for nested/repeated columns
            // (e.g. "scores" matches "scores.list.element")
            int columnCount = schema.getColumnCount();
            for (int i = 0; i < columnCount; i++) {
                ColumnSchema col = schema.getColumn(i);
                if (col.fieldPath().topLevelName().equals(columnName)) {
                    return col;
                }
            }
            throw new IllegalArgumentException(
                    "Column '" + columnName + "' not found in schema", e);
        }
    }

    // ==================== Type validation ====================

    private static void validateType(String columnName, PhysicalType expectedType,
            ColumnSchema columnSchema) {
        PhysicalType actualType = columnSchema.type();
        if (actualType != expectedType && !isBinaryCompatible(actualType, expectedType)) {
            throw new IllegalArgumentException(
                    "Column '" + columnName + "' has physical type " + actualType
                            + "; given filter predicate type " + expectedType + " is incompatible");
        }
    }

    private static boolean isBinaryCompatible(PhysicalType actual, PhysicalType expected) {
        return (actual == PhysicalType.BYTE_ARRAY || actual == PhysicalType.FIXED_LEN_BYTE_ARRAY)
                && (expected == PhysicalType.BYTE_ARRAY || expected == PhysicalType.FIXED_LEN_BYTE_ARRAY);
    }

    // ==================== Value conversion helpers ====================

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
