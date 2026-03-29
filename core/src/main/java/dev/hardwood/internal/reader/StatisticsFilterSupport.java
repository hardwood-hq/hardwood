/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.FilterPredicate.BinaryColumnPredicate;
import dev.hardwood.reader.FilterPredicate.BinaryInPredicate;
import dev.hardwood.reader.FilterPredicate.BooleanColumnPredicate;
import dev.hardwood.reader.FilterPredicate.DoubleColumnPredicate;
import dev.hardwood.reader.FilterPredicate.FloatColumnPredicate;
import dev.hardwood.reader.FilterPredicate.IntColumnPredicate;
import dev.hardwood.reader.FilterPredicate.IntInPredicate;
import dev.hardwood.reader.FilterPredicate.LongColumnPredicate;
import dev.hardwood.reader.FilterPredicate.LongInPredicate;
import dev.hardwood.reader.FilterPredicate.SignedBinaryColumnPredicate;

/// Shared utilities for evaluating filter predicates against min/max statistics.
///
/// Used by both [RowGroupFilterEvaluator] (row-group-level statistics) and
/// [PageFilterEvaluator] (page-level Column Index statistics) via the
/// [MinMaxStats] abstraction.
final class StatisticsFilterSupport {

    private StatisticsFilterSupport() {
    }

    // ==================== Leaf predicate evaluation ====================

    /// Evaluates a resolved physical leaf predicate against [MinMaxStats].
    ///
    /// @return `true` if the predicate proves no rows can match (safe to drop)
    static boolean canDropLeaf(FilterPredicate leaf, MinMaxStats stats) {
        if (stats.minValue() == null || stats.maxValue() == null) {
            return false;
        }
        return switch (leaf) {
            case IntColumnPredicate p -> canDrop(p.op(), p.value(),
                    StatisticsDecoder.decodeInt(stats.minValue()),
                    StatisticsDecoder.decodeInt(stats.maxValue()));
            case LongColumnPredicate p -> canDrop(p.op(), p.value(),
                    StatisticsDecoder.decodeLong(stats.minValue()),
                    StatisticsDecoder.decodeLong(stats.maxValue()));
            case FloatColumnPredicate p -> canDropFloat(p.op(), p.value(),
                    StatisticsDecoder.decodeFloat(stats.minValue()),
                    StatisticsDecoder.decodeFloat(stats.maxValue()));
            case DoubleColumnPredicate p -> canDropDouble(p.op(), p.value(),
                    StatisticsDecoder.decodeDouble(stats.minValue()),
                    StatisticsDecoder.decodeDouble(stats.maxValue()));
            case BooleanColumnPredicate p -> canDrop(p.op(), p.value() ? 1 : 0,
                    StatisticsDecoder.decodeBoolean(stats.minValue()) ? 1 : 0,
                    StatisticsDecoder.decodeBoolean(stats.maxValue()) ? 1 : 0);
            case BinaryColumnPredicate p -> {
                int cmpMin = StatisticsDecoder.compareBinary(p.value(), stats.minValue());
                int cmpMax = StatisticsDecoder.compareBinary(p.value(), stats.maxValue());
                yield canDropCompared(p.op(), cmpMin, cmpMax,
                        StatisticsDecoder.compareBinary(stats.minValue(), stats.maxValue()));
            }
            case SignedBinaryColumnPredicate p -> {
                int cmpMin = StatisticsDecoder.compareSignedBinary(p.value(), stats.minValue());
                int cmpMax = StatisticsDecoder.compareSignedBinary(p.value(), stats.maxValue());
                yield canDropCompared(p.op(), cmpMin, cmpMax,
                        StatisticsDecoder.compareSignedBinary(stats.minValue(), stats.maxValue()));
            }
            case IntInPredicate p -> canDropIntIn(p.values(),
                    StatisticsDecoder.decodeInt(stats.minValue()),
                    StatisticsDecoder.decodeInt(stats.maxValue()));
            case LongInPredicate p -> canDropLongIn(p.values(),
                    StatisticsDecoder.decodeLong(stats.minValue()),
                    StatisticsDecoder.decodeLong(stats.maxValue()));
            case BinaryInPredicate p -> canDropBinaryIn(p.values(),
                    stats.minValue(), stats.maxValue());
            default -> false;
        };
    }

    // ==================== Predicate introspection ====================

    /// Extracts the column name from a leaf predicate.
    static String columnOf(FilterPredicate predicate) {
        return switch (predicate) {
            case IntColumnPredicate p -> p.column();
            case LongColumnPredicate p -> p.column();
            case FloatColumnPredicate p -> p.column();
            case DoubleColumnPredicate p -> p.column();
            case BooleanColumnPredicate p -> p.column();
            case BinaryColumnPredicate p -> p.column();
            case SignedBinaryColumnPredicate p -> p.column();
            case IntInPredicate p -> p.column();
            case LongInPredicate p -> p.column();
            case BinaryInPredicate p -> p.column();
            default -> throw new IllegalArgumentException(
                    "Not a leaf predicate: " + predicate.getClass().getSimpleName());
        };
    }

    /// Returns the expected physical type for a leaf predicate.
    static PhysicalType expectedPhysicalType(FilterPredicate predicate) {
        return switch (predicate) {
            case IntColumnPredicate ignored -> PhysicalType.INT32;
            case LongColumnPredicate ignored -> PhysicalType.INT64;
            case FloatColumnPredicate ignored -> PhysicalType.FLOAT;
            case DoubleColumnPredicate ignored -> PhysicalType.DOUBLE;
            case BooleanColumnPredicate ignored -> PhysicalType.BOOLEAN;
            case BinaryColumnPredicate ignored -> PhysicalType.BYTE_ARRAY;
            case SignedBinaryColumnPredicate ignored -> PhysicalType.FIXED_LEN_BYTE_ARRAY;
            case IntInPredicate ignored -> PhysicalType.INT32;
            case LongInPredicate ignored -> PhysicalType.INT64;
            case BinaryInPredicate ignored -> PhysicalType.BYTE_ARRAY;
            default -> throw new IllegalArgumentException(
                    "Not a leaf predicate: " + predicate.getClass().getSimpleName());
        };
    }

    // ==================== Type compatibility ====================

    static boolean isBinaryCompatible(PhysicalType actual, PhysicalType expected) {
        return (actual == PhysicalType.BYTE_ARRAY || actual == PhysicalType.FIXED_LEN_BYTE_ARRAY)
                && (expected == PhysicalType.BYTE_ARRAY || expected == PhysicalType.FIXED_LEN_BYTE_ARRAY);
    }

    // ==================== Range comparison logic ====================

    /// Determines if a range can be dropped given integer-comparable min/max statistics.
    /// Works for int, long, boolean (mapped to 0/1).
    static boolean canDrop(FilterPredicate.Operator op, long value, long min, long max) {
        return switch (op) {
            case EQ -> value < min || value > max;
            case NOT_EQ -> min == max && value == min;
            case LT -> min >= value;
            case LT_EQ -> min > value;
            case GT -> max <= value;
            case GT_EQ -> max < value;
        };
    }

    static boolean canDropFloat(FilterPredicate.Operator op, float value, float min, float max) {
        return switch (op) {
            case EQ -> Float.compare(value, min) < 0 || Float.compare(value, max) > 0;
            case NOT_EQ -> Float.compare(min, max) == 0 && Float.compare(value, min) == 0;
            case LT -> Float.compare(min, value) >= 0;
            case LT_EQ -> Float.compare(min, value) > 0;
            case GT -> Float.compare(max, value) <= 0;
            case GT_EQ -> Float.compare(max, value) < 0;
        };
    }

    static boolean canDropDouble(FilterPredicate.Operator op, double value, double min, double max) {
        return switch (op) {
            case EQ -> Double.compare(value, min) < 0 || Double.compare(value, max) > 0;
            case NOT_EQ -> Double.compare(min, max) == 0 && Double.compare(value, min) == 0;
            case LT -> Double.compare(min, value) >= 0;
            case LT_EQ -> Double.compare(min, value) > 0;
            case GT -> Double.compare(max, value) <= 0;
            case GT_EQ -> Double.compare(max, value) < 0;
        };
    }

    /// Determines if a range can be dropped given pre-computed comparison results for binary values.
    ///
    /// @param cmpMin comparison of value vs min (negative if value < min)
    /// @param cmpMax comparison of value vs max (positive if value > max)
    /// @param minEqMax comparison of min vs max (0 if min == max)
    static boolean canDropCompared(FilterPredicate.Operator op, int cmpMin, int cmpMax, int minEqMax) {
        return switch (op) {
            case EQ -> cmpMin < 0 || cmpMax > 0;
            case NOT_EQ -> minEqMax == 0 && cmpMin == 0;
            case LT -> cmpMin <= 0;
            case LT_EQ -> cmpMin < 0;
            case GT -> cmpMax >= 0;
            case GT_EQ -> cmpMax > 0;
        };
    }

    // ==================== IN predicate range checks ====================

    static boolean canDropIntIn(int[] values, int min, int max) {
        for (int value : values) {
            if (value >= min && value <= max) {
                return false;
            }
        }
        return true;
    }

    static boolean canDropLongIn(long[] values, long min, long max) {
        for (long value : values) {
            if (value >= min && value <= max) {
                return false;
            }
        }
        return true;
    }

    static boolean canDropBinaryIn(byte[][] values, byte[] min, byte[] max) {
        for (byte[] value : values) {
            if (StatisticsDecoder.compareBinary(value, min) >= 0
                    && StatisticsDecoder.compareBinary(value, max) <= 0) {
                return false;
            }
        }
        return true;
    }
}
