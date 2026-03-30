/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import dev.hardwood.reader.FilterPredicate;

/// Shared utilities for evaluating filter predicates against min/max statistics.
///
/// Used by both [RowGroupFilterEvaluator] (row-group-level statistics) and
/// [PageFilterEvaluator] (page-level Column Index statistics) via the
/// [MinMaxStats] abstraction.
final class StatisticsFilterSupport {

    private StatisticsFilterSupport() {
    }

    // ==================== Leaf predicate evaluation ====================

    /// Evaluates a resolved leaf predicate against [MinMaxStats].
    ///
    /// @return `true` if the predicate proves no rows can match (safe to drop)
    static boolean canDropLeaf(ResolvedPredicate leaf, MinMaxStats stats) {
        if (stats.minValue() == null || stats.maxValue() == null) {
            return false;
        }
        return switch (leaf) {
            case ResolvedPredicate.IntPredicate p -> canDrop(p.op(), p.value(),
                    StatisticsDecoder.decodeInt(stats.minValue()),
                    StatisticsDecoder.decodeInt(stats.maxValue()));
            case ResolvedPredicate.LongPredicate p -> canDrop(p.op(), p.value(),
                    StatisticsDecoder.decodeLong(stats.minValue()),
                    StatisticsDecoder.decodeLong(stats.maxValue()));
            case ResolvedPredicate.FloatPredicate p -> canDropFloat(p.op(), p.value(),
                    StatisticsDecoder.decodeFloat(stats.minValue()),
                    StatisticsDecoder.decodeFloat(stats.maxValue()));
            case ResolvedPredicate.DoublePredicate p -> canDropDouble(p.op(), p.value(),
                    StatisticsDecoder.decodeDouble(stats.minValue()),
                    StatisticsDecoder.decodeDouble(stats.maxValue()));
            case ResolvedPredicate.BooleanPredicate p -> canDrop(p.op(), p.value() ? 1 : 0,
                    StatisticsDecoder.decodeBoolean(stats.minValue()) ? 1 : 0,
                    StatisticsDecoder.decodeBoolean(stats.maxValue()) ? 1 : 0);
            case ResolvedPredicate.BinaryPredicate p -> {
                if (p.signed()) {
                    int cmpMin = BinaryComparator.compareSigned(p.value(), stats.minValue());
                    int cmpMax = BinaryComparator.compareSigned(p.value(), stats.maxValue());
                    yield canDropCompared(p.op(), cmpMin, cmpMax,
                            BinaryComparator.compareSigned(stats.minValue(), stats.maxValue()));
                }
                else {
                    int cmpMin = BinaryComparator.compareUnsigned(p.value(), stats.minValue());
                    int cmpMax = BinaryComparator.compareUnsigned(p.value(), stats.maxValue());
                    yield canDropCompared(p.op(), cmpMin, cmpMax,
                            BinaryComparator.compareUnsigned(stats.minValue(), stats.maxValue()));
                }
            }
            case ResolvedPredicate.IntInPredicate p -> canDropIntIn(p.values(),
                    StatisticsDecoder.decodeInt(stats.minValue()),
                    StatisticsDecoder.decodeInt(stats.maxValue()));
            case ResolvedPredicate.LongInPredicate p -> canDropLongIn(p.values(),
                    StatisticsDecoder.decodeLong(stats.minValue()),
                    StatisticsDecoder.decodeLong(stats.maxValue()));
            case ResolvedPredicate.BinaryInPredicate p -> canDropBinaryIn(p.values(),
                    stats.minValue(), stats.maxValue());
            case ResolvedPredicate.IsNullPredicate ignored -> false;
            case ResolvedPredicate.IsNotNullPredicate ignored -> false;
            case ResolvedPredicate.And ignored -> false;
            case ResolvedPredicate.Or ignored -> false;
        };
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
            if (BinaryComparator.compareUnsigned(value, min) >= 0
                    && BinaryComparator.compareUnsigned(value, max) <= 0) {
                return false;
            }
        }
        return true;
    }
}
