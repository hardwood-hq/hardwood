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
                    StatisticsDecoder.decodeFloat(stats.maxValue()), p.ieee754TotalOrder());
            case ResolvedPredicate.Float16Predicate p -> canDropFloat(p.op(), p.value(),
                    StatisticsDecoder.decodeFloat16(stats.minValue()),
                    StatisticsDecoder.decodeFloat16(stats.maxValue()), p.ieee754TotalOrder());
            case ResolvedPredicate.DoublePredicate p -> canDropDouble(p.op(), p.value(),
                    StatisticsDecoder.decodeDouble(stats.minValue()),
                    StatisticsDecoder.decodeDouble(stats.maxValue()), p.ieee754TotalOrder());
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
            case ResolvedPredicate.GeospatialPredicate ignored -> false;
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

    static boolean canDropFloat(FilterPredicate.Operator op, float value, float min, float max,
            boolean ieee754TotalOrder) {
        // The Parquet spec forbids writing NaN to statistics min/max, but older / buggy writers
        // have produced such bounds. NaN sorts above every finite value in Float.compare's total
        // order, so applying the range checks below would silently prune row groups / pages that
        // hold matching finite rows. Treat NaN bounds as no-bound and never prune.
        if (Float.isNaN(min) || Float.isNaN(max)) {
            return false;
        }
        // Under the type-defined ordering the spec leaves +0/-0 ambiguous: a +0 min may hide -0, a
        // -0 max may hide +0. Float.compare's total order separates them (-0 < +0), which could
        // wrongly drop the opposite zero, so widen each zero bound to its total-order extreme. The
        // IEEE 754 total order is unambiguous, so its bounds are already exact and left as-is.
        if (!ieee754TotalOrder) {
            min = (min == 0.0f) ? -0.0f : min;
            max = (max == 0.0f) ? 0.0f : max;
        }
        return switch (op) {
            case EQ -> Float.compare(value, min) < 0 || Float.compare(value, max) > 0;
            case NOT_EQ -> Float.compare(min, max) == 0 && Float.compare(value, min) == 0;
            case LT -> Float.compare(min, value) >= 0;
            case LT_EQ -> Float.compare(min, value) > 0;
            case GT -> Float.compare(max, value) <= 0;
            case GT_EQ -> Float.compare(max, value) < 0;
        };
    }

    static boolean canDropDouble(FilterPredicate.Operator op, double value, double min, double max,
            boolean ieee754TotalOrder) {
        if (Double.isNaN(min) || Double.isNaN(max)) {
            return false;
        }
        // See canDropFloat: widen ±0 bounds under the type-defined ordering, leave them exact for
        // the unambiguous IEEE 754 total order.
        if (!ieee754TotalOrder) {
            min = (min == 0.0) ? -0.0 : min;
            max = (max == 0.0) ? 0.0 : max;
        }
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
