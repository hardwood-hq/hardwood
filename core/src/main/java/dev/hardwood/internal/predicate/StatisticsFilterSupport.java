/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import dev.hardwood.reader.FilterPredicate;

/// What each [dev.hardwood.reader.FilterPredicate.Operator] proves over an interval, as pure
/// functions of a probe value and a pair of bounds.
///
/// The bounds arrive already decoded and already established as comparable — [MinMaxStats] is
/// where a unit's statistics are read and where a pair that cannot be compared against is
/// turned away — so nothing here asks which column it is working for, or whether the interval
/// it was handed makes sense.
final class StatisticsFilterSupport {

    private StatisticsFilterSupport() {
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

    /// Determines if a range can be dropped given `FLOAT` or `FLOAT16` min/max statistics.
    ///
    /// The bounds are assumed usable — neither `NaN` nor inverted — which [MinMaxStats]
    /// establishes where it sources them.
    static boolean canDropFloat(FilterPredicate.Operator op, float value, float min, float max,
            boolean ieee754TotalOrder) {
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

    /// Determines if a range can be dropped given `DOUBLE` min/max statistics. See
    /// [#canDropFloat] for what the bounds are assumed to be.
    static boolean canDropDouble(FilterPredicate.Operator op, double value, double min, double max,
            boolean ieee754TotalOrder) {
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

    /// Determines if every value in `[min, max]` satisfies the operator, given
    /// integer-comparable min/max statistics. Works for int, long, boolean (mapped to 0/1).
    static boolean alwaysMatches(FilterPredicate.Operator op, long value, long min, long max) {
        return switch (op) {
            case EQ -> min == max && value == min;
            case NOT_EQ -> value < min || value > max;
            case LT -> max < value;
            case LT_EQ -> max <= value;
            case GT -> min > value;
            case GT_EQ -> min >= value;
        };
    }

    /// Determines if every value in `[min, max]` satisfies the operator, given pre-computed
    /// comparison results for binary values.
    ///
    /// @param cmpMin comparison of value vs min (negative if value < min)
    /// @param cmpMax comparison of value vs max (positive if value > max)
    /// @param minEqMax comparison of min vs max (0 if min == max)
    static boolean alwaysMatchesCompared(FilterPredicate.Operator op, int cmpMin, int cmpMax,
            int minEqMax) {
        return switch (op) {
            case EQ -> minEqMax == 0 && cmpMin == 0;
            case NOT_EQ -> cmpMin < 0 || cmpMax > 0;
            case LT -> cmpMax > 0;
            case LT_EQ -> cmpMax >= 0;
            case GT -> cmpMin < 0;
            case GT_EQ -> cmpMin <= 0;
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

    /// Whether the bounds prove every value in a floating-point `IN` list absent.
    ///
    /// Takes both widths over `double`: a `float` bound widens exactly, and a probe is never
    /// narrowed to the stored width, so one that no `float` can represent simply matches nothing.
    ///
    /// A `NaN` probe stops the list from pruning at all. Stored `NaN` values sit outside the
    /// min/max ordering, so no interval can prove one absent — where an unusable *bound* is
    /// caught when the unit is sourced, this is a property of the probe.
    static boolean canDropDoubleIn(double[] values, double min, double max, boolean ieee754TotalOrder) {
        // See canDropFloat: widen ±0 bounds under the type-defined ordering, leave them exact for
        // the unambiguous IEEE 754 total order.
        if (!ieee754TotalOrder) {
            min = (min == 0.0) ? -0.0 : min;
            max = (max == 0.0) ? 0.0 : max;
        }
        for (double value : values) {
            if (Double.isNaN(value)) {
                return false;
            }
        }
        for (double value : values) {
            if (Double.compare(value, min) >= 0 && Double.compare(value, max) <= 0) {
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

    // ==================== IN predicate always-match checks ====================
    // An IN predicate matches every row only in the single-point case: min == max
    // and that one value is a member of the set.

    static boolean alwaysMatchesIntIn(int[] values, int min, int max) {
        if (min != max) {
            return false;
        }
        for (int value : values) {
            if (value == min) {
                return true;
            }
        }
        return false;
    }

    static boolean alwaysMatchesLongIn(long[] values, long min, long max) {
        if (min != max) {
            return false;
        }
        for (long value : values) {
            if (value == min) {
                return true;
            }
        }
        return false;
    }

    static boolean alwaysMatchesBinaryIn(byte[][] values, byte[] min, byte[] max) {
        if (BinaryComparator.compareUnsigned(min, max) != 0) {
            return false;
        }
        for (byte[] value : values) {
            if (BinaryComparator.compareUnsigned(value, min) == 0) {
                return true;
            }
        }
        return false;
    }
}
