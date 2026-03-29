/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.Arrays;
import java.util.BitSet;

import dev.hardwood.internal.util.StringToIntMap;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.FilterPredicate.And;
import dev.hardwood.reader.FilterPredicate.BinaryColumnPredicate;
import dev.hardwood.reader.FilterPredicate.BinaryInPredicate;
import dev.hardwood.reader.FilterPredicate.BooleanColumnPredicate;
import dev.hardwood.reader.FilterPredicate.DoubleColumnPredicate;
import dev.hardwood.reader.FilterPredicate.FloatColumnPredicate;
import dev.hardwood.reader.FilterPredicate.IntColumnPredicate;
import dev.hardwood.reader.FilterPredicate.IntInPredicate;
import dev.hardwood.reader.FilterPredicate.LongColumnPredicate;
import dev.hardwood.reader.FilterPredicate.LongInPredicate;
import dev.hardwood.reader.FilterPredicate.Not;
import dev.hardwood.reader.FilterPredicate.Or;

/// Evaluates a [FilterPredicate] against a single row's values from the flat
/// cached arrays in [dev.hardwood.reader.AbstractRowReader].
///
/// Used for record-level filtering after page decoding, so that only rows
/// matching the predicate are returned to the caller.
public class RecordFilterEvaluator {

    /// Returns `true` if the row at `rowIndex` matches the predicate.
    ///
    /// @param predicate   the filter predicate to evaluate
    /// @param rowIndex    the row position within the current batch
    /// @param valueArrays cached primitive arrays per projected column
    /// @param nulls       null bitmaps per projected column (may contain null entries for required columns)
    /// @param nameCache   column name → projected index mapping
    public static boolean matches(FilterPredicate predicate, int rowIndex,
            Object[] valueArrays, BitSet[] nulls, StringToIntMap nameCache) {
        return switch (predicate) {
            case IntColumnPredicate p -> matchesInt(p, rowIndex, valueArrays, nulls, nameCache);
            case LongColumnPredicate p -> matchesLong(p, rowIndex, valueArrays, nulls, nameCache);
            case FloatColumnPredicate p -> matchesFloat(p, rowIndex, valueArrays, nulls, nameCache);
            case DoubleColumnPredicate p -> matchesDouble(p, rowIndex, valueArrays, nulls, nameCache);
            case BooleanColumnPredicate p -> matchesBoolean(p, rowIndex, valueArrays, nulls, nameCache);
            case BinaryColumnPredicate p -> matchesBinary(p, rowIndex, valueArrays, nulls, nameCache);
            case IntInPredicate p -> matchesIntIn(p, rowIndex, valueArrays, nulls, nameCache);
            case LongInPredicate p -> matchesLongIn(p, rowIndex, valueArrays, nulls, nameCache);
            case BinaryInPredicate p -> matchesBinaryIn(p, rowIndex, valueArrays, nulls, nameCache);
            case And and -> {
                for (FilterPredicate filter : and.filters()) {
                    if (!matches(filter, rowIndex, valueArrays, nulls, nameCache)) {
                        yield false;
                    }
                }
                yield true;
            }
            case Or or -> {
                for (FilterPredicate filter : or.filters()) {
                    if (matches(filter, rowIndex, valueArrays, nulls, nameCache)) {
                        yield true;
                    }
                }
                yield false;
            }
            case Not n -> !matches(n.delegate(), rowIndex, valueArrays, nulls, nameCache);
        };
    }

    private static boolean matchesInt(IntColumnPredicate p, int rowIndex,
            Object[] valueArrays, BitSet[] nulls, StringToIntMap nameCache) {
        int cachedIndex = nameCache.get(p.column());
        if (cachedIndex < 0 || !(valueArrays[cachedIndex] instanceof int[])) {
            return true; // column not in projection — conservative
        }
        if (isNull(nulls, cachedIndex, rowIndex)) {
            return false;
        }
        int recordValue = ((int[]) valueArrays[cachedIndex])[rowIndex];
        return compareInt(p.op(), recordValue, p.value());
    }

    private static boolean matchesLong(LongColumnPredicate p, int rowIndex,
            Object[] valueArrays, BitSet[] nulls, StringToIntMap nameCache) {
        int cachedIndex = nameCache.get(p.column());
        if (cachedIndex < 0 || !(valueArrays[cachedIndex] instanceof long[])) {
            return true;
        }
        if (isNull(nulls, cachedIndex, rowIndex)) {
            return false;
        }
        long recordValue = ((long[]) valueArrays[cachedIndex])[rowIndex];
        return compareLong(p.op(), recordValue, p.value());
    }

    private static boolean matchesFloat(FloatColumnPredicate p, int rowIndex,
            Object[] valueArrays, BitSet[] nulls, StringToIntMap nameCache) {
        int cachedIndex = nameCache.get(p.column());
        if (cachedIndex < 0 || !(valueArrays[cachedIndex] instanceof float[])) {
            return true;
        }
        if (isNull(nulls, cachedIndex, rowIndex)) {
            return false;
        }
        float recordValue = ((float[]) valueArrays[cachedIndex])[rowIndex];
        return compareFloat(p.op(), recordValue, p.value());
    }

    private static boolean matchesDouble(DoubleColumnPredicate p, int rowIndex,
            Object[] valueArrays, BitSet[] nulls, StringToIntMap nameCache) {
        int cachedIndex = nameCache.get(p.column());
        if (cachedIndex < 0 || !(valueArrays[cachedIndex] instanceof double[])) {
            return true;
        }
        if (isNull(nulls, cachedIndex, rowIndex)) {
            return false;
        }
        double recordValue = ((double[]) valueArrays[cachedIndex])[rowIndex];
        return compareDouble(p.op(), recordValue, p.value());
    }

    private static boolean matchesBoolean(BooleanColumnPredicate p, int rowIndex,
            Object[] valueArrays, BitSet[] nulls, StringToIntMap nameCache) {
        int cachedIndex = nameCache.get(p.column());
        if (cachedIndex < 0 || !(valueArrays[cachedIndex] instanceof boolean[])) {
            return true;
        }
        if (isNull(nulls, cachedIndex, rowIndex)) {
            return false;
        }
        boolean recordValue = ((boolean[]) valueArrays[cachedIndex])[rowIndex];
        return switch (p.op()) {
            case EQ -> recordValue == p.value();
            case NOT_EQ -> recordValue != p.value();
            default -> true; // LT/GT not meaningful for boolean — conservative
        };
    }

    private static boolean matchesBinary(BinaryColumnPredicate p, int rowIndex,
            Object[] valueArrays, BitSet[] nulls, StringToIntMap nameCache) {
        int cachedIndex = nameCache.get(p.column());
        if (cachedIndex < 0 || !(valueArrays[cachedIndex] instanceof byte[][])) {
            return true;
        }
        if (isNull(nulls, cachedIndex, rowIndex)) {
            return false;
        }
        byte[] recordValue = ((byte[][]) valueArrays[cachedIndex])[rowIndex];
        int cmp = StatisticsDecoder.compareBinary(recordValue, p.value());
        return switch (p.op()) {
            case EQ -> cmp == 0;
            case NOT_EQ -> cmp != 0;
            case LT -> cmp < 0;
            case LT_EQ -> cmp <= 0;
            case GT -> cmp > 0;
            case GT_EQ -> cmp >= 0;
        };
    }

    // ==================== IN predicate evaluation ====================

    private static boolean matchesIntIn(IntInPredicate p, int rowIndex,
            Object[] valueArrays, BitSet[] nulls, StringToIntMap nameCache) {
        int cachedIndex = nameCache.get(p.column());
        if (cachedIndex < 0 || !(valueArrays[cachedIndex] instanceof int[])) {
            return true;
        }
        if (isNull(nulls, cachedIndex, rowIndex)) {
            return false;
        }
        int recordValue = ((int[]) valueArrays[cachedIndex])[rowIndex];
        for (int v : p.values()) {
            if (recordValue == v) {
                return true;
            }
        }
        return false;
    }

    private static boolean matchesLongIn(LongInPredicate p, int rowIndex,
            Object[] valueArrays, BitSet[] nulls, StringToIntMap nameCache) {
        int cachedIndex = nameCache.get(p.column());
        if (cachedIndex < 0 || !(valueArrays[cachedIndex] instanceof long[])) {
            return true;
        }
        if (isNull(nulls, cachedIndex, rowIndex)) {
            return false;
        }
        long recordValue = ((long[]) valueArrays[cachedIndex])[rowIndex];
        for (long v : p.values()) {
            if (recordValue == v) {
                return true;
            }
        }
        return false;
    }

    private static boolean matchesBinaryIn(BinaryInPredicate p, int rowIndex,
            Object[] valueArrays, BitSet[] nulls, StringToIntMap nameCache) {
        int cachedIndex = nameCache.get(p.column());
        if (cachedIndex < 0 || !(valueArrays[cachedIndex] instanceof byte[][])) {
            return true;
        }
        if (isNull(nulls, cachedIndex, rowIndex)) {
            return false;
        }
        byte[] recordValue = ((byte[][]) valueArrays[cachedIndex])[rowIndex];
        for (byte[] v : p.values()) {
            if (Arrays.equals(recordValue, v)) {
                return true;
            }
        }
        return false;
    }

    private static boolean compareInt(FilterPredicate.Operator op, int recordValue, int predicateValue) {
        return switch (op) {
            case EQ -> recordValue == predicateValue;
            case NOT_EQ -> recordValue != predicateValue;
            case LT -> recordValue < predicateValue;
            case LT_EQ -> recordValue <= predicateValue;
            case GT -> recordValue > predicateValue;
            case GT_EQ -> recordValue >= predicateValue;
        };
    }

    private static boolean compareLong(FilterPredicate.Operator op, long recordValue, long predicateValue) {
        return switch (op) {
            case EQ -> recordValue == predicateValue;
            case NOT_EQ -> recordValue != predicateValue;
            case LT -> recordValue < predicateValue;
            case LT_EQ -> recordValue <= predicateValue;
            case GT -> recordValue > predicateValue;
            case GT_EQ -> recordValue >= predicateValue;
        };
    }

    private static boolean compareFloat(FilterPredicate.Operator op, float recordValue, float predicateValue) {
        int cmp = Float.compare(recordValue, predicateValue);
        return switch (op) {
            case EQ -> cmp == 0;
            case NOT_EQ -> cmp != 0;
            case LT -> cmp < 0;
            case LT_EQ -> cmp <= 0;
            case GT -> cmp > 0;
            case GT_EQ -> cmp >= 0;
        };
    }

    private static boolean compareDouble(FilterPredicate.Operator op, double recordValue, double predicateValue) {
        int cmp = Double.compare(recordValue, predicateValue);
        return switch (op) {
            case EQ -> cmp == 0;
            case NOT_EQ -> cmp != 0;
            case LT -> cmp < 0;
            case LT_EQ -> cmp <= 0;
            case GT -> cmp > 0;
            case GT_EQ -> cmp >= 0;
        };
    }

    private static boolean isNull(BitSet[] nulls, int columnIndex, int rowIndex) {
        BitSet columnNulls = nulls[columnIndex];
        return columnNulls != null && columnNulls.get(rowIndex);
    }
}
