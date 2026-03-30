/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.util.Arrays;
import java.util.BitSet;

import dev.hardwood.reader.FilterPredicate;

/// Evaluates a [ResolvedPredicate] against a single row's values from the flat
/// cached arrays in [dev.hardwood.reader.AbstractRowReader].
///
/// Used for record-level filtering after page decoding, so that only rows
/// matching the predicate are returned to the caller.
public class RecordFilterEvaluator {

    /// Evaluates the predicate against all rows in a batch, returning a BitSet
    /// where set bits indicate matching rows. This enables batch-level iteration
    /// in `AbstractRowReader` instead of per-row `matches()` calls.
    ///
    /// @param predicate     the resolved predicate to evaluate
    /// @param batchSize     number of rows in the batch
    /// @param valueArrays   cached primitive arrays per projected column (e.g., `int[]`, `long[]`, `double[]`).
    /// @param nulls         null bitmaps per projected column
    /// @param columnMapping maps schema column index to projected array index; -1 if not projected
    /// @return BitSet with bits set for matching rows
    public static BitSet matchBatch(ResolvedPredicate predicate, int batchSize,
            Object[] valueArrays, BitSet[] nulls, int[] columnMapping) {
        BitSet result = new BitSet(batchSize);
        for (int i = 0; i < batchSize; i++) {
            if (matches(predicate, i, valueArrays, nulls, columnMapping)) {
                result.set(i);
            }
        }
        return result;
    }

    /// Returns `true` if the row at `rowIndex` matches the predicate.
    ///
    /// @param predicate     the resolved predicate to evaluate
    /// @param rowIndex      the row position within the current batch
    /// @param valueArrays   cached primitive arrays per projected column
    /// @param nulls         null bitmaps per projected column (may contain null entries for required columns)
    /// @param columnMapping maps schema column index to projected array index; -1 if not projected
    public static boolean matches(ResolvedPredicate predicate, int rowIndex,
            Object[] valueArrays, BitSet[] nulls, int[] columnMapping) {
        return switch (predicate) {
            case ResolvedPredicate.IntPredicate p -> matchesInt(p, rowIndex, valueArrays, nulls, columnMapping);
            case ResolvedPredicate.LongPredicate p -> matchesLong(p, rowIndex, valueArrays, nulls, columnMapping);
            case ResolvedPredicate.FloatPredicate p -> matchesFloat(p, rowIndex, valueArrays, nulls, columnMapping);
            case ResolvedPredicate.DoublePredicate p -> matchesDouble(p, rowIndex, valueArrays, nulls, columnMapping);
            case ResolvedPredicate.BooleanPredicate p -> matchesBoolean(p, rowIndex, valueArrays, nulls, columnMapping);
            case ResolvedPredicate.BinaryPredicate p -> matchesBinary(p, rowIndex, valueArrays, nulls, columnMapping);
            case ResolvedPredicate.IntInPredicate p -> matchesIntIn(p, rowIndex, valueArrays, nulls, columnMapping);
            case ResolvedPredicate.LongInPredicate p -> matchesLongIn(p, rowIndex, valueArrays, nulls, columnMapping);
            case ResolvedPredicate.BinaryInPredicate p -> matchesBinaryIn(p, rowIndex, valueArrays, nulls, columnMapping);
            case ResolvedPredicate.IsNullPredicate p -> {
                int cachedIndex = projectedIndex(p.columnIndex(), columnMapping);
                if (cachedIndex < 0) {
                    yield true; // column not in projection — conservative
                }
                BitSet columnNulls = nulls[cachedIndex];
                yield columnNulls != null && columnNulls.get(rowIndex);
            }
            case ResolvedPredicate.IsNotNullPredicate p -> {
                int cachedIndex = projectedIndex(p.columnIndex(), columnMapping);
                if (cachedIndex < 0) {
                    yield true; // column not in projection — conservative
                }
                BitSet columnNulls = nulls[cachedIndex];
                yield columnNulls == null || !columnNulls.get(rowIndex);
            }
            case ResolvedPredicate.And and -> {
                for (ResolvedPredicate child : and.children()) {
                    if (!matches(child, rowIndex, valueArrays, nulls, columnMapping)) {
                        yield false;
                    }
                }
                yield true;
            }
            case ResolvedPredicate.Or or -> {
                for (ResolvedPredicate child : or.children()) {
                    if (matches(child, rowIndex, valueArrays, nulls, columnMapping)) {
                        yield true;
                    }
                }
                yield false;
            }
        };
    }

    /// Maps a schema column index to the projected array index using the column mapping.
    /// Returns -1 if the column is not in the projection.
    private static int projectedIndex(int schemaColumnIndex, int[] columnMapping) {
        if (schemaColumnIndex < 0 || schemaColumnIndex >= columnMapping.length) {
            return -1;
        }
        return columnMapping[schemaColumnIndex];
    }

    private static boolean matchesInt(ResolvedPredicate.IntPredicate p, int rowIndex,
            Object[] valueArrays, BitSet[] nulls, int[] columnMapping) {
        int cachedIndex = projectedIndex(p.columnIndex(), columnMapping);
        if (cachedIndex < 0 || !(valueArrays[cachedIndex] instanceof int[])) {
            return true; // column not in projection — conservative
        }
        if (isNull(nulls, cachedIndex, rowIndex)) {
            return false;
        }
        int recordValue = ((int[]) valueArrays[cachedIndex])[rowIndex];
        return compareInt(p.op(), recordValue, p.value());
    }

    private static boolean matchesLong(ResolvedPredicate.LongPredicate p, int rowIndex,
            Object[] valueArrays, BitSet[] nulls, int[] columnMapping) {
        int cachedIndex = projectedIndex(p.columnIndex(), columnMapping);
        if (cachedIndex < 0 || !(valueArrays[cachedIndex] instanceof long[])) {
            return true;
        }
        if (isNull(nulls, cachedIndex, rowIndex)) {
            return false;
        }
        long recordValue = ((long[]) valueArrays[cachedIndex])[rowIndex];
        return compareLong(p.op(), recordValue, p.value());
    }

    private static boolean matchesFloat(ResolvedPredicate.FloatPredicate p, int rowIndex,
            Object[] valueArrays, BitSet[] nulls, int[] columnMapping) {
        int cachedIndex = projectedIndex(p.columnIndex(), columnMapping);
        if (cachedIndex < 0 || !(valueArrays[cachedIndex] instanceof float[])) {
            return true;
        }
        if (isNull(nulls, cachedIndex, rowIndex)) {
            return false;
        }
        float recordValue = ((float[]) valueArrays[cachedIndex])[rowIndex];
        return compareFloat(p.op(), recordValue, p.value());
    }

    private static boolean matchesDouble(ResolvedPredicate.DoublePredicate p, int rowIndex,
            Object[] valueArrays, BitSet[] nulls, int[] columnMapping) {
        int cachedIndex = projectedIndex(p.columnIndex(), columnMapping);
        if (cachedIndex < 0 || !(valueArrays[cachedIndex] instanceof double[])) {
            return true;
        }
        if (isNull(nulls, cachedIndex, rowIndex)) {
            return false;
        }
        double recordValue = ((double[]) valueArrays[cachedIndex])[rowIndex];
        return compareDouble(p.op(), recordValue, p.value());
    }

    private static boolean matchesBoolean(ResolvedPredicate.BooleanPredicate p, int rowIndex,
            Object[] valueArrays, BitSet[] nulls, int[] columnMapping) {
        int cachedIndex = projectedIndex(p.columnIndex(), columnMapping);
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

    private static boolean matchesBinary(ResolvedPredicate.BinaryPredicate p, int rowIndex,
            Object[] valueArrays, BitSet[] nulls, int[] columnMapping) {
        int cachedIndex = projectedIndex(p.columnIndex(), columnMapping);
        if (cachedIndex < 0 || !(valueArrays[cachedIndex] instanceof byte[][])) {
            return true;
        }
        if (isNull(nulls, cachedIndex, rowIndex)) {
            return false;
        }
        byte[] recordValue = ((byte[][]) valueArrays[cachedIndex])[rowIndex];
        int cmp = p.signed()
                ? BinaryComparator.compareSigned(recordValue, p.value())
                : BinaryComparator.compareUnsigned(recordValue, p.value());
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

    private static boolean matchesIntIn(ResolvedPredicate.IntInPredicate p, int rowIndex,
            Object[] valueArrays, BitSet[] nulls, int[] columnMapping) {
        int cachedIndex = projectedIndex(p.columnIndex(), columnMapping);
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

    private static boolean matchesLongIn(ResolvedPredicate.LongInPredicate p, int rowIndex,
            Object[] valueArrays, BitSet[] nulls, int[] columnMapping) {
        int cachedIndex = projectedIndex(p.columnIndex(), columnMapping);
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

    private static boolean matchesBinaryIn(ResolvedPredicate.BinaryInPredicate p, int rowIndex,
            Object[] valueArrays, BitSet[] nulls, int[] columnMapping) {
        int cachedIndex = projectedIndex(p.columnIndex(), columnMapping);
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
