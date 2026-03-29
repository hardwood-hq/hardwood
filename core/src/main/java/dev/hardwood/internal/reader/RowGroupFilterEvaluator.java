/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.List;

import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.metadata.Statistics;
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
import dev.hardwood.schema.FileSchema;

/// Evaluates filter predicates against row group statistics to determine
/// whether a row group can be skipped.
///
/// Uses a conservative approach: if statistics are absent for a column,
/// the row group is never dropped (it may contain matching rows).
public class RowGroupFilterEvaluator {

    /// Determines whether a row group can be skipped based on the given filter predicate.
    ///
    /// @param predicate the filter predicate to evaluate
    /// @param rowGroup the row group to check
    /// @param schema the file schema
    /// @return `true` if the row group can be safely skipped (no matching rows),
    ///         `false` if it may contain matching rows
    public static boolean canDropRowGroup(FilterPredicate predicate, RowGroup rowGroup, FileSchema schema) {
        return switch (predicate) {
            case IntColumnPredicate p -> evaluateInt(p, rowGroup, schema);
            case LongColumnPredicate p -> evaluateLong(p, rowGroup, schema);
            case FloatColumnPredicate p -> evaluateFloat(p, rowGroup, schema);
            case DoubleColumnPredicate p -> evaluateDouble(p, rowGroup, schema);
            case BooleanColumnPredicate p -> evaluateBoolean(p, rowGroup, schema);
            case BinaryColumnPredicate p -> evaluateBinary(p, rowGroup, schema);
            case IntInPredicate p -> evaluateIntIn(p, rowGroup, schema);
            case LongInPredicate p -> evaluateLongIn(p, rowGroup, schema);
            case BinaryInPredicate p -> evaluateBinaryIn(p, rowGroup, schema);
            case And a -> {
                for (FilterPredicate f : a.filters()) {
                    if (canDropRowGroup(f, rowGroup, schema)) {
                        yield true;
                    }
                }
                yield false;
            }
            case Or o -> {
                for (FilterPredicate f : o.filters()) {
                    if (!canDropRowGroup(f, rowGroup, schema)) {
                        yield false;
                    }
                }
                yield true;
            }
            // NOT cannot safely determine drops from statistics alone; be conservative.
            case Not ignored -> false;
        };
    }

    private static Statistics findStatistics(String columnName, RowGroup rowGroup, FileSchema schema) {
        int columnIndex = resolveColumnIndex(columnName, rowGroup, schema);
        if (columnIndex < 0) {
            return null;
        }
        ColumnChunk chunk = rowGroup.columns().get(columnIndex);
        return chunk.metaData().statistics();
    }

    /// Resolves a column name to its index in the row group, trying exact leaf-name
    /// lookup first, then falling back to path-based matching for nested/repeated columns.
    ///
    /// @return the column index, or -1 if not found
    static int resolveColumnIndex(String columnName, RowGroup rowGroup, FileSchema schema) {
        int columnIndex = findColumnIndex(columnName, schema);
        if (columnIndex < 0) {
            columnIndex = findColumnIndexByPath(columnName, rowGroup);
        }
        if (columnIndex < 0 || columnIndex >= rowGroup.columns().size()) {
            return -1;
        }
        return columnIndex;
    }

    private static int findColumnIndex(String columnName, FileSchema schema) {
        try {
            return schema.getColumn(columnName).columnIndex();
        }
        catch (IllegalArgumentException e) {
            return -1;
        }
    }

    private static int findColumnIndexByPath(String columnName, RowGroup rowGroup) {
        List<ColumnChunk> columns = rowGroup.columns();
        for (int i = 0; i < columns.size(); i++) {
            var path = columns.get(i).metaData().pathInSchema();
            if (path.isEmpty()) {
                continue;
            }
            if (path.matchesDottedName(columnName)) {
                return i;
            }
            // Match top-level name for repeated columns (e.g. "scores" matches ["scores", "list", "element"])
            if (path.topLevelName().equals(columnName)) {
                return i;
            }
        }
        return -1;
    }

    // ==================== INT32 ====================

    private static boolean evaluateInt(IntColumnPredicate p, RowGroup rowGroup, FileSchema schema) {
        Statistics stats = findStatistics(p.column(), rowGroup, schema);
        if (stats == null || stats.minValue() == null || stats.maxValue() == null) {
            return false;
        }
        int min = StatisticsDecoder.decodeInt(stats.minValue());
        int max = StatisticsDecoder.decodeInt(stats.maxValue());
        int value = p.value();
        return canDrop(p.op(), value, min, max);
    }

    // ==================== INT64 ====================

    private static boolean evaluateLong(LongColumnPredicate p, RowGroup rowGroup, FileSchema schema) {
        Statistics stats = findStatistics(p.column(), rowGroup, schema);
        if (stats == null || stats.minValue() == null || stats.maxValue() == null) {
            return false;
        }
        long min = StatisticsDecoder.decodeLong(stats.minValue());
        long max = StatisticsDecoder.decodeLong(stats.maxValue());
        long value = p.value();
        return canDrop(p.op(), value, min, max);
    }

    // ==================== FLOAT ====================

    private static boolean evaluateFloat(FloatColumnPredicate p, RowGroup rowGroup, FileSchema schema) {
        Statistics stats = findStatistics(p.column(), rowGroup, schema);
        if (stats == null || stats.minValue() == null || stats.maxValue() == null) {
            return false;
        }
        float min = StatisticsDecoder.decodeFloat(stats.minValue());
        float max = StatisticsDecoder.decodeFloat(stats.maxValue());
        float value = p.value();
        return canDropFloat(p.op(), value, min, max);
    }

    // ==================== DOUBLE ====================

    private static boolean evaluateDouble(DoubleColumnPredicate p, RowGroup rowGroup, FileSchema schema) {
        Statistics stats = findStatistics(p.column(), rowGroup, schema);
        if (stats == null || stats.minValue() == null || stats.maxValue() == null) {
            return false;
        }
        double min = StatisticsDecoder.decodeDouble(stats.minValue());
        double max = StatisticsDecoder.decodeDouble(stats.maxValue());
        double value = p.value();
        return canDropDouble(p.op(), value, min, max);
    }

    // ==================== BOOLEAN ====================

    private static boolean evaluateBoolean(BooleanColumnPredicate p, RowGroup rowGroup, FileSchema schema) {
        Statistics stats = findStatistics(p.column(), rowGroup, schema);
        if (stats == null || stats.minValue() == null || stats.maxValue() == null) {
            return false;
        }
        boolean min = StatisticsDecoder.decodeBoolean(stats.minValue());
        boolean max = StatisticsDecoder.decodeBoolean(stats.maxValue());
        boolean value = p.value();
        int minInt = min ? 1 : 0;
        int maxInt = max ? 1 : 0;
        int valueInt = value ? 1 : 0;
        return canDrop(p.op(), valueInt, minInt, maxInt);
    }

    // ==================== BINARY (byte[]) ====================

    private static boolean evaluateBinary(BinaryColumnPredicate p, RowGroup rowGroup, FileSchema schema) {
        Statistics stats = findStatistics(p.column(), rowGroup, schema);
        if (stats == null || stats.minValue() == null || stats.maxValue() == null) {
            return false;
        }
        byte[] min = stats.minValue();
        byte[] max = stats.maxValue();
        byte[] value = p.value();
        int cmpMin = StatisticsDecoder.compareBinary(value, min);
        int cmpMax = StatisticsDecoder.compareBinary(value, max);
        return canDropCompared(p.op(), cmpMin, cmpMax,
                StatisticsDecoder.compareBinary(min, max));
    }

    private static boolean evaluateIntIn(IntInPredicate p, RowGroup rowGroup, FileSchema schema) {
        Statistics stats = findStatistics(p.column(), rowGroup, schema);
        if (stats == null || stats.minValue() == null || stats.maxValue() == null) {
            return false;
        }
        int min = StatisticsDecoder.decodeInt(stats.minValue());
        int max = StatisticsDecoder.decodeInt(stats.maxValue());
        return canDropIntIn(p.values(), min, max);
    }

    private static boolean evaluateLongIn(LongInPredicate p, RowGroup rowGroup, FileSchema schema) {
        Statistics stats = findStatistics(p.column(), rowGroup, schema);
        if (stats == null || stats.minValue() == null || stats.maxValue() == null) {
            return false;
        }
        long min = StatisticsDecoder.decodeLong(stats.minValue());
        long max = StatisticsDecoder.decodeLong(stats.maxValue());
        return canDropLongIn(p.values(), min, max);
    }

    private static boolean evaluateBinaryIn(BinaryInPredicate p, RowGroup rowGroup, FileSchema schema) {
        Statistics stats = findStatistics(p.column(), rowGroup, schema);
        if (stats == null || stats.minValue() == null || stats.maxValue() == null) {
            return false;
        }
        byte[] min = stats.minValue();
        byte[] max = stats.maxValue();
        return canDropBinaryIn(p.values(), min, max);
    }

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

    // ==================== Generic comparison logic ====================

    /// Determines if a range can be dropped given integer-comparable min/max statistics.
    /// Works for int, long, boolean (mapped to 0/1).
    /// Shared with [PageFilterEvaluator] for page-level filtering.
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
}
