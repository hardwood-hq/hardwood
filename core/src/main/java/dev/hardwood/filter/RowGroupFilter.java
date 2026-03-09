/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.filter;

import java.nio.charset.StandardCharsets;
import java.util.List;

import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

/**
 * Filter predicates for row group and page-level filtering via statistics.
 *
 * <p>Usage example:</p>
 * <pre>{@code
 * import static dev.hardwood.filter.RowGroupFilter.*;
 *
 * RowGroupFilter filter = and(
 *     gt("fare_amount", 50.0),
 *     eq("vendor_id", 1)
 * );
 *
 * try (RowReader reader = fileReader.createRowReader(filter)) {
 *     while (reader.hasNext()) {
 *         reader.next();
 *         // only rows from matching row groups
 *     }
 * }
 * }</pre>
 *
 * <p>Filters work at two levels:</p>
 * <ul>
 *   <li><b>Row group level:</b> uses column chunk statistics (min/max) to skip entire row groups</li>
 *   <li><b>Page level:</b> uses column index (per-page min/max) to skip individual pages</li>
 * </ul>
 */
public sealed interface RowGroupFilter {

    /**
     * Evaluate this filter against a row group's statistics.
     *
     * @param rowGroup the row group metadata
     * @param schema the file schema (for column resolution)
     * @return {@code true} if the row group might contain matching rows (cannot be skipped),
     *         {@code false} if the row group definitely contains no matching rows
     */
    boolean canDrop(RowGroup rowGroup, FileSchema schema);

    /**
     * Evaluate this filter against a single page's statistics from the column index.
     *
     * @param minBytes per-page minimum value bytes
     * @param maxBytes per-page maximum value bytes
     * @param physicalType the column's physical type
     * @return {@code true} if the page can be dropped (definitely no matching rows),
     *         {@code false} if the page might contain matching rows
     */
    boolean canDropPage(byte[] minBytes, byte[] maxBytes, PhysicalType physicalType);

    /**
     * Returns the column name this filter operates on, or {@code null} for compound filters.
     */
    String columnName();

    // --- Factory methods ---

    static RowGroupFilter eq(String column, long value) {
        return new IntFilter(column, Operator.EQ, value);
    }

    static RowGroupFilter eq(String column, double value) {
        return new DoubleFilter(column, Operator.EQ, value);
    }

    static RowGroupFilter eq(String column, String value) {
        return new StringFilter(column, Operator.EQ, value);
    }

    static RowGroupFilter notEq(String column, long value) {
        return new IntFilter(column, Operator.NOT_EQ, value);
    }

    static RowGroupFilter notEq(String column, double value) {
        return new DoubleFilter(column, Operator.NOT_EQ, value);
    }

    static RowGroupFilter gt(String column, long value) {
        return new IntFilter(column, Operator.GT, value);
    }

    static RowGroupFilter gt(String column, double value) {
        return new DoubleFilter(column, Operator.GT, value);
    }

    static RowGroupFilter gtEq(String column, long value) {
        return new IntFilter(column, Operator.GT_EQ, value);
    }

    static RowGroupFilter gtEq(String column, double value) {
        return new DoubleFilter(column, Operator.GT_EQ, value);
    }

    static RowGroupFilter lt(String column, long value) {
        return new IntFilter(column, Operator.LT, value);
    }

    static RowGroupFilter lt(String column, double value) {
        return new DoubleFilter(column, Operator.LT, value);
    }

    static RowGroupFilter ltEq(String column, long value) {
        return new IntFilter(column, Operator.LT_EQ, value);
    }

    static RowGroupFilter ltEq(String column, double value) {
        return new DoubleFilter(column, Operator.LT_EQ, value);
    }

    static RowGroupFilter and(RowGroupFilter... filters) {
        return new AndFilter(List.of(filters));
    }

    static RowGroupFilter or(RowGroupFilter... filters) {
        return new OrFilter(List.of(filters));
    }

    static RowGroupFilter not(RowGroupFilter filter) {
        return new NotFilter(filter);
    }

    // --- Operator enum ---

    enum Operator {
        EQ, NOT_EQ, GT, GT_EQ, LT, LT_EQ
    }

    // --- Implementations ---

    record IntFilter(String columnName, Operator op, long value) implements RowGroupFilter {

        @Override
        public boolean canDrop(RowGroup rowGroup, FileSchema schema) {
            Statistics stats = findStats(rowGroup, schema, columnName);
            if (stats == null || !stats.hasMinMax()) {
                return false; // no stats, can't skip
            }

            ColumnSchema col = schema.getColumn(columnName);
            long min = StatisticsConverter.bytesToLong(stats.min(), col.type());
            long max = StatisticsConverter.bytesToLong(stats.max(), col.type());
            return evaluateIntRange(op, value, min, max);
        }

        @Override
        public boolean canDropPage(byte[] minBytes, byte[] maxBytes, PhysicalType physicalType) {
            long min = StatisticsConverter.bytesToLong(minBytes, physicalType);
            long max = StatisticsConverter.bytesToLong(maxBytes, physicalType);
            return evaluateIntRange(op, value, min, max);
        }
    }

    record DoubleFilter(String columnName, Operator op, double value) implements RowGroupFilter {

        @Override
        public boolean canDrop(RowGroup rowGroup, FileSchema schema) {
            Statistics stats = findStats(rowGroup, schema, columnName);
            if (stats == null || !stats.hasMinMax()) {
                return false;
            }

            ColumnSchema col = schema.getColumn(columnName);
            double min = StatisticsConverter.bytesToDouble(stats.min(), col.type());
            double max = StatisticsConverter.bytesToDouble(stats.max(), col.type());
            return evaluateDoubleRange(op, value, min, max);
        }

        @Override
        public boolean canDropPage(byte[] minBytes, byte[] maxBytes, PhysicalType physicalType) {
            double min = StatisticsConverter.bytesToDouble(minBytes, physicalType);
            double max = StatisticsConverter.bytesToDouble(maxBytes, physicalType);
            return evaluateDoubleRange(op, value, min, max);
        }
    }

    record StringFilter(String columnName, Operator op, String value) implements RowGroupFilter {

        @Override
        public boolean canDrop(RowGroup rowGroup, FileSchema schema) {
            Statistics stats = findStats(rowGroup, schema, columnName);
            if (stats == null || !stats.hasMinMax()) {
                return false;
            }

            byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
            return evaluateByteRange(op, valueBytes, stats.min(), stats.max());
        }

        @Override
        public boolean canDropPage(byte[] minBytes, byte[] maxBytes, PhysicalType physicalType) {
            byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
            return evaluateByteRange(op, valueBytes, minBytes, maxBytes);
        }
    }

    record AndFilter(List<RowGroupFilter> filters) implements RowGroupFilter {

        @Override
        public boolean canDrop(RowGroup rowGroup, FileSchema schema) {
            for (RowGroupFilter filter : filters) {
                if (filter.canDrop(rowGroup, schema)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean canDropPage(byte[] minBytes, byte[] maxBytes, PhysicalType physicalType) {
            return false; // compound filters don't operate on individual pages
        }

        @Override
        public String columnName() {
            return null;
        }
    }

    record OrFilter(List<RowGroupFilter> filters) implements RowGroupFilter {

        @Override
        public boolean canDrop(RowGroup rowGroup, FileSchema schema) {
            for (RowGroupFilter filter : filters) {
                if (!filter.canDrop(rowGroup, schema)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public boolean canDropPage(byte[] minBytes, byte[] maxBytes, PhysicalType physicalType) {
            return false;
        }

        @Override
        public String columnName() {
            return null;
        }
    }

    record NotFilter(RowGroupFilter delegate) implements RowGroupFilter {

        @Override
        public boolean canDrop(RowGroup rowGroup, FileSchema schema) {
            // NOT cannot safely determine drops from statistics alone
            // (e.g., NOT(gt(x, 5)) means x <= 5, which we could evaluate, but
            // it's complex for compound predicates). Be conservative.
            return false;
        }

        @Override
        public boolean canDropPage(byte[] minBytes, byte[] maxBytes, PhysicalType physicalType) {
            return false;
        }

        @Override
        public String columnName() {
            return delegate.columnName();
        }
    }

    // --- Internal helpers ---

    private static Statistics findStats(RowGroup rowGroup, FileSchema schema, String columnName) {
        ColumnSchema col = schema.getColumn(columnName);
        int colIndex = schema.getColumns().indexOf(col);
        if (colIndex < 0 || colIndex >= rowGroup.columns().size()) {
            return null;
        }
        return rowGroup.columns().get(colIndex).metaData().statistics();
    }

    private static boolean evaluateIntRange(Operator op, long value, long min, long max) {
        return switch (op) {
            case EQ -> value < min || value > max;
            case NOT_EQ -> min == max && min == value;
            case GT -> max <= value;
            case GT_EQ -> max < value;
            case LT -> min >= value;
            case LT_EQ -> min > value;
        };
    }

    private static boolean evaluateDoubleRange(Operator op, double value, double min, double max) {
        return switch (op) {
            case EQ -> value < min || value > max;
            case NOT_EQ -> min == max && Double.compare(min, value) == 0;
            case GT -> max <= value;
            case GT_EQ -> max < value;
            case LT -> min >= value;
            case LT_EQ -> min > value;
        };
    }

    private static boolean evaluateByteRange(Operator op, byte[] value, byte[] min, byte[] max) {
        int cmpMin = StatisticsConverter.compareBytes(value, min);
        int cmpMax = StatisticsConverter.compareBytes(value, max);
        return switch (op) {
            case EQ -> cmpMin < 0 || cmpMax > 0;
            case NOT_EQ -> StatisticsConverter.compareBytes(min, max) == 0 && cmpMin == 0;
            case GT -> StatisticsConverter.compareBytes(max, value) <= 0;
            case GT_EQ -> StatisticsConverter.compareBytes(max, value) < 0;
            case LT -> StatisticsConverter.compareBytes(min, value) >= 0;
            case LT_EQ -> StatisticsConverter.compareBytes(min, value) > 0;
        };
    }
}
