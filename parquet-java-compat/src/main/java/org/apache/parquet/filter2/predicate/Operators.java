/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.parquet.filter2.predicate;

import org.apache.parquet.hadoop.metadata.ColumnPath;

/// Shim for parquet-java's `Operators` class.
///
/// Contains column types, comparison predicates, and logical combinators
/// that mirror the upstream API. The shim covers the core comparison operators
/// (`eq`, `notEq`, `lt`, `ltEq`, `gt`, `gtEq`)
/// and logical combinators (`and`, `or`, `not`).
public final class Operators {

    private Operators() {
    }

    // ==================== Marker interfaces ====================

    public interface SupportsEqNotEq {
    }

    public interface SupportsLtGt extends SupportsEqNotEq {
    }

    // ==================== Column types ====================

    public abstract static class Column<T extends Comparable<T>> {

        private final ColumnPath columnPath;
        private final Class<T> columnType;

        protected Column(ColumnPath columnPath, Class<T> columnType) {
            this.columnPath = columnPath;
            this.columnType = columnType;
        }

        public ColumnPath getColumnPath() {
            return columnPath;
        }

        public Class<T> getColumnType() {
            return columnType;
        }

        @Override
        public String toString() {
            return "column(" + columnPath.toDotString() + ")";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (!(o instanceof Column<?> other))
                return false;
            return columnPath.equals(other.columnPath) && columnType.equals(other.columnType);
        }

        @Override
        public int hashCode() {
            return 31 * columnPath.hashCode() + columnType.hashCode();
        }
    }

    public static final class IntColumn extends Column<Integer> implements SupportsLtGt {

        IntColumn(ColumnPath columnPath) {
            super(columnPath, Integer.class);
        }
    }

    public static final class LongColumn extends Column<Long> implements SupportsLtGt {

        LongColumn(ColumnPath columnPath) {
            super(columnPath, Long.class);
        }
    }

    public static final class FloatColumn extends Column<Float> implements SupportsLtGt {

        FloatColumn(ColumnPath columnPath) {
            super(columnPath, Float.class);
        }
    }

    public static final class DoubleColumn extends Column<Double> implements SupportsLtGt {

        DoubleColumn(ColumnPath columnPath) {
            super(columnPath, Double.class);
        }
    }

    public static final class BooleanColumn extends Column<Boolean> implements SupportsEqNotEq {

        BooleanColumn(ColumnPath columnPath) {
            super(columnPath, Boolean.class);
        }
    }

    public static final class BinaryColumn extends Column<org.apache.parquet.io.api.Binary>
            implements SupportsLtGt {

        BinaryColumn(ColumnPath columnPath) {
            super(columnPath, org.apache.parquet.io.api.Binary.class);
        }
    }

    // ==================== Base for column predicates ====================

    static abstract class ColumnFilterPredicate<T extends Comparable<T>> implements FilterPredicate {

        private final Column<T> column;
        private final T value;

        protected ColumnFilterPredicate(Column<T> column, T value) {
            this.column = column;
            this.value = value;
        }

        public Column<T> getColumn() {
            return column;
        }

        public T getValue() {
            return value;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(" + column.getColumnPath().toDotString() + ", " + value + ")";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            ColumnFilterPredicate<?> that = (ColumnFilterPredicate<?>) o;
            return column.equals(that.column) && java.util.Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return 31 * column.hashCode() + java.util.Objects.hashCode(value);
        }
    }

    // ==================== Comparison predicates ====================

    public static final class Eq<T extends Comparable<T>> extends ColumnFilterPredicate<T> {

        public Eq(Column<T> column, T value) {
            super(column, value);
        }
    }

    public static final class NotEq<T extends Comparable<T>> extends ColumnFilterPredicate<T> {

        NotEq(Column<T> column, T value) {
            super(column, value);
        }
    }

    public static final class Lt<T extends Comparable<T>> extends ColumnFilterPredicate<T> {

        Lt(Column<T> column, T value) {
            super(column, value);
        }
    }

    public static final class LtEq<T extends Comparable<T>> extends ColumnFilterPredicate<T> {

        LtEq(Column<T> column, T value) {
            super(column, value);
        }
    }

    public static final class Gt<T extends Comparable<T>> extends ColumnFilterPredicate<T> {

        Gt(Column<T> column, T value) {
            super(column, value);
        }
    }

    public static final class GtEq<T extends Comparable<T>> extends ColumnFilterPredicate<T> {

        GtEq(Column<T> column, T value) {
            super(column, value);
        }
    }

    // ==================== Logical combinators ====================

    static abstract class BinaryLogicalFilterPredicate implements FilterPredicate {

        private final FilterPredicate left;
        private final FilterPredicate right;

        protected BinaryLogicalFilterPredicate(FilterPredicate left, FilterPredicate right) {
            this.left = left;
            this.right = right;
        }

        public FilterPredicate getLeft() {
            return left;
        }

        public FilterPredicate getRight() {
            return right;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "(" + left + ", " + right + ")";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            BinaryLogicalFilterPredicate that = (BinaryLogicalFilterPredicate) o;
            return left.equals(that.left) && right.equals(that.right);
        }

        @Override
        public int hashCode() {
            return 31 * left.hashCode() + right.hashCode();
        }
    }

    public static final class And extends BinaryLogicalFilterPredicate {

        And(FilterPredicate left, FilterPredicate right) {
            super(left, right);
        }
    }

    public static final class Or extends BinaryLogicalFilterPredicate {

        Or(FilterPredicate left, FilterPredicate right) {
            super(left, right);
        }
    }

    public static final class Not implements FilterPredicate {

        private final FilterPredicate predicate;

        Not(FilterPredicate predicate) {
            this.predicate = predicate;
        }

        public FilterPredicate getPredicate() {
            return predicate;
        }

        @Override
        public String toString() {
            return "Not(" + predicate + ")";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (!(o instanceof Not that))
                return false;
            return predicate.equals(that.predicate);
        }

        @Override
        public int hashCode() {
            return ~predicate.hashCode();
        }
    }
}
