/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.parquet.filter2.predicate;

import org.apache.parquet.hadoop.metadata.ColumnPath;

/// Shim for parquet-java's `FilterApi`.
///
/// Provides static factory methods for creating column references and
/// filter predicates. Usage mirrors the upstream API:
///
/// ```java
/// import static org.apache.parquet.filter2.predicate.FilterApi.*;
///
/// FilterPredicate pred = and(
///     gtEq(longColumn("id"), 2L),
///     eq(intColumn("status"), 1)
/// );
/// ```
public final class FilterApi {

    private FilterApi() {
    }

    // ==================== Column selectors ====================

    public static Operators.IntColumn intColumn(String columnPath) {
        return new Operators.IntColumn(ColumnPath.fromDotString(columnPath));
    }

    public static Operators.LongColumn longColumn(String columnPath) {
        return new Operators.LongColumn(ColumnPath.fromDotString(columnPath));
    }

    public static Operators.FloatColumn floatColumn(String columnPath) {
        return new Operators.FloatColumn(ColumnPath.fromDotString(columnPath));
    }

    public static Operators.DoubleColumn doubleColumn(String columnPath) {
        return new Operators.DoubleColumn(ColumnPath.fromDotString(columnPath));
    }

    public static Operators.BooleanColumn booleanColumn(String columnPath) {
        return new Operators.BooleanColumn(ColumnPath.fromDotString(columnPath));
    }

    public static Operators.BinaryColumn binaryColumn(String columnPath) {
        return new Operators.BinaryColumn(ColumnPath.fromDotString(columnPath));
    }

    // ==================== Comparison predicates ====================

    public static <T extends Comparable<T>,
            C extends Operators.Column<T> & Operators.SupportsEqNotEq> Operators.Eq<T> eq(C column, T value) {
        return new Operators.Eq<>(column, value);
    }

    public static <T extends Comparable<T>,
            C extends Operators.Column<T> & Operators.SupportsEqNotEq> Operators.NotEq<T> notEq(C column, T value) {
        return new Operators.NotEq<>(column, value);
    }

    public static <T extends Comparable<T>,
            C extends Operators.Column<T> & Operators.SupportsLtGt> Operators.Lt<T> lt(C column, T value) {
        return new Operators.Lt<>(column, value);
    }

    public static <T extends Comparable<T>,
            C extends Operators.Column<T> & Operators.SupportsLtGt> Operators.LtEq<T> ltEq(C column, T value) {
        return new Operators.LtEq<>(column, value);
    }

    public static <T extends Comparable<T>,
            C extends Operators.Column<T> & Operators.SupportsLtGt> Operators.Gt<T> gt(C column, T value) {
        return new Operators.Gt<>(column, value);
    }

    public static <T extends Comparable<T>,
            C extends Operators.Column<T> & Operators.SupportsLtGt> Operators.GtEq<T> gtEq(C column, T value) {
        return new Operators.GtEq<>(column, value);
    }

    // ==================== Logical combinators ====================

    public static FilterPredicate and(FilterPredicate left, FilterPredicate right) {
        return new Operators.And(left, right);
    }

    public static FilterPredicate or(FilterPredicate left, FilterPredicate right) {
        return new Operators.Or(left, right);
    }

    public static FilterPredicate not(FilterPredicate predicate) {
        return new Operators.Not(predicate);
    }
}
