/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.parquet.hadoop.util;

import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.io.api.Binary;

/// Converts parquet-java [FilterPredicate] trees to Hardwood
/// [dev.hardwood.reader.FilterPredicate] trees.
final class FilterConverter {

    private FilterConverter() {
    }

    /// Convert a parquet-java FilterPredicate to a Hardwood FilterPredicate.
    ///
    /// @param predicate the parquet-java predicate
    /// @return the equivalent Hardwood predicate
    /// @throws UnsupportedOperationException if the predicate type is not supported
    static dev.hardwood.reader.FilterPredicate convert(FilterPredicate predicate) {
        return switch (predicate) {
            case Operators.Eq<?> p -> convertComparison(p.getColumn(), p.getValue(),
                    dev.hardwood.reader.FilterPredicate.Operator.EQ);
            case Operators.NotEq<?> p -> convertComparison(p.getColumn(), p.getValue(),
                    dev.hardwood.reader.FilterPredicate.Operator.NOT_EQ);
            case Operators.Lt<?> p -> convertComparison(p.getColumn(), p.getValue(),
                    dev.hardwood.reader.FilterPredicate.Operator.LT);
            case Operators.LtEq<?> p -> convertComparison(p.getColumn(), p.getValue(),
                    dev.hardwood.reader.FilterPredicate.Operator.LT_EQ);
            case Operators.Gt<?> p -> convertComparison(p.getColumn(), p.getValue(),
                    dev.hardwood.reader.FilterPredicate.Operator.GT);
            case Operators.GtEq<?> p -> convertComparison(p.getColumn(), p.getValue(),
                    dev.hardwood.reader.FilterPredicate.Operator.GT_EQ);
            case Operators.And p -> dev.hardwood.reader.FilterPredicate.and(
                    convert(p.getLeft()), convert(p.getRight()));
            case Operators.Or p -> dev.hardwood.reader.FilterPredicate.or(
                    convert(p.getLeft()), convert(p.getRight()));
            case Operators.Not p -> dev.hardwood.reader.FilterPredicate.not(
                    convert(p.getPredicate()));
            default -> throw new UnsupportedOperationException(
                    "Unsupported filter predicate type: " + predicate.getClass().getName());
        };
    }

    private static dev.hardwood.reader.FilterPredicate convertComparison(
            Operators.Column<?> column, Object value,
            dev.hardwood.reader.FilterPredicate.Operator op) {
        String columnName = column.getColumnPath().toDotString();

        return switch (value) {
            case Integer v -> new dev.hardwood.reader.FilterPredicate.IntColumnPredicate(columnName, op, v);
            case Long v -> new dev.hardwood.reader.FilterPredicate.LongColumnPredicate(columnName, op, v);
            case Float v -> new dev.hardwood.reader.FilterPredicate.FloatColumnPredicate(columnName, op, v);
            case Double v -> new dev.hardwood.reader.FilterPredicate.DoubleColumnPredicate(columnName, op, v);
            case Boolean v -> new dev.hardwood.reader.FilterPredicate.BooleanColumnPredicate(columnName, op, v);
            case Binary v -> new dev.hardwood.reader.FilterPredicate.BinaryColumnPredicate(
                    columnName, op, v.getBytesUnsafe());
            case null -> throw new IllegalArgumentException(
                    "Null filter values are not supported for column: " + columnName);
            default -> throw new UnsupportedOperationException(
                    "Unsupported filter value type: " + value.getClass().getName()
                            + " for column: " + columnName);
        };
    }
}
