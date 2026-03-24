/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/// A predicate for filtering row groups based on column statistics.
///
/// Filter predicates enable predicate push-down: row groups whose statistics
/// prove that no rows can match the predicate are skipped entirely, avoiding
/// unnecessary I/O and decoding.
///
/// Usage examples:
/// ```java
/// // Simple comparison
/// FilterPredicate filter = FilterPredicate.gt("age", 21);
///
/// // Compound predicate
/// FilterPredicate filter = FilterPredicate.and(
///     FilterPredicate.gtEq("salary", 50000L),
///     FilterPredicate.lt("age", 65)
/// );
///
/// // Use with reader
/// try (ColumnReader reader = fileReader.createColumnReader("salary", filter)) {
///     while (reader.nextBatch()) { ... }
/// }
/// ```
public sealed interface FilterPredicate
        permits FilterPredicate.IntColumnPredicate,
                FilterPredicate.LongColumnPredicate,
                FilterPredicate.FloatColumnPredicate,
                FilterPredicate.DoubleColumnPredicate,
                FilterPredicate.BooleanColumnPredicate,
                FilterPredicate.BinaryColumnPredicate,
                FilterPredicate.And,
                FilterPredicate.Or,
                FilterPredicate.Not {

    // ==================== Operators ====================

    enum Operator {
        EQ, NOT_EQ, LT, LT_EQ, GT, GT_EQ
    }

    // ==================== INT32 Predicates ====================

    static FilterPredicate eq(String column, int value) {
        return new IntColumnPredicate(column, Operator.EQ, value);
    }

    static FilterPredicate notEq(String column, int value) {
        return new IntColumnPredicate(column, Operator.NOT_EQ, value);
    }

    static FilterPredicate lt(String column, int value) {
        return new IntColumnPredicate(column, Operator.LT, value);
    }

    static FilterPredicate ltEq(String column, int value) {
        return new IntColumnPredicate(column, Operator.LT_EQ, value);
    }

    static FilterPredicate gt(String column, int value) {
        return new IntColumnPredicate(column, Operator.GT, value);
    }

    static FilterPredicate gtEq(String column, int value) {
        return new IntColumnPredicate(column, Operator.GT_EQ, value);
    }

    // ==================== INT64 Predicates ====================

    static FilterPredicate eq(String column, long value) {
        return new LongColumnPredicate(column, Operator.EQ, value);
    }

    static FilterPredicate notEq(String column, long value) {
        return new LongColumnPredicate(column, Operator.NOT_EQ, value);
    }

    static FilterPredicate lt(String column, long value) {
        return new LongColumnPredicate(column, Operator.LT, value);
    }

    static FilterPredicate ltEq(String column, long value) {
        return new LongColumnPredicate(column, Operator.LT_EQ, value);
    }

    static FilterPredicate gt(String column, long value) {
        return new LongColumnPredicate(column, Operator.GT, value);
    }

    static FilterPredicate gtEq(String column, long value) {
        return new LongColumnPredicate(column, Operator.GT_EQ, value);
    }

    // ==================== FLOAT Predicates ====================

    static FilterPredicate eq(String column, float value) {
        return new FloatColumnPredicate(column, Operator.EQ, value);
    }

    static FilterPredicate notEq(String column, float value) {
        return new FloatColumnPredicate(column, Operator.NOT_EQ, value);
    }

    static FilterPredicate lt(String column, float value) {
        return new FloatColumnPredicate(column, Operator.LT, value);
    }

    static FilterPredicate ltEq(String column, float value) {
        return new FloatColumnPredicate(column, Operator.LT_EQ, value);
    }

    static FilterPredicate gt(String column, float value) {
        return new FloatColumnPredicate(column, Operator.GT, value);
    }

    static FilterPredicate gtEq(String column, float value) {
        return new FloatColumnPredicate(column, Operator.GT_EQ, value);
    }

    // ==================== DOUBLE Predicates ====================

    static FilterPredicate eq(String column, double value) {
        return new DoubleColumnPredicate(column, Operator.EQ, value);
    }

    static FilterPredicate notEq(String column, double value) {
        return new DoubleColumnPredicate(column, Operator.NOT_EQ, value);
    }

    static FilterPredicate lt(String column, double value) {
        return new DoubleColumnPredicate(column, Operator.LT, value);
    }

    static FilterPredicate ltEq(String column, double value) {
        return new DoubleColumnPredicate(column, Operator.LT_EQ, value);
    }

    static FilterPredicate gt(String column, double value) {
        return new DoubleColumnPredicate(column, Operator.GT, value);
    }

    static FilterPredicate gtEq(String column, double value) {
        return new DoubleColumnPredicate(column, Operator.GT_EQ, value);
    }

    // ==================== BOOLEAN Predicates ====================

    static FilterPredicate eq(String column, boolean value) {
        return new BooleanColumnPredicate(column, Operator.EQ, value);
    }

    static FilterPredicate notEq(String column, boolean value) {
        return new BooleanColumnPredicate(column, Operator.NOT_EQ, value);
    }

    // ==================== STRING (BYTE_ARRAY) Predicates ====================

    static FilterPredicate eq(String column, String value) {
        return new BinaryColumnPredicate(column, Operator.EQ, value.getBytes(StandardCharsets.UTF_8));
    }

    static FilterPredicate notEq(String column, String value) {
        return new BinaryColumnPredicate(column, Operator.NOT_EQ, value.getBytes(StandardCharsets.UTF_8));
    }

    static FilterPredicate lt(String column, String value) {
        return new BinaryColumnPredicate(column, Operator.LT, value.getBytes(StandardCharsets.UTF_8));
    }

    static FilterPredicate ltEq(String column, String value) {
        return new BinaryColumnPredicate(column, Operator.LT_EQ, value.getBytes(StandardCharsets.UTF_8));
    }

    static FilterPredicate gt(String column, String value) {
        return new BinaryColumnPredicate(column, Operator.GT, value.getBytes(StandardCharsets.UTF_8));
    }

    static FilterPredicate gtEq(String column, String value) {
        return new BinaryColumnPredicate(column, Operator.GT_EQ, value.getBytes(StandardCharsets.UTF_8));
    }

    // ==================== Logical Combinators ====================

    static FilterPredicate and(FilterPredicate left, FilterPredicate right) {
        return new And(List.of(left, right));
    }

    static FilterPredicate and(FilterPredicate... filters) {
        return new And(List.of(filters));
    }

    static FilterPredicate or(FilterPredicate left, FilterPredicate right) {
        return new Or(List.of(left, right));
    }

    static FilterPredicate or(FilterPredicate... filters) {
        return new Or(List.of(filters));
    }

    static FilterPredicate not(FilterPredicate filter) {
        return new Not(filter);
    }

    // ==================== Leaf Predicate Records ====================

    record IntColumnPredicate(String column, Operator op, int value) implements FilterPredicate {
    }

    record LongColumnPredicate(String column, Operator op, long value) implements FilterPredicate {
    }

    record FloatColumnPredicate(String column, Operator op, float value) implements FilterPredicate {
    }

    record DoubleColumnPredicate(String column, Operator op, double value) implements FilterPredicate {
    }

    record BooleanColumnPredicate(String column, Operator op, boolean value) implements FilterPredicate {
    }

    record BinaryColumnPredicate(String column, Operator op, byte[] value) implements FilterPredicate {

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof BinaryColumnPredicate that)) return false;
            return column.equals(that.column) && op == that.op && Arrays.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            int result = column.hashCode();
            result = 31 * result + op.hashCode();
            result = 31 * result + Arrays.hashCode(value);
            return result;
        }
    }

    // ==================== Logical Combinator Records ====================

    record And(List<FilterPredicate> filters) implements FilterPredicate {
    }

    record Or(List<FilterPredicate> filters) implements FilterPredicate {
    }

    record Not(FilterPredicate delegate) implements FilterPredicate {
    }
}
