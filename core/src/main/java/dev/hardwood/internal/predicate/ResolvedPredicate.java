/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.util.ArrayList;
import java.util.List;

import dev.hardwood.reader.FilterPredicate;

/// Internal execution-facing predicate tree, produced by [FilterPredicateResolver] from
/// the user-facing [FilterPredicate].
///
/// All logical-type conversions, column name resolution, and physical type validation
/// have already been performed. Evaluators ([RowGroupFilterEvaluator],
/// [PageFilterEvaluator], [RecordFilterEvaluator]) work exclusively with this type.
public sealed interface ResolvedPredicate {

    record IntPredicate(int columnIndex, FilterPredicate.Operator op, int value) implements ResolvedPredicate {}
    record LongPredicate(int columnIndex, FilterPredicate.Operator op, long value) implements ResolvedPredicate {}
    record FloatPredicate(int columnIndex, FilterPredicate.Operator op, float value) implements ResolvedPredicate {}
    record DoublePredicate(int columnIndex, FilterPredicate.Operator op, double value) implements ResolvedPredicate {}
    record BooleanPredicate(int columnIndex, FilterPredicate.Operator op, boolean value) implements ResolvedPredicate {}

    /// Binary predicate with optional signed comparison for FIXED_LEN_BYTE_ARRAY decimals.
    record BinaryPredicate(int columnIndex, FilterPredicate.Operator op, byte[] value,
            boolean signed) implements ResolvedPredicate {}

    record IntInPredicate(int columnIndex, int[] values) implements ResolvedPredicate {}
    record LongInPredicate(int columnIndex, long[] values) implements ResolvedPredicate {}
    record BinaryInPredicate(int columnIndex, byte[][] values) implements ResolvedPredicate {}

    record IsNullPredicate(int columnIndex) implements ResolvedPredicate {}
    record IsNotNullPredicate(int columnIndex) implements ResolvedPredicate {}

    record And(List<ResolvedPredicate> children) implements ResolvedPredicate {
        public And {
            if (children.isEmpty()) {
                throw new IllegalArgumentException("AND requires at least one child predicate");
            }
        }
    }

    record Or(List<ResolvedPredicate> children) implements ResolvedPredicate {
        public Or {
            if (children.isEmpty()) {
                throw new IllegalArgumentException("OR requires at least one child predicate");
            }
        }
    }

    /// Negates a predicate. For leaf predicates, the operator is inverted (e.g. GT → LT_EQ).
    /// For compound predicates, De Morgan's laws are applied:
    /// `NOT(AND(a, b))` → `OR(NOT(a), NOT(b))` and `NOT(OR(a, b))` → `AND(NOT(a), NOT(b))`.
    /// For IN predicates, expanded to `AND(NOT_EQ(v1), NOT_EQ(v2), ...)`.
    static ResolvedPredicate negate(ResolvedPredicate predicate) {
        return switch (predicate) {
            case IntPredicate p -> new IntPredicate(p.columnIndex(), p.op().invert(), p.value());
            case LongPredicate p -> new LongPredicate(p.columnIndex(), p.op().invert(), p.value());
            case FloatPredicate p -> new FloatPredicate(p.columnIndex(), p.op().invert(), p.value());
            case DoublePredicate p -> new DoublePredicate(p.columnIndex(), p.op().invert(), p.value());
            case BooleanPredicate p -> new BooleanPredicate(p.columnIndex(), p.op().invert(), p.value());
            case BinaryPredicate p -> new BinaryPredicate(p.columnIndex(), p.op().invert(), p.value(), p.signed());
            case IsNullPredicate p -> new IsNotNullPredicate(p.columnIndex());
            case IsNotNullPredicate p -> new IsNullPredicate(p.columnIndex());
            case And a -> new Or(a.children().stream()
                    .map(ResolvedPredicate::negate).toList());
            case Or o -> new And(o.children().stream()
                    .map(ResolvedPredicate::negate).toList());
            case IntInPredicate p -> {
                List<ResolvedPredicate> notEqs = new ArrayList<>(p.values().length);
                for (int value : p.values()) {
                    notEqs.add(new IntPredicate(p.columnIndex(), FilterPredicate.Operator.NOT_EQ, value));
                }
                yield new And(notEqs);
            }
            case LongInPredicate p -> {
                List<ResolvedPredicate> notEqs = new ArrayList<>(p.values().length);
                for (long value : p.values()) {
                    notEqs.add(new LongPredicate(p.columnIndex(), FilterPredicate.Operator.NOT_EQ, value));
                }
                yield new And(notEqs);
            }
            case BinaryInPredicate p -> {
                List<ResolvedPredicate> notEqs = new ArrayList<>(p.values().length);
                for (byte[] value : p.values()) {
                    notEqs.add(new BinaryPredicate(p.columnIndex(), FilterPredicate.Operator.NOT_EQ, value, false));
                }
                yield new And(notEqs);
            }
        };
    }
}
