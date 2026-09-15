/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.tools.predicateaudit;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.UUID;

import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.FilterPredicate.Operator;
import dev.hardwood.row.PqInterval;
import dev.hardwood.tools.predicateaudit.Oracle.And;
import dev.hardwood.tools.predicateaudit.Oracle.In;
import dev.hardwood.tools.predicateaudit.Oracle.Intersects;
import dev.hardwood.tools.predicateaudit.Oracle.IsNotNull;
import dev.hardwood.tools.predicateaudit.Oracle.IsNull;
import dev.hardwood.tools.predicateaudit.Oracle.Leaf;
import dev.hardwood.tools.predicateaudit.Oracle.Not;
import dev.hardwood.tools.predicateaudit.Oracle.Or;
import dev.hardwood.tools.predicateaudit.Oracle.P;

/// Builds the Hardwood [FilterPredicate] for a predicate of the audit, through the public factories.
final class Predicates {

    private Predicates() {
    }

    static FilterPredicate of(P predicate) {
        return switch (predicate) {
            case Leaf leaf -> leaf(leaf.column(), operator(leaf.op()), leaf.literal());
            case In in -> set(in.column(), in.literals());
            case IsNull isNull -> FilterPredicate.isNull(isNull.column());
            case IsNotNull isNotNull -> FilterPredicate.isNotNull(isNotNull.column());
            case And and -> FilterPredicate.and(and.children().stream().map(Predicates::of).toArray(FilterPredicate[]::new));
            case Or or -> FilterPredicate.or(or.children().stream().map(Predicates::of).toArray(FilterPredicate[]::new));
            case Not not -> FilterPredicate.not(of(not.child()));
            case Intersects i -> FilterPredicate.intersects(i.column(), i.xmin(), i.ymin(), i.xmax(), i.ymax());
        };
    }

    private static Operator operator(Oracle.Op op) {
        return switch (op) {
            case EQ -> Operator.EQ;
            case NE -> Operator.NOT_EQ;
            case LT -> Operator.LT;
            case LE -> Operator.LT_EQ;
            case GT -> Operator.GT;
            case GE -> Operator.GT_EQ;
        };
    }

    /// The leaf through the factory for the literal's type. An ordered operator on a [PqInterval],
    /// which has no factory, goes through the record, as the rule's refusal of it is audited too.
    private static FilterPredicate leaf(String column, Operator op, Object literal) {
        return switch (literal) {
            case Boolean v -> comparison(op, () -> FilterPredicate.eq(column, v), () -> FilterPredicate.notEq(column, v),
                    () -> FilterPredicate.lt(column, v), () -> FilterPredicate.ltEq(column, v),
                    () -> FilterPredicate.gt(column, v), () -> FilterPredicate.gtEq(column, v));
            case Integer v -> comparison(op, () -> FilterPredicate.eq(column, v), () -> FilterPredicate.notEq(column, v),
                    () -> FilterPredicate.lt(column, v), () -> FilterPredicate.ltEq(column, v),
                    () -> FilterPredicate.gt(column, v), () -> FilterPredicate.gtEq(column, v));
            case Long v -> comparison(op, () -> FilterPredicate.eq(column, v), () -> FilterPredicate.notEq(column, v),
                    () -> FilterPredicate.lt(column, v), () -> FilterPredicate.ltEq(column, v),
                    () -> FilterPredicate.gt(column, v), () -> FilterPredicate.gtEq(column, v));
            case Float v -> comparison(op, () -> FilterPredicate.eq(column, v), () -> FilterPredicate.notEq(column, v),
                    () -> FilterPredicate.lt(column, v), () -> FilterPredicate.ltEq(column, v),
                    () -> FilterPredicate.gt(column, v), () -> FilterPredicate.gtEq(column, v));
            case Double v -> comparison(op, () -> FilterPredicate.eq(column, v), () -> FilterPredicate.notEq(column, v),
                    () -> FilterPredicate.lt(column, v), () -> FilterPredicate.ltEq(column, v),
                    () -> FilterPredicate.gt(column, v), () -> FilterPredicate.gtEq(column, v));
            case byte[] v -> comparison(op, () -> FilterPredicate.eq(column, v), () -> FilterPredicate.notEq(column, v),
                    () -> FilterPredicate.lt(column, v), () -> FilterPredicate.ltEq(column, v),
                    () -> FilterPredicate.gt(column, v), () -> FilterPredicate.gtEq(column, v));
            case String v -> comparison(op, () -> FilterPredicate.eq(column, v), () -> FilterPredicate.notEq(column, v),
                    () -> FilterPredicate.lt(column, v), () -> FilterPredicate.ltEq(column, v),
                    () -> FilterPredicate.gt(column, v), () -> FilterPredicate.gtEq(column, v));
            case LocalDate v -> comparison(op, () -> FilterPredicate.eq(column, v), () -> FilterPredicate.notEq(column, v),
                    () -> FilterPredicate.lt(column, v), () -> FilterPredicate.ltEq(column, v),
                    () -> FilterPredicate.gt(column, v), () -> FilterPredicate.gtEq(column, v));
            case Instant v -> comparison(op, () -> FilterPredicate.eq(column, v), () -> FilterPredicate.notEq(column, v),
                    () -> FilterPredicate.lt(column, v), () -> FilterPredicate.ltEq(column, v),
                    () -> FilterPredicate.gt(column, v), () -> FilterPredicate.gtEq(column, v));
            case LocalDateTime v -> comparison(op, () -> FilterPredicate.eq(column, v), () -> FilterPredicate.notEq(column, v),
                    () -> FilterPredicate.lt(column, v), () -> FilterPredicate.ltEq(column, v),
                    () -> FilterPredicate.gt(column, v), () -> FilterPredicate.gtEq(column, v));
            case LocalTime v -> comparison(op, () -> FilterPredicate.eq(column, v), () -> FilterPredicate.notEq(column, v),
                    () -> FilterPredicate.lt(column, v), () -> FilterPredicate.ltEq(column, v),
                    () -> FilterPredicate.gt(column, v), () -> FilterPredicate.gtEq(column, v));
            case BigDecimal v -> comparison(op, () -> FilterPredicate.eq(column, v), () -> FilterPredicate.notEq(column, v),
                    () -> FilterPredicate.lt(column, v), () -> FilterPredicate.ltEq(column, v),
                    () -> FilterPredicate.gt(column, v), () -> FilterPredicate.gtEq(column, v));
            case UUID v -> comparison(op, () -> FilterPredicate.eq(column, v), () -> FilterPredicate.notEq(column, v),
                    () -> FilterPredicate.lt(column, v), () -> FilterPredicate.ltEq(column, v),
                    () -> FilterPredicate.gt(column, v), () -> FilterPredicate.gtEq(column, v));
            case PqInterval v -> switch (op) {
                case EQ -> FilterPredicate.eq(column, v);
                case NOT_EQ -> FilterPredicate.notEq(column, v);
                default -> new FilterPredicate.IntervalColumnPredicate(column, op, v);
            };
            default -> throw new IllegalArgumentException("No factory for a " + literal.getClass().getName());
        };
    }

    @FunctionalInterface
    private interface Factory {
        FilterPredicate build();
    }

    private static FilterPredicate comparison(Operator op, Factory eq, Factory notEq, Factory lt, Factory ltEq, Factory gt,
            Factory gtEq) {
        return switch (op) {
            case EQ -> eq.build();
            case NOT_EQ -> notEq.build();
            case LT -> lt.build();
            case LT_EQ -> ltEq.build();
            case GT -> gt.build();
            case GT_EQ -> gtEq.build();
        };
    }

    private static FilterPredicate set(String column, List<Object> literals) {
        int count = literals.size();
        return switch (literals.getFirst()) {
            case Integer ignored -> FilterPredicate.in(column, literals.stream().mapToInt(Integer.class::cast).toArray());
            case Long ignored -> FilterPredicate.in(column, literals.stream().mapToLong(Long.class::cast).toArray());
            case Float ignored -> {
                float[] values = new float[count];
                for (int i = 0; i < count; i++) {
                    values[i] = (Float) literals.get(i);
                }
                yield FilterPredicate.in(column, values);
            }
            case Double ignored -> FilterPredicate.in(column, literals.stream().mapToDouble(Double.class::cast).toArray());
            case byte[] ignored -> FilterPredicate.in(column, literals.toArray(byte[][]::new));
            case String ignored -> FilterPredicate.in(column, literals.toArray(String[]::new));
            case LocalDate ignored -> FilterPredicate.in(column, literals.toArray(LocalDate[]::new));
            case Instant ignored -> FilterPredicate.in(column, literals.toArray(Instant[]::new));
            case LocalDateTime ignored -> FilterPredicate.in(column, literals.toArray(LocalDateTime[]::new));
            case LocalTime ignored -> FilterPredicate.in(column, literals.toArray(LocalTime[]::new));
            case BigDecimal ignored -> FilterPredicate.in(column, literals.toArray(BigDecimal[]::new));
            case UUID ignored -> FilterPredicate.in(column, literals.toArray(UUID[]::new));
            case PqInterval ignored -> FilterPredicate.in(column, literals.toArray(PqInterval[]::new));
            default -> throw new IllegalArgumentException("No set form for a " + literals.getFirst().getClass().getName());
        };
    }
}
