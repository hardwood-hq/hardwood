/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

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

    record And(List<ResolvedPredicate> children) implements ResolvedPredicate {}
    record Or(List<ResolvedPredicate> children) implements ResolvedPredicate {}
    record Not(ResolvedPredicate delegate) implements ResolvedPredicate {}
}
