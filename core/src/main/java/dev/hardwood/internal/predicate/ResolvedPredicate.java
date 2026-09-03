/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import dev.hardwood.reader.FilterPredicate;

/// Internal execution-facing predicate tree, produced by [FilterPredicateResolver] from
/// the user-facing [FilterPredicate].
///
/// All logical-type conversions, column name resolution, and physical type validation
/// have already been performed. Evaluators ([RowGroupFilterEvaluator],
/// [PageFilterEvaluator]) and the record-level [RecordFilterCompiler] work
/// exclusively with this type.
public sealed interface ResolvedPredicate {

    record IntPredicate(int columnIndex, FilterPredicate.Operator op, int value) implements ResolvedPredicate {}
    record LongPredicate(int columnIndex, FilterPredicate.Operator op, long value) implements ResolvedPredicate {}

    /// `ieee754TotalOrder` carries the column's decoded [dev.hardwood.metadata.ColumnOrder]: `true`
    /// only when it is the IEEE 754 total order, which orders `-0` below `+0` unambiguously, so
    /// statistics min/max are exact. Under any other ordering (type-defined / absent / unrecognized)
    /// the Parquet spec leaves `±0` ambiguous — a `+0` min may hide `-0` — so statistics pruning
    /// widens `±0` bounds. The 3-arg convenience constructor defaults it to `false` (the conservative,
    /// widening case) for callers that do not consult statistics (record/batch matching).
    record FloatPredicate(int columnIndex, FilterPredicate.Operator op, float value,
            boolean ieee754TotalOrder) implements ResolvedPredicate {
        public FloatPredicate(int columnIndex, FilterPredicate.Operator op, float value) {
            this(columnIndex, op, value, false);
        }
    }

    /// User-facing `FloatColumnPredicate` against a column whose physical type is
    /// `FIXED_LEN_BYTE_ARRAY(2)` annotated `Float16Type`. Carried as a separate
    /// resolved type so evaluators can dispatch to the 2-byte decode path for
    /// both record values and stats min/max.
    record Float16Predicate(int columnIndex, FilterPredicate.Operator op, float value,
            boolean ieee754TotalOrder) implements ResolvedPredicate {
        public Float16Predicate(int columnIndex, FilterPredicate.Operator op, float value) {
            this(columnIndex, op, value, false);
        }
    }

    record DoublePredicate(int columnIndex, FilterPredicate.Operator op, double value,
            boolean ieee754TotalOrder) implements ResolvedPredicate {
        public DoublePredicate(int columnIndex, FilterPredicate.Operator op, double value) {
            this(columnIndex, op, value, false);
        }
    }
    record BooleanPredicate(int columnIndex, FilterPredicate.Operator op, boolean value) implements ResolvedPredicate {}

    /// A comparison against a byte string.
    ///
    /// The [Comparison] fixes both the order the bytes compare in and whether a number has a
    /// single encoding in the column. The two are not independent — only a variable-length
    /// `DECIMAL` compares signed while admitting more than one encoding of the same value — so
    /// they are carried as one three-valued choice rather than as two flags.
    record BinaryPredicate(int columnIndex, FilterPredicate.Operator op, byte[] value,
            Comparison comparison) implements ResolvedPredicate {

        /// The order a binary column's values compare in, and whether a value has one encoding
        /// in it.
        public enum Comparison {

            /// Unsigned lexicographic — the type-defined order of a byte string compared as
            /// itself. A value is exactly the bytes it was written as.
            BYTE_STRING(false, true),

            /// Big-endian two's complement over a `FIXED_LEN_BYTE_ARRAY` `DECIMAL`, whose every
            /// value is padded to the column width, so a number has one encoding in it.
            ///
            /// `DECIMAL(scale = 2)` over `FIXED_LEN_BYTE_ARRAY(4)`:
            ///
            /// ```text
            ///  1.27  ->  00 00 00 7F
            ///  1.28  ->  00 00 00 80
            /// -1.00  ->  FF FF FF 9C
            /// ```
            ///
            /// Equal widths, so the sign decides first and the rest compares unsigned. One
            /// encoding per number, so a bloom-filter or dictionary probe for the literal's
            /// bytes answers for the value.
            FIXED_DECIMAL(true, true),

            /// Big-endian two's complement over a `BYTE_ARRAY` `DECIMAL`. The format asks such a
            /// column for the fewest bytes that hold each value but does not require them, so a
            /// writer may pad and the same number may appear under more than one encoding.
            ///
            /// `DECIMAL(scale = 2)`, each value in the fewest bytes that hold it:
            ///
            /// ```text
            ///  1.27  ->     7F
            ///  1.28  ->  00 80     the leading 00 keeps 128 positive
            /// -1.00  ->     9C
            /// -2.56  ->  FF 00
            /// ```
            ///
            /// Length does not track magnitude: byte-wise, `7F` outranks `00 80` and `9C` falls
            /// below `FF 00`, both backwards. The shorter value is sign-extended to the longer
            /// before the bytes compare — `7F` against `00 80` is `00 7F` against `00 80`.
            ///
            /// And `00 00 00 7F` is a legal spelling of the same `1.27`, so equality cannot be
            /// decided by hashing or matching the literal's own bytes: the probe would miss a
            /// value the column holds.
            VARIABLE_DECIMAL(true, false);

            private final boolean signed;
            private final boolean byteExact;

            Comparison(boolean signed, boolean byteExact) {
                this.signed = signed;
                this.byteExact = byteExact;
            }

            /// Whether the bytes compare as a big-endian two's complement value — the order a
            /// `DECIMAL`'s unscaled value sorts in — rather than unsigned lexicographically.
            public boolean signed() {
                return signed;
            }

            /// Whether the column can hold a given value only as exactly one byte string.
            /// Equality shortcuts that test bytes rather than order (bloom filters, dictionary
            /// membership) are sound only when it holds; without it a padded value hashes to a
            /// miss and its rows would be dropped.
            public boolean byteExact() {
                return byteExact;
            }
        }

        /// Whether the predicate's bytes compare signed. See [Comparison#signed()].
        public boolean signed() {
            return comparison.signed();
        }

        /// Whether the column encodes a value as exactly these bytes. See
        /// [Comparison#byteExact()].
        public boolean byteExact() {
            return comparison.byteExact();
        }
    }

    record IntInPredicate(int columnIndex, int[] values) implements ResolvedPredicate {}
    record LongInPredicate(int columnIndex, long[] values) implements ResolvedPredicate {}
    record BinaryInPredicate(int columnIndex, byte[][] values) implements ResolvedPredicate {}
    record DoubleInPredicate(int columnIndex, double[] values, boolean floatColumn,
            boolean ieee754TotalOrder) implements ResolvedPredicate {}

    record IsNullPredicate(int columnIndex) implements ResolvedPredicate {}
    record IsNotNullPredicate(int columnIndex) implements ResolvedPredicate {}

    /// Conjunction of child predicates. Nested `And` children are flattened at
    /// construction time so consumers can rely on a single flat level.
    record And(List<ResolvedPredicate> children) implements ResolvedPredicate {
        public And {
            if (children.isEmpty()) {
                throw new IllegalArgumentException("AND requires at least one child predicate");
            }
            children = flattenSameKind(children, And.class);
        }
    }

    /// Disjunction of child predicates. Nested `Or` children are flattened at
    /// construction time so consumers can rely on a single flat level.
    record Or(List<ResolvedPredicate> children) implements ResolvedPredicate {
        public Or {
            if (children.isEmpty()) {
                throw new IllegalArgumentException("OR requires at least one child predicate");
            }
            children = flattenSameKind(children, Or.class);
        }
    }

    private static <T extends ResolvedPredicate> List<ResolvedPredicate> flattenSameKind(
            List<ResolvedPredicate> children, Class<T> sameKind) {
        boolean hasNested = false;
        for (ResolvedPredicate child : children) {
            if (sameKind.isInstance(child)) {
                hasNested = true;
                break;
            }
        }
        if (!hasNested) {
            return children;
        }
        List<ResolvedPredicate> flat = new ArrayList<>(children.size());
        for (ResolvedPredicate child : children) {
            if (sameKind.isInstance(child)) {
                List<ResolvedPredicate> nested = (child instanceof And a) ? a.children() : ((Or) child).children();
                flat.addAll(nested);
            } else {
                flat.add(child);
            }
        }
        return flat;
    }

    record GeospatialPredicate(int columnIndex, double xmin, double ymin,
                               double xmax, double ymax) implements ResolvedPredicate {}

    /// The column index a leaf predicate tests, or `-1` for the compound [And] and
    /// [Or] nodes, which test no column of their own.
    static int leafColumnIndex(ResolvedPredicate predicate) {
        return switch (predicate) {
            case IntPredicate p -> p.columnIndex();
            case LongPredicate p -> p.columnIndex();
            case FloatPredicate p -> p.columnIndex();
            case Float16Predicate p -> p.columnIndex();
            case DoublePredicate p -> p.columnIndex();
            case BooleanPredicate p -> p.columnIndex();
            case BinaryPredicate p -> p.columnIndex();
            case IntInPredicate p -> p.columnIndex();
            case LongInPredicate p -> p.columnIndex();
            case BinaryInPredicate p -> p.columnIndex();
            case DoubleInPredicate p -> p.columnIndex();
            case IsNullPredicate p -> p.columnIndex();
            case IsNotNullPredicate p -> p.columnIndex();
            case GeospatialPredicate p -> p.columnIndex();
            case And ignored -> -1;
            case Or ignored -> -1;
        };
    }

    /// Adds every column index the tree tests to `columns`.
    static void collectColumnIndices(ResolvedPredicate predicate, BitSet columns) {
        int leafColumn = leafColumnIndex(predicate);
        if (leafColumn >= 0) {
            columns.set(leafColumn);
            return;
        }
        List<ResolvedPredicate> children = (predicate instanceof And a) ? a.children() : ((Or) predicate).children();
        for (ResolvedPredicate child : children) {
            collectColumnIndices(child, columns);
        }
    }

    /// Rewrites every leaf's `columnIndex` through `columnMapping`, which maps a
    /// column index in the schema the predicate was resolved against onto the
    /// corresponding index in another schema.
    ///
    /// Used by the multi-file read path: predicates are resolved once against the
    /// first file's schema, but metadata pruning indexes into each file's own
    /// `RowGroup.columns` list, whose order is a property of that file.
    ///
    /// @param predicate the predicate to rewrite
    /// @param columnMapping target index per source index; `-1` marks a column
    ///        absent from the target schema
    /// @throws IllegalArgumentException if a leaf references a column mapped to `-1`
    static ResolvedPredicate remapColumns(ResolvedPredicate predicate, int[] columnMapping) {
        return switch (predicate) {
            case IntPredicate p -> new IntPredicate(mapped(p.columnIndex(), columnMapping), p.op(), p.value());
            case LongPredicate p -> new LongPredicate(mapped(p.columnIndex(), columnMapping), p.op(), p.value());
            case FloatPredicate p -> new FloatPredicate(mapped(p.columnIndex(), columnMapping), p.op(), p.value(),
                    p.ieee754TotalOrder());
            case Float16Predicate p -> new Float16Predicate(mapped(p.columnIndex(), columnMapping), p.op(), p.value(),
                    p.ieee754TotalOrder());
            case DoublePredicate p -> new DoublePredicate(mapped(p.columnIndex(), columnMapping), p.op(), p.value(),
                    p.ieee754TotalOrder());
            case BooleanPredicate p -> new BooleanPredicate(mapped(p.columnIndex(), columnMapping), p.op(), p.value());
            case BinaryPredicate p -> new BinaryPredicate(mapped(p.columnIndex(), columnMapping), p.op(), p.value(),
                    p.comparison());
            case IntInPredicate p -> new IntInPredicate(mapped(p.columnIndex(), columnMapping), p.values());
            case LongInPredicate p -> new LongInPredicate(mapped(p.columnIndex(), columnMapping), p.values());
            case BinaryInPredicate p -> new BinaryInPredicate(mapped(p.columnIndex(), columnMapping), p.values());
            case DoubleInPredicate p -> new DoubleInPredicate(mapped(p.columnIndex(), columnMapping), p.values(),
                    p.floatColumn(), p.ieee754TotalOrder());
            case IsNullPredicate p -> new IsNullPredicate(mapped(p.columnIndex(), columnMapping));
            case IsNotNullPredicate p -> new IsNotNullPredicate(mapped(p.columnIndex(), columnMapping));
            case GeospatialPredicate p -> new GeospatialPredicate(mapped(p.columnIndex(), columnMapping),
                    p.xmin(), p.ymin(), p.xmax(), p.ymax());
            case And a -> new And(remapChildren(a.children(), columnMapping));
            case Or o -> new Or(remapChildren(o.children(), columnMapping));
        };
    }

    private static List<ResolvedPredicate> remapChildren(List<ResolvedPredicate> children, int[] columnMapping) {
        List<ResolvedPredicate> remapped = new ArrayList<>(children.size());
        for (ResolvedPredicate child : children) {
            remapped.add(remapColumns(child, columnMapping));
        }
        return remapped;
    }

    private static int mapped(int columnIndex, int[] columnMapping) {
        int target = columnMapping[columnIndex];
        if (target < 0) {
            throw new IllegalArgumentException(
                    "Predicate column index " + columnIndex + " has no counterpart in the target schema");
        }
        return target;
    }

    /// Negates a predicate. For leaf predicates, the operator is inverted (e.g. GT → LT_EQ).
    /// For compound predicates, De Morgan's laws are applied:
    /// `NOT(AND(a, b))` → `OR(NOT(a), NOT(b))` and `NOT(OR(a, b))` → `AND(NOT(a), NOT(b))`.
    /// For IN predicates, expanded to `AND(NOT_EQ(v1), NOT_EQ(v2), ...)`.
    static ResolvedPredicate negate(ResolvedPredicate predicate) {
        return switch (predicate) {
            case IntPredicate p -> new IntPredicate(p.columnIndex(), p.op().invert(), p.value());
            case LongPredicate p -> new LongPredicate(p.columnIndex(), p.op().invert(), p.value());
            case FloatPredicate p -> new FloatPredicate(p.columnIndex(), p.op().invert(), p.value(),
                    p.ieee754TotalOrder());
            case Float16Predicate p -> new Float16Predicate(p.columnIndex(), p.op().invert(), p.value(),
                    p.ieee754TotalOrder());
            case DoublePredicate p -> new DoublePredicate(p.columnIndex(), p.op().invert(), p.value(),
                    p.ieee754TotalOrder());
            case BooleanPredicate p -> new BooleanPredicate(p.columnIndex(), p.op().invert(), p.value());
            case BinaryPredicate p -> new BinaryPredicate(p.columnIndex(), p.op().invert(), p.value(),
                    p.comparison());
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
                    notEqs.add(new BinaryPredicate(p.columnIndex(), FilterPredicate.Operator.NOT_EQ, value,
                            BinaryPredicate.Comparison.BYTE_STRING));
                }
                yield new And(notEqs);
            }
            case DoubleInPredicate p -> {
                if (p.floatColumn()) {
                    List<ResolvedPredicate> notEqs = new ArrayList<>(p.values().length);
                    for (double v : p.values()) {
                        if (Double.isNaN(v) || (double) (float) v == v) {
                            notEqs.add(new FloatPredicate(p.columnIndex(), FilterPredicate.Operator.NOT_EQ, (float) v));
                        }
                    }
                    if (notEqs.isEmpty()) {
                        yield new IsNotNullPredicate(p.columnIndex());
                    }
                    yield new And(notEqs);
                }
                List<ResolvedPredicate> notEqs = new ArrayList<>(p.values().length);
                for (double value : p.values()) {
                    notEqs.add(new DoublePredicate(p.columnIndex(), FilterPredicate.Operator.NOT_EQ, value));
                }
                yield new And(notEqs);
            }
            case GeospatialPredicate p -> throw new UnsupportedOperationException(
                    "Negation of spatial intersects predicate is not supported");
        };
    }
}
