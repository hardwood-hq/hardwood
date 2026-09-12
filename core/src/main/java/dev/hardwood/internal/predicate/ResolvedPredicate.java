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

    /// A comparison against an `INT32` column annotated `INT(bitWidth, isSigned = false)`, whose
    /// values order by their unsigned magnitude. `value` is the literal's stored two's-complement
    /// bit pattern, the same form the accessors hand back for such a column, so `4_000_000_000`
    /// arrives as `-294_967_296` and orders above every positive `int`.
    ///
    /// Held apart from [IntPredicate] rather than carried as a flag on it so that every
    /// `switch` over this hierarchy has to name the unsigned case: a flag can be read at one
    /// evaluator and forgotten at the next, and the two disagreeing is what silently drops rows.
    record UnsignedIntPredicate(int columnIndex, FilterPredicate.Operator op, int value)
            implements ResolvedPredicate {}

    /// A comparison against an `INT64` column annotated `INT(64, isSigned = false)`. `value` is
    /// the literal's stored bit pattern; see [UnsignedIntPredicate].
    record UnsignedLongPredicate(int columnIndex, FilterPredicate.Operator op, long value)
            implements ResolvedPredicate {}

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
    /// A comparison against a `BOOLEAN` column, which carries `EQ` or `NOT_EQ` and nothing else.
    ///
    /// The column holds two values and nothing between them, so
    /// [FilterPredicateResolver#booleanLeaf] answers each ordered operator on one as an equality
    /// against `false` or `true`, or as a constant. Stating that here keeps every evaluator's
    /// boolean arm — statistics, dictionary, batch and record — reading one comparison, and names
    /// the wiring error at the point it is made rather than at whichever evaluator sees it first.
    record BooleanPredicate(int columnIndex, FilterPredicate.Operator op, boolean value) implements ResolvedPredicate {
        public BooleanPredicate {
            if (op != FilterPredicate.Operator.EQ && op != FilterPredicate.Operator.NOT_EQ) {
                throw new IllegalArgumentException("A boolean column takes EQ and NOT_EQ; " + op
                        + " on column " + columnIndex + " resolves to an equality or a constant");
            }
        }
    }

    /// A comparison against a byte string.
    ///
    /// The [Comparison] fixes both the order the bytes compare in and whether a value has a
    /// single encoding in the column. The two are not independent — the order a column's values
    /// compare in is what decides which encodings stand for the same value — so they are carried
    /// as one choice rather than as an order and a flag.
    record BinaryPredicate(int columnIndex, FilterPredicate.Operator op, byte[] value,
            Comparison comparison) implements ResolvedPredicate {

        /// The order a binary column's values compare in, and whether a value has one encoding
        /// in it.
        public enum Comparison {

            /// Unsigned lexicographic — the type-defined order of a byte string compared as
            /// itself. A value is exactly the bytes it was written as.
            BYTE_STRING(true),

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
            FIXED_DECIMAL(true),

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
            VARIABLE_DECIMAL(false),

            /// A legacy `INT96` timestamp, compared as the instant it encodes: twelve
            /// little-endian bytes of nanoseconds of the day, then the Julian day.
            ///
            /// ```text
            ///  2023-11-13T12:00:00Z  ->  00 80 A7 48 4A 27 00 00  66 8A 25 00
            ///  2023-11-14T00:00:00Z  ->  00 00 00 00 00 00 00 00  67 8A 25 00
            /// ```
            ///
            /// Byte-wise, the later instant ranks first, since the low byte of the nanoseconds
            /// leads. And the format does not bound the nanoseconds by a day, so
            /// `00 00 4F 91 94 4E 00 00  66 8A 25 00` — the day before and a full day of
            /// nanoseconds — is a legal spelling of midnight on the 14th, which a probe for its
            /// canonical bytes would miss.
            INT96_INSTANT(false);

            private final boolean byteExact;

            Comparison(boolean byteExact) {
                this.byteExact = byteExact;
            }

            /// `left` against `right` in this order.
            ///
            /// @return negative if `left` sorts first, zero if the two are the same value,
            ///         positive if `right` sorts first
            public int compare(byte[] left, byte[] right) {
                return switch (this) {
                    case BYTE_STRING -> BinaryComparator.compareUnsigned(left, right);
                    case FIXED_DECIMAL, VARIABLE_DECIMAL -> BinaryComparator.compareSigned(left, right);
                    case INT96_INSTANT -> BinaryComparator.compareInt96(left, right);
                };
            }

            /// Whether the column can hold a given value only as exactly one byte string.
            /// Equality shortcuts that test bytes rather than order (bloom filters, dictionary
            /// membership) are sound only when it holds; without it a padded value hashes to a
            /// miss and its rows would be dropped.
            public boolean byteExact() {
                return byteExact;
            }
        }

        /// Whether the column encodes a value as exactly these bytes. See
        /// [Comparison#byteExact()].
        public boolean byteExact() {
            return comparison.byteExact();
        }
    }

    record IntInPredicate(int columnIndex, int[] values) implements ResolvedPredicate {}
    record LongInPredicate(int columnIndex, long[] values) implements ResolvedPredicate {}

    /// Membership against an unsigned `INT32` column. Membership itself is bit equality and so
    /// reads the same either way; the unsigned form exists because statistics pruning orders the
    /// probes against the column's bounds, which are recorded in unsigned order.
    record UnsignedIntInPredicate(int columnIndex, int[] values) implements ResolvedPredicate {}

    /// Membership against an unsigned `INT64` column; see [UnsignedIntInPredicate].
    record UnsignedLongInPredicate(int columnIndex, long[] values) implements ResolvedPredicate {}
    /// Membership against a binary column, comparing each probe in the column's own order —
    /// see [BinaryPredicate.Comparison]. A `DECIMAL` compares by the value its bytes stand for,
    /// so a padded encoding of a probe is still a member; only the Bloom filter and dictionary
    /// shortcuts, which test exact bytes, depend on [#byteExact()].
    record BinaryInPredicate(int columnIndex, byte[][] values, BinaryPredicate.Comparison comparison)
            implements ResolvedPredicate {

        /// Whether the column encodes a value as exactly these bytes. See
        /// [BinaryPredicate.Comparison#byteExact()].
        public boolean byteExact() {
            return comparison.byteExact();
        }
    }
    record DoubleInPredicate(int columnIndex, double[] values, boolean floatColumn,
            boolean ieee754TotalOrder) implements ResolvedPredicate {}

    /// Membership against a `FLOAT16` column, each probe compared with the decoded half the way
    /// [DoubleInPredicate] compares a `FLOAT` column's widened values, so a probe no half can
    /// represent matches nothing. `ieee754TotalOrder` is as on [Float16Predicate].
    record Float16InPredicate(int columnIndex, double[] values, boolean ieee754TotalOrder)
            implements ResolvedPredicate {}

    /// Every non-null row of the leaf, and no null one: the answer to an ordered predicate whose
    /// literal lies past every value the column can hold, in the direction the predicate admits.
    ///
    /// It is not [IsNotNullPredicate]. The two match the same rows, but they negate differently:
    /// a comparison is unknown on a null row and stays unknown under `not`, so the negation of
    /// "every non-null row" is [NoRowPredicate] rather than a test that returns the null rows.
    /// That difference is why the pair is carried as two predicates of its own — using
    /// [IsNotNullPredicate] as the stand-in made `not(not(p))` return exactly the rows `p`
    /// excludes for being null.
    record EveryNonNullRowPredicate(int columnIndex) implements ResolvedPredicate {}

    /// No row at all, the negation of [EveryNonNullRowPredicate].
    record NoRowPredicate(int columnIndex) implements ResolvedPredicate {}

    /// A test for the absence of the node named by the predicate, which is either the leaf column
    /// `columnIndex` itself or a non-repeated group enclosing it.
    ///
    /// `definitionLevel` is the level at or above which that node is present, and
    /// `leafDefinitionLevel` is the leaf column's maximum definition level. The gap between them
    /// is where a present group with a null child sits. A leaf predicate has the two equal, and so
    /// does a group with only required nodes down to the leaf, which is null exactly where the
    /// group is absent and is answered as that leaf. Carrying both means an evaluator can tell the
    /// two apart, and can size a definition level histogram, without the schema the resolver read
    /// them from.
    record IsNullPredicate(int columnIndex, int definitionLevel, int leafDefinitionLevel)
            implements ResolvedPredicate {

        public IsNullPredicate {
            checkDefinitionLevels(definitionLevel, leafDefinitionLevel);
        }

        /// A predicate on the leaf column itself, whose two definition levels are the same.
        public IsNullPredicate(int columnIndex, int definitionLevel) {
            this(columnIndex, definitionLevel, definitionLevel);
        }

        /// Whether a definition level separates the tested node from the leaf, so that the leaf
        /// can be null where the node is present. `false` for the leaf itself and for a group with
        /// only required nodes down to it.
        public boolean group() {
            return definitionLevel < leafDefinitionLevel;
        }
    }

    /// The negation of [IsNullPredicate], with the same two definition levels.
    record IsNotNullPredicate(int columnIndex, int definitionLevel, int leafDefinitionLevel)
            implements ResolvedPredicate {

        public IsNotNullPredicate {
            checkDefinitionLevels(definitionLevel, leafDefinitionLevel);
        }

        /// A predicate on the leaf column itself, whose two definition levels are the same.
        public IsNotNullPredicate(int columnIndex, int definitionLevel) {
            this(columnIndex, definitionLevel, definitionLevel);
        }

        /// Whether a definition level separates the tested node from the leaf, so that the leaf
        /// can be null where the node is present. `false` for the leaf itself and for a group with
        /// only required nodes down to it.
        public boolean group() {
            return definitionLevel < leafDefinitionLevel;
        }
    }

    private static void checkDefinitionLevels(int definitionLevel, int leafDefinitionLevel) {
        if (definitionLevel < 0 || definitionLevel > leafDefinitionLevel) {
            throw new IllegalArgumentException(
                    "Definition level of a null predicate must be between 0 and the leaf column's "
                            + "maximum definition level " + leafDefinitionLevel + ", but was " + definitionLevel);
        }
    }

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
            case UnsignedIntPredicate p -> p.columnIndex();
            case UnsignedLongPredicate p -> p.columnIndex();
            case FloatPredicate p -> p.columnIndex();
            case Float16Predicate p -> p.columnIndex();
            case DoublePredicate p -> p.columnIndex();
            case BooleanPredicate p -> p.columnIndex();
            case BinaryPredicate p -> p.columnIndex();
            case IntInPredicate p -> p.columnIndex();
            case LongInPredicate p -> p.columnIndex();
            case UnsignedIntInPredicate p -> p.columnIndex();
            case UnsignedLongInPredicate p -> p.columnIndex();
            case BinaryInPredicate p -> p.columnIndex();
            case DoubleInPredicate p -> p.columnIndex();
            case Float16InPredicate p -> p.columnIndex();
            case IsNullPredicate p -> p.columnIndex();
            case IsNotNullPredicate p -> p.columnIndex();
            case EveryNonNullRowPredicate p -> p.columnIndex();
            case NoRowPredicate p -> p.columnIndex();
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
            case UnsignedIntPredicate p -> new UnsignedIntPredicate(mapped(p.columnIndex(), columnMapping),
                    p.op(), p.value());
            case UnsignedLongPredicate p -> new UnsignedLongPredicate(mapped(p.columnIndex(), columnMapping),
                    p.op(), p.value());
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
            case UnsignedIntInPredicate p -> new UnsignedIntInPredicate(mapped(p.columnIndex(), columnMapping),
                    p.values());
            case UnsignedLongInPredicate p -> new UnsignedLongInPredicate(mapped(p.columnIndex(), columnMapping),
                    p.values());
            case BinaryInPredicate p -> new BinaryInPredicate(mapped(p.columnIndex(), columnMapping), p.values(),
                    p.comparison());
            case DoubleInPredicate p -> new DoubleInPredicate(mapped(p.columnIndex(), columnMapping), p.values(),
                    p.floatColumn(), p.ieee754TotalOrder());
            case Float16InPredicate p -> new Float16InPredicate(mapped(p.columnIndex(), columnMapping), p.values(),
                    p.ieee754TotalOrder());
            case IsNullPredicate p -> new IsNullPredicate(
                    mapped(p.columnIndex(), columnMapping), p.definitionLevel(), p.leafDefinitionLevel());
            case IsNotNullPredicate p -> new IsNotNullPredicate(
                    mapped(p.columnIndex(), columnMapping), p.definitionLevel(), p.leafDefinitionLevel());
            case EveryNonNullRowPredicate p -> new EveryNonNullRowPredicate(mapped(p.columnIndex(), columnMapping));
            case NoRowPredicate p -> new NoRowPredicate(mapped(p.columnIndex(), columnMapping));
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
            case UnsignedIntPredicate p -> new UnsignedIntPredicate(p.columnIndex(), p.op().invert(),
                    p.value());
            case UnsignedLongPredicate p -> new UnsignedLongPredicate(p.columnIndex(), p.op().invert(),
                    p.value());
            case FloatPredicate p -> new FloatPredicate(p.columnIndex(), p.op().invert(), p.value(),
                    p.ieee754TotalOrder());
            case Float16Predicate p -> new Float16Predicate(p.columnIndex(), p.op().invert(), p.value(),
                    p.ieee754TotalOrder());
            case DoublePredicate p -> new DoublePredicate(p.columnIndex(), p.op().invert(), p.value(),
                    p.ieee754TotalOrder());
            case BooleanPredicate p -> new BooleanPredicate(p.columnIndex(), p.op().invert(), p.value());
            case BinaryPredicate p -> new BinaryPredicate(p.columnIndex(), p.op().invert(), p.value(),
                    p.comparison());
            case IsNullPredicate p -> new IsNotNullPredicate(p.columnIndex(), p.definitionLevel(),
                    p.leafDefinitionLevel());
            case IsNotNullPredicate p -> new IsNullPredicate(p.columnIndex(), p.definitionLevel(),
                    p.leafDefinitionLevel());
            case EveryNonNullRowPredicate p -> new NoRowPredicate(p.columnIndex());
            case NoRowPredicate p -> new EveryNonNullRowPredicate(p.columnIndex());
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
            case UnsignedIntInPredicate p -> {
                List<ResolvedPredicate> notEqs = new ArrayList<>(p.values().length);
                for (int value : p.values()) {
                    notEqs.add(new UnsignedIntPredicate(p.columnIndex(),
                            FilterPredicate.Operator.NOT_EQ, value));
                }
                yield new And(notEqs);
            }
            case UnsignedLongInPredicate p -> {
                List<ResolvedPredicate> notEqs = new ArrayList<>(p.values().length);
                for (long value : p.values()) {
                    notEqs.add(new UnsignedLongPredicate(p.columnIndex(),
                            FilterPredicate.Operator.NOT_EQ, value));
                }
                yield new And(notEqs);
            }
            case BinaryInPredicate p -> {
                List<ResolvedPredicate> notEqs = new ArrayList<>(p.values().length);
                for (byte[] value : p.values()) {
                    notEqs.add(new BinaryPredicate(p.columnIndex(), FilterPredicate.Operator.NOT_EQ, value,
                            p.comparison()));
                }
                yield new And(notEqs);
            }
            case Float16InPredicate p -> {
                // A probe no half can represent is never equal to a stored value, so it drops out of
                // the conjunction rather than narrowing onto a half `in` would not have matched; with
                // none left, every non-null row is outside the set.
                List<ResolvedPredicate> notEqs = new ArrayList<>(p.values().length);
                for (double v : p.values()) {
                    if (Double.isNaN(v) || isFloat16(v)) {
                        notEqs.add(new Float16Predicate(p.columnIndex(), FilterPredicate.Operator.NOT_EQ,
                                (float) v, p.ieee754TotalOrder()));
                    }
                }
                if (notEqs.isEmpty()) {
                    yield new EveryNonNullRowPredicate(p.columnIndex());
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
                        yield new EveryNonNullRowPredicate(p.columnIndex());
                    }
                    yield new And(notEqs);
                }
                List<ResolvedPredicate> notEqs = new ArrayList<>(p.values().length);
                for (double value : p.values()) {
                    notEqs.add(new DoublePredicate(p.columnIndex(), FilterPredicate.Operator.NOT_EQ, value));
                }
                yield new And(notEqs);
            }
            case GeospatialPredicate p -> throw new IllegalStateException(
                    "A spatial intersects predicate on column " + p.columnIndex() + " reached"
                            + " negation; the resolver refuses one below not");
        };
    }

    /// Whether `value` is exactly a `FLOAT16`: representable as a `float`, and that `float` as a
    /// half.
    private static boolean isFloat16(double value) {
        float asFloat = (float) value;
        return asFloat == value && Float.float16ToFloat(Float.floatToFloat16(asFloat)) == asFloat;
    }
}
