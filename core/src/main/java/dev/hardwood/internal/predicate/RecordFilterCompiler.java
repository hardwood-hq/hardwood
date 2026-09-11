/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.util.List;
import java.util.function.IntUnaryOperator;

import dev.hardwood.reader.FilterPredicate.Operator;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.StructAccessor;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;

/// Compiles a [ResolvedPredicate] into a [RowMatcher] tree once per reader.
///
/// All field-name lookups, struct-path resolutions, and operator decisions
/// are performed at compile time. The returned matcher only reads values
/// and runs comparisons per row, eliminating the type and operator
/// switches a generic tree-walking evaluator would perform for every row.
public final class RecordFilterCompiler {

    static final String[] EMPTY_PATH = new String[0];

    private RecordFilterCompiler() {
    }

    public static RowMatcher compile(ResolvedPredicate predicate, FileSchema schema) {
        return compile(predicate, schema, null);
    }

    /// Indexed-access overload: when the row reader is known to be a
    /// [RowReader] whose `getXxx(int)` accessors can address top-level
    /// fields directly, pass a `topLevelFieldIndex` callback that maps a
    /// **file leaf-column index** to the **field index** the reader's
    /// indexed accessors expect. The function should return `-1` for
    /// columns that aren't directly addressable that way (e.g. not in the
    /// projection); the compiler then falls back to the name-keyed leaf.
    ///
    /// The semantic of the returned index differs by reader:
    /// - [dev.hardwood.internal.reader.FlatRowReader]: projected leaf-column
    ///   index (since for flat schemas every leaf is a top-level field).
    /// - [dev.hardwood.internal.reader.NestedRowReader]: projected
    ///   top-level field index in the row reader's projected fields.
    ///
    /// Nested paths (path length > 1) always use the name-keyed leaves
    /// regardless, since indexed access is only meaningful for top-level
    /// columns.
    public static RowMatcher compile(ResolvedPredicate predicate, FileSchema schema,
            IntUnaryOperator topLevelFieldIndex) {
        return switch (predicate) {
            case ResolvedPredicate.IntPredicate p -> {
                int idx = indexedTopLevel(schema, p.columnIndex(), topLevelFieldIndex);
                yield idx >= 0
                        ? indexedIntLeaf(idx, p.op(), p.value())
                        : intLeaf(pathSegments(schema, p.columnIndex()), leafName(schema, p.columnIndex()), p.op(), p.value());
            }
            case ResolvedPredicate.LongPredicate p -> {
                int idx = indexedTopLevel(schema, p.columnIndex(), topLevelFieldIndex);
                yield idx >= 0
                        ? indexedLongLeaf(idx, p.op(), p.value())
                        : longLeaf(pathSegments(schema, p.columnIndex()), leafName(schema, p.columnIndex()), p.op(), p.value());
            }
            case ResolvedPredicate.UnsignedIntPredicate p -> {
                int idx = indexedTopLevel(schema, p.columnIndex(), topLevelFieldIndex);
                yield idx >= 0
                        ? indexedUnsignedIntLeaf(idx, p.op(), p.value())
                        : unsignedIntLeaf(pathSegments(schema, p.columnIndex()), leafName(schema, p.columnIndex()), p.op(), p.value());
            }
            case ResolvedPredicate.UnsignedLongPredicate p -> {
                int idx = indexedTopLevel(schema, p.columnIndex(), topLevelFieldIndex);
                yield idx >= 0
                        ? indexedUnsignedLongLeaf(idx, p.op(), p.value())
                        : unsignedLongLeaf(pathSegments(schema, p.columnIndex()), leafName(schema, p.columnIndex()), p.op(), p.value());
            }
            case ResolvedPredicate.FloatPredicate p -> {
                int idx = indexedTopLevel(schema, p.columnIndex(), topLevelFieldIndex);
                yield idx >= 0
                        ? indexedFloatLeaf(idx, p.op(), p.value())
                        : floatLeaf(pathSegments(schema, p.columnIndex()), leafName(schema, p.columnIndex()), p.op(), p.value());
            }
            case ResolvedPredicate.Float16Predicate p -> {
                // FLOAT16 record-level eval reuses floatLeaf because `getFloat`
                // dispatches on the column's logical type and decodes the 2-byte
                // payload itself. The Float16Predicate distinction matters for
                // stats pushdown (different decode width on min/max bytes), not
                // for per-row reads.
                int idx = indexedTopLevel(schema, p.columnIndex(), topLevelFieldIndex);
                yield idx >= 0
                        ? indexedFloatLeaf(idx, p.op(), p.value())
                        : floatLeaf(pathSegments(schema, p.columnIndex()), leafName(schema, p.columnIndex()), p.op(), p.value());
            }
            case ResolvedPredicate.DoublePredicate p -> {
                int idx = indexedTopLevel(schema, p.columnIndex(), topLevelFieldIndex);
                yield idx >= 0
                        ? indexedDoubleLeaf(idx, p.op(), p.value())
                        : doubleLeaf(pathSegments(schema, p.columnIndex()), leafName(schema, p.columnIndex()), p.op(), p.value());
            }
            case ResolvedPredicate.BooleanPredicate p -> {
                int idx = indexedTopLevel(schema, p.columnIndex(), topLevelFieldIndex);
                yield idx >= 0
                        ? indexedBooleanLeaf(idx, p.op(), p.value())
                        : booleanLeaf(pathSegments(schema, p.columnIndex()), leafName(schema, p.columnIndex()), p.op(), p.value());
            }
            case ResolvedPredicate.BinaryPredicate p ->
                    binaryLeaf(pathSegments(schema, p.columnIndex()), leafName(schema, p.columnIndex()),
                            p.op(), p.value(), p.signed());
            case ResolvedPredicate.IntInPredicate p ->
                    intInLeaf(pathSegments(schema, p.columnIndex()), leafName(schema, p.columnIndex()), p.values());
            case ResolvedPredicate.LongInPredicate p ->
                    longInLeaf(pathSegments(schema, p.columnIndex()), leafName(schema, p.columnIndex()), p.values());
            // Membership is bit equality, which reads the same signed or unsigned, so an unsigned
            // IN list matches through the same leaf as a signed one.
            case ResolvedPredicate.UnsignedIntInPredicate p ->
                    intInLeaf(pathSegments(schema, p.columnIndex()), leafName(schema, p.columnIndex()), p.values());
            case ResolvedPredicate.UnsignedLongInPredicate p ->
                    longInLeaf(pathSegments(schema, p.columnIndex()), leafName(schema, p.columnIndex()), p.values());
            case ResolvedPredicate.BinaryInPredicate p ->
                    binaryInLeaf(pathSegments(schema, p.columnIndex()), leafName(schema, p.columnIndex()), p.values(),
                            p.signed());
            case ResolvedPredicate.DoubleInPredicate p ->
                    doubleInLeaf(pathSegments(schema, p.columnIndex()), leafName(schema, p.columnIndex()), p.values(), p.floatColumn());
            // `getFloat` decodes a FLOAT16 column's two bytes itself, as for Float16Predicate, so
            // membership reads the half through the FLOAT path.
            case ResolvedPredicate.Float16InPredicate p ->
                    doubleInLeaf(pathSegments(schema, p.columnIndex()), leafName(schema, p.columnIndex()), p.values(), true);
            case ResolvedPredicate.IsNullPredicate p -> {
                if (!p.group()) {
                    int idx = indexedTopLevel(schema, p.columnIndex(), topLevelFieldIndex);
                    yield idx >= 0
                            ? indexedIsNullLeaf(idx)
                            : isNullLeaf(pathSegments(schema, p.columnIndex()), leafName(schema, p.columnIndex()));
                }

                NullFieldTarget target = nullFieldTarget(schema, p.columnIndex(), p.definitionLevel());
                yield isNullLeaf(target.path(), target.name());
            }
            case ResolvedPredicate.IsNotNullPredicate p -> {
                if (!p.group()) {
                    int idx = indexedTopLevel(schema, p.columnIndex(), topLevelFieldIndex);
                    yield idx >= 0
                            ? indexedIsNotNullLeaf(idx)
                            : isNotNullLeaf(pathSegments(schema, p.columnIndex()), leafName(schema, p.columnIndex()));
                }

                NullFieldTarget target = nullFieldTarget(schema, p.columnIndex(), p.definitionLevel());
                yield isNotNullLeaf(target.path(), target.name());
            }
            // Every non-null row of the leaf and no row at all: the same rows an IS NOT NULL
            // test and an unsatisfiable comparison return, which is what a comparison whose
            // literal lies past the column's range answers.
            case ResolvedPredicate.EveryNonNullRowPredicate p -> {
                int idx = indexedTopLevel(schema, p.columnIndex(), topLevelFieldIndex);
                yield idx >= 0
                        ? indexedIsNotNullLeaf(idx)
                        : isNotNullLeaf(pathSegments(schema, p.columnIndex()), leafName(schema, p.columnIndex()));
            }
            case ResolvedPredicate.NoRowPredicate ignored -> row -> false;
            case ResolvedPredicate.And and -> compileAnd(and.children(), schema, topLevelFieldIndex);
            case ResolvedPredicate.Or or -> compileOr(or.children(), schema, topLevelFieldIndex);
            // Spatial intersects is bbox-only pushdown (row group + page level). Per-row WKB
            // decoding is left to the caller, so every surviving row passes here.
            case ResolvedPredicate.GeospatialPredicate p -> row -> true;
        };
    }

    /// Returns the reader field index for a top-level column, or `-1` when
    /// the leaf cannot use indexed access — either because it isn't
    /// top-level (path length > 1), no callback was supplied, or the
    /// callback declines to map this column.
    static int indexedTopLevel(FileSchema schema, int columnIndex,
            IntUnaryOperator topLevelFieldIndex) {
        if (topLevelFieldIndex == null) {
            return -1;
        }
        if (schema.getColumn(columnIndex).fieldPath().elements().size() > 1) {
            return -1;
        }
        return topLevelFieldIndex.applyAsInt(columnIndex);
    }

    // ==================== Compounds ====================

    private static RowMatcher compileAnd(List<ResolvedPredicate> children, FileSchema schema,
            IntUnaryOperator topLevelFieldIndex) {
        RowMatcher[] compiled = compileAll(children, schema, topLevelFieldIndex);
        return switch (compiled.length) {
            case 1 -> compiled[0];
            case 2 -> new And2Matcher(compiled[0], compiled[1]);
            case 3 -> new And3Matcher(compiled[0], compiled[1], compiled[2]);
            case 4 -> new And4Matcher(compiled[0], compiled[1], compiled[2], compiled[3]);
            default -> new AndNMatcher(compiled);
        };
    }

    private static RowMatcher compileOr(List<ResolvedPredicate> children, FileSchema schema,
            IntUnaryOperator topLevelFieldIndex) {
        RowMatcher[] compiled = compileAll(children, schema, topLevelFieldIndex);
        return switch (compiled.length) {
            case 1 -> compiled[0];
            case 2 -> new Or2Matcher(compiled[0], compiled[1]);
            case 3 -> new Or3Matcher(compiled[0], compiled[1], compiled[2]);
            case 4 -> new Or4Matcher(compiled[0], compiled[1], compiled[2], compiled[3]);
            default -> new OrNMatcher(compiled);
        };
    }

    // ==================== Fixed-arity AND/OR matchers ====================
    //
    // Final-field classes give the JIT statically-known children at each call
    // site. Since each leaf type/op produces a distinct lambda class, the
    // call sites `a.test(row)`, `b.test(row)`, ... see one specific receiver
    // type per query and inline aggressively — effectively fusing the leaf
    // bodies at runtime without combinatorial code in the source.

    private static final class And2Matcher implements RowMatcher {
        private final RowMatcher a;
        private final RowMatcher b;
        And2Matcher(RowMatcher a, RowMatcher b) {
            this.a = a;
            this.b = b;
        }
        @Override
        public boolean test(StructAccessor row) {
            return a.test(row) && b.test(row);
        }
    }

    private static final class And3Matcher implements RowMatcher {
        private final RowMatcher a;
        private final RowMatcher b;
        private final RowMatcher c;
        And3Matcher(RowMatcher a, RowMatcher b, RowMatcher c) {
            this.a = a;
            this.b = b;
            this.c = c;
        }
        @Override
        public boolean test(StructAccessor row) {
            return a.test(row) && b.test(row) && c.test(row);
        }
    }

    private static final class And4Matcher implements RowMatcher {
        private final RowMatcher a;
        private final RowMatcher b;
        private final RowMatcher c;
        private final RowMatcher d;
        And4Matcher(RowMatcher a, RowMatcher b, RowMatcher c, RowMatcher d) {
            this.a = a;
            this.b = b;
            this.c = c;
            this.d = d;
        }
        @Override
        public boolean test(StructAccessor row) {
            return a.test(row) && b.test(row) && c.test(row) && d.test(row);
        }
    }

    private static final class AndNMatcher implements RowMatcher {
        private final RowMatcher[] children;
        AndNMatcher(RowMatcher[] children) {
            this.children = children;
        }
        @Override
        public boolean test(StructAccessor row) {
            for (int i = 0; i < children.length; i++) {
                if (!children[i].test(row)) {
                    return false;
                }
            }
            return true;
        }
    }

    private static final class Or2Matcher implements RowMatcher {
        private final RowMatcher a;
        private final RowMatcher b;
        Or2Matcher(RowMatcher a, RowMatcher b) {
            this.a = a;
            this.b = b;
        }
        @Override
        public boolean test(StructAccessor row) {
            return a.test(row) || b.test(row);
        }
    }

    private static final class Or3Matcher implements RowMatcher {
        private final RowMatcher a;
        private final RowMatcher b;
        private final RowMatcher c;
        Or3Matcher(RowMatcher a, RowMatcher b, RowMatcher c) {
            this.a = a;
            this.b = b;
            this.c = c;
        }
        @Override
        public boolean test(StructAccessor row) {
            return a.test(row) || b.test(row) || c.test(row);
        }
    }

    private static final class Or4Matcher implements RowMatcher {
        private final RowMatcher a;
        private final RowMatcher b;
        private final RowMatcher c;
        private final RowMatcher d;
        Or4Matcher(RowMatcher a, RowMatcher b, RowMatcher c, RowMatcher d) {
            this.a = a;
            this.b = b;
            this.c = c;
            this.d = d;
        }
        @Override
        public boolean test(StructAccessor row) {
            return a.test(row) || b.test(row) || c.test(row) || d.test(row);
        }
    }

    private static final class OrNMatcher implements RowMatcher {
        private final RowMatcher[] children;
        OrNMatcher(RowMatcher[] children) {
            this.children = children;
        }
        @Override
        public boolean test(StructAccessor row) {
            for (int i = 0; i < children.length; i++) {
                if (children[i].test(row)) {
                    return true;
                }
            }
            return false;
        }
    }

    private static RowMatcher[] compileAll(List<ResolvedPredicate> children, FileSchema schema,
            IntUnaryOperator topLevelFieldIndex) {
        RowMatcher[] out = new RowMatcher[children.size()];
        for (int i = 0; i < out.length; i++) {
            out[i] = compile(children.get(i), schema, topLevelFieldIndex);
        }
        return out;
    }

    // ==================== Name-keyed leaf factories ====================
    //
    // Each factory returns a different lambda per operator. The switch on
    // op happens once at compile time; the returned lambda has the operator
    // baked in as a literal comparison — no per-row dispatch.
    //
    // `path` is the array of intermediate struct names (empty for top-level).
    // `name` is the leaf field name.

    private static RowMatcher intLeaf(String[] path, String name, Operator op, int v) {
        return switch (op) {
            case EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && a.getInt(name) == v; };
            case NOT_EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && a.getInt(name) != v; };
            case LT -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && a.getInt(name) < v; };
            case LT_EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && a.getInt(name) <= v; };
            case GT -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && a.getInt(name) > v; };
            case GT_EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && a.getInt(name) >= v; };
        };
    }

    private static RowMatcher longLeaf(String[] path, String name, Operator op, long v) {
        return switch (op) {
            case EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && a.getLong(name) == v; };
            case NOT_EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && a.getLong(name) != v; };
            case LT -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && a.getLong(name) < v; };
            case LT_EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && a.getLong(name) <= v; };
            case GT -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && a.getLong(name) > v; };
            case GT_EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && a.getLong(name) >= v; };
        };
    }

    // Float and Double use Float.compare / Double.compare so NaN orders after
    // all other values (and -0.0 < +0.0), consistent with the file-level stats path.

    private static RowMatcher floatLeaf(String[] path, String name, Operator op, float v) {
        return switch (op) {
            case EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && Float.compare(a.getFloat(name), v) == 0; };
            case NOT_EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && Float.compare(a.getFloat(name), v) != 0; };
            case LT -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && Float.compare(a.getFloat(name), v) < 0; };
            case LT_EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && Float.compare(a.getFloat(name), v) <= 0; };
            case GT -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && Float.compare(a.getFloat(name), v) > 0; };
            case GT_EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && Float.compare(a.getFloat(name), v) >= 0; };
        };
    }

    private static RowMatcher doubleLeaf(String[] path, String name, Operator op, double v) {
        return switch (op) {
            case EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && Double.compare(a.getDouble(name), v) == 0; };
            case NOT_EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && Double.compare(a.getDouble(name), v) != 0; };
            case LT -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && Double.compare(a.getDouble(name), v) < 0; };
            case LT_EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && Double.compare(a.getDouble(name), v) <= 0; };
            case GT -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && Double.compare(a.getDouble(name), v) > 0; };
            case GT_EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && Double.compare(a.getDouble(name), v) >= 0; };
        };
    }

    private static RowMatcher booleanLeaf(String[] path, String name, Operator op, boolean v) {
        // BooleanPredicate honours only EQ and NOT_EQ; matchesRow returns true for any other op
        // when the value is non-null (equivalent to a non-null check).
        return switch (op) {
            case EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && a.getBoolean(name) == v; };
            case NOT_EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && a.getBoolean(name) != v; };
            default -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name); };
        };
    }

    private static RowMatcher binaryLeaf(String[] path, String name, Operator op, byte[] v, boolean signed) {
        return switch (op) {
            case EQ -> row -> {
                StructAccessor a = resolve(row, path);
                if (a == null || a.isNull(name)) return false;
                return compareBinary(a.getBinary(name), v, signed) == 0;
            };
            case NOT_EQ -> row -> {
                StructAccessor a = resolve(row, path);
                if (a == null || a.isNull(name)) return false;
                return compareBinary(a.getBinary(name), v, signed) != 0;
            };
            case LT -> row -> {
                StructAccessor a = resolve(row, path);
                if (a == null || a.isNull(name)) return false;
                return compareBinary(a.getBinary(name), v, signed) < 0;
            };
            case LT_EQ -> row -> {
                StructAccessor a = resolve(row, path);
                if (a == null || a.isNull(name)) return false;
                return compareBinary(a.getBinary(name), v, signed) <= 0;
            };
            case GT -> row -> {
                StructAccessor a = resolve(row, path);
                if (a == null || a.isNull(name)) return false;
                return compareBinary(a.getBinary(name), v, signed) > 0;
            };
            case GT_EQ -> row -> {
                StructAccessor a = resolve(row, path);
                if (a == null || a.isNull(name)) return false;
                return compareBinary(a.getBinary(name), v, signed) >= 0;
            };
        };
    }

    static int compareBinary(byte[] left, byte[] right, boolean signed) {
        return signed
                ? BinaryComparator.compareSigned(left, right)
                : BinaryComparator.compareUnsigned(left, right);
    }

    private static RowMatcher intInLeaf(String[] path, String name, int[] values) {
        return row -> {
            StructAccessor a = resolve(row, path);
            if (a == null || a.isNull(name)) return false;
            int val = a.getInt(name);
            for (int value : values) {
                if (value == val) return true;
            }
            return false;
        };
    }

    private static RowMatcher longInLeaf(String[] path, String name, long[] values) {
        return row -> {
            StructAccessor a = resolve(row, path);
            if (a == null || a.isNull(name)) return false;
            long val = a.getLong(name);
            for (long value : values) {
                if (value == val) return true;
            }
            return false;
        };
    }

    /// Membership in the column's order, like [#binaryLeaf]'s `EQ`: a `DECIMAL` compares by value,
    /// so a padded encoding of a probe is still a member.
    private static RowMatcher binaryInLeaf(String[] path, String name, byte[][] values, boolean signed) {
        return row -> {
            StructAccessor a = resolve(row, path);
            if (a == null || a.isNull(name)) return false;
            byte[] val = a.getBinary(name);
            for (byte[] value : values) {
                if (compareBinary(val, value, signed) == 0) return true;
            }
            return false;
        };
    }

    private static RowMatcher doubleInLeaf(String[] path, String name, double[] values, boolean floatColumn) {
        if (floatColumn) {
            return row -> {
                StructAccessor a = resolve(row, path);
                if (a == null || a.isNull(name)) return false;
                double val = a.getFloat(name);
                for (double member : values) {
                    if (Double.compare(val, member) == 0) return true;
                }
                return false;
            };
        }
        return row -> {
            StructAccessor a = resolve(row, path);
            if (a == null || a.isNull(name)) return false;
            double val = a.getDouble(name);
            for (double member : values) {
                if (Double.compare(val, member) == 0) return true;
            }
            return false;
        };
    }

    private static RowMatcher isNullLeaf(String[] path, String name) {
        return row -> {
            StructAccessor a = resolve(row, path);
            return a == null || a.isNull(name);
        };
    }

    private static RowMatcher isNotNullLeaf(String[] path, String name) {
        return row -> {
            StructAccessor a = resolve(row, path);
            return a != null && !a.isNull(name);
        };
    }

    // ==================== Indexed leaf factories ====================
    //
    // Used when the row is known to be a [RowReader] and the leaf operates
    // on a top-level column. The cast is safe by construction — the matcher is
    // invoked only by a row reader, which passes itself. The compiler emits these
    // leaves only when the caller passes a `topLevelFieldIndex` callback,
    // which today is done by both [dev.hardwood.internal.reader.FlatRowReader]
    // and [dev.hardwood.internal.reader.NestedRowReader].

    /// Record-level comparison for an unsigned `INT32` column. `EQ` and `NOT_EQ` read the same
    /// under either interpretation, so they reuse the signed leaf; the four ordered operators
    /// bias both sides by `Integer.MIN_VALUE`, which reorders the two halves of the range
    /// without disturbing the order within either.
    private static RowMatcher indexedUnsignedIntLeaf(int idx, Operator op, int v) {
        int b = v ^ Integer.MIN_VALUE;
        return switch (op) {
            case EQ, NOT_EQ -> indexedIntLeaf(idx, op, v);
            case LT -> row -> { RowReader r = (RowReader) row; return !r.isNull(idx) && (r.getInt(idx) ^ Integer.MIN_VALUE) < b; };
            case LT_EQ -> row -> { RowReader r = (RowReader) row; return !r.isNull(idx) && (r.getInt(idx) ^ Integer.MIN_VALUE) <= b; };
            case GT -> row -> { RowReader r = (RowReader) row; return !r.isNull(idx) && (r.getInt(idx) ^ Integer.MIN_VALUE) > b; };
            case GT_EQ -> row -> { RowReader r = (RowReader) row; return !r.isNull(idx) && (r.getInt(idx) ^ Integer.MIN_VALUE) >= b; };
        };
    }

    /// Name-keyed counterpart of [#indexedUnsignedIntLeaf].
    private static RowMatcher unsignedIntLeaf(String[] path, String name, Operator op, int v) {
        int b = v ^ Integer.MIN_VALUE;
        return switch (op) {
            case EQ, NOT_EQ -> intLeaf(path, name, op, v);
            case LT -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && (a.getInt(name) ^ Integer.MIN_VALUE) < b; };
            case LT_EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && (a.getInt(name) ^ Integer.MIN_VALUE) <= b; };
            case GT -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && (a.getInt(name) ^ Integer.MIN_VALUE) > b; };
            case GT_EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && (a.getInt(name) ^ Integer.MIN_VALUE) >= b; };
        };
    }

    /// Record-level comparison for an unsigned `INT64` column; see [#indexedUnsignedIntLeaf].
    private static RowMatcher indexedUnsignedLongLeaf(int idx, Operator op, long v) {
        long b = v ^ Long.MIN_VALUE;
        return switch (op) {
            case EQ, NOT_EQ -> indexedLongLeaf(idx, op, v);
            case LT -> row -> { RowReader r = (RowReader) row; return !r.isNull(idx) && (r.getLong(idx) ^ Long.MIN_VALUE) < b; };
            case LT_EQ -> row -> { RowReader r = (RowReader) row; return !r.isNull(idx) && (r.getLong(idx) ^ Long.MIN_VALUE) <= b; };
            case GT -> row -> { RowReader r = (RowReader) row; return !r.isNull(idx) && (r.getLong(idx) ^ Long.MIN_VALUE) > b; };
            case GT_EQ -> row -> { RowReader r = (RowReader) row; return !r.isNull(idx) && (r.getLong(idx) ^ Long.MIN_VALUE) >= b; };
        };
    }

    /// Name-keyed counterpart of [#indexedUnsignedLongLeaf].
    private static RowMatcher unsignedLongLeaf(String[] path, String name, Operator op, long v) {
        long b = v ^ Long.MIN_VALUE;
        return switch (op) {
            case EQ, NOT_EQ -> longLeaf(path, name, op, v);
            case LT -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && (a.getLong(name) ^ Long.MIN_VALUE) < b; };
            case LT_EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && (a.getLong(name) ^ Long.MIN_VALUE) <= b; };
            case GT -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && (a.getLong(name) ^ Long.MIN_VALUE) > b; };
            case GT_EQ -> row -> { StructAccessor a = resolve(row, path); return a != null && !a.isNull(name) && (a.getLong(name) ^ Long.MIN_VALUE) >= b; };
        };
    }

    private static RowMatcher indexedIntLeaf(int idx, Operator op, int v) {
        return switch (op) {
            case EQ -> row -> { RowReader r = (RowReader) row; return !r.isNull(idx) && r.getInt(idx) == v; };
            case NOT_EQ -> row -> { RowReader r = (RowReader) row; return !r.isNull(idx) && r.getInt(idx) != v; };
            case LT -> row -> { RowReader r = (RowReader) row; return !r.isNull(idx) && r.getInt(idx) < v; };
            case LT_EQ -> row -> { RowReader r = (RowReader) row; return !r.isNull(idx) && r.getInt(idx) <= v; };
            case GT -> row -> { RowReader r = (RowReader) row; return !r.isNull(idx) && r.getInt(idx) > v; };
            case GT_EQ -> row -> { RowReader r = (RowReader) row; return !r.isNull(idx) && r.getInt(idx) >= v; };
        };
    }

    private static RowMatcher indexedLongLeaf(int idx, Operator op, long v) {
        return switch (op) {
            case EQ -> row -> { RowReader r = (RowReader) row; return !r.isNull(idx) && r.getLong(idx) == v; };
            case NOT_EQ -> row -> { RowReader r = (RowReader) row; return !r.isNull(idx) && r.getLong(idx) != v; };
            case LT -> row -> { RowReader r = (RowReader) row; return !r.isNull(idx) && r.getLong(idx) < v; };
            case LT_EQ -> row -> { RowReader r = (RowReader) row; return !r.isNull(idx) && r.getLong(idx) <= v; };
            case GT -> row -> { RowReader r = (RowReader) row; return !r.isNull(idx) && r.getLong(idx) > v; };
            case GT_EQ -> row -> { RowReader r = (RowReader) row; return !r.isNull(idx) && r.getLong(idx) >= v; };
        };
    }

    private static RowMatcher indexedFloatLeaf(int idx, Operator op, float v) {
        return switch (op) {
            case EQ -> row -> { RowReader r = (RowReader) row; return !r.isNull(idx) && Float.compare(r.getFloat(idx), v) == 0; };
            case NOT_EQ -> row -> { RowReader r = (RowReader) row; return !r.isNull(idx) && Float.compare(r.getFloat(idx), v) != 0; };
            case LT -> row -> { RowReader r = (RowReader) row; return !r.isNull(idx) && Float.compare(r.getFloat(idx), v) < 0; };
            case LT_EQ -> row -> { RowReader r = (RowReader) row; return !r.isNull(idx) && Float.compare(r.getFloat(idx), v) <= 0; };
            case GT -> row -> { RowReader r = (RowReader) row; return !r.isNull(idx) && Float.compare(r.getFloat(idx), v) > 0; };
            case GT_EQ -> row -> { RowReader r = (RowReader) row; return !r.isNull(idx) && Float.compare(r.getFloat(idx), v) >= 0; };
        };
    }

    private static RowMatcher indexedDoubleLeaf(int idx, Operator op, double v) {
        return switch (op) {
            case EQ -> row -> { RowReader r = (RowReader) row; return !r.isNull(idx) && Double.compare(r.getDouble(idx), v) == 0; };
            case NOT_EQ -> row -> { RowReader r = (RowReader) row; return !r.isNull(idx) && Double.compare(r.getDouble(idx), v) != 0; };
            case LT -> row -> { RowReader r = (RowReader) row; return !r.isNull(idx) && Double.compare(r.getDouble(idx), v) < 0; };
            case LT_EQ -> row -> { RowReader r = (RowReader) row; return !r.isNull(idx) && Double.compare(r.getDouble(idx), v) <= 0; };
            case GT -> row -> { RowReader r = (RowReader) row; return !r.isNull(idx) && Double.compare(r.getDouble(idx), v) > 0; };
            case GT_EQ -> row -> { RowReader r = (RowReader) row; return !r.isNull(idx) && Double.compare(r.getDouble(idx), v) >= 0; };
        };
    }

    private static RowMatcher indexedBooleanLeaf(int idx, Operator op, boolean v) {
        return switch (op) {
            case EQ -> row -> { RowReader r = (RowReader) row; return !r.isNull(idx) && r.getBoolean(idx) == v; };
            case NOT_EQ -> row -> { RowReader r = (RowReader) row; return !r.isNull(idx) && r.getBoolean(idx) != v; };
            default -> row -> { RowReader r = (RowReader) row; return !r.isNull(idx); };
        };
    }

    private static RowMatcher indexedIsNullLeaf(int idx) {
        return row -> ((RowReader) row).isNull(idx);
    }

    private static RowMatcher indexedIsNotNullLeaf(int idx) {
        return row -> !((RowReader) row).isNull(idx);
    }

    // ==================== Path resolution ====================

    private record NullFieldTarget(String[] path, String name) {
    }

    /// The field on the leaf's path whose presence a group null predicate tests, named by the
    /// definition level the predicate carries.
    ///
    /// The first node on the path to reach `definitionLevel` is the one to test. Later nodes may
    /// share that level — a required field does not raise it — but a required field is present
    /// exactly when its parent is, so all of them are null together and the first is the only one
    /// declared optional. Testing it asks about the field that can actually be null rather than
    /// about a required field below it, which is null only by way of the walk to it failing.
    private static NullFieldTarget nullFieldTarget(FileSchema schema, int columnIndex, int definitionLevel) {
        List<String> elements = schema.getColumn(columnIndex).fieldPath().elements();

        SchemaNode current = schema.getRootNode();
        int targetIndex = -1;

        for (int i = 0; i < elements.size() && targetIndex < 0; i++) {
            if (!(current instanceof SchemaNode.GroupNode group)) {
                break;
            }

            current = child(group, elements.get(i));
            if (current == null) {
                break;
            }

            if (current.maxDefinitionLevel() == definitionLevel) {
                targetIndex = i;
            }
            else if (current.maxDefinitionLevel() > definitionLevel) {
                break;
            }
        }

        if (targetIndex < 0) {
            throw new IllegalStateException(
                    "No field on the path of column " + columnIndex + " sits at definition level "
                            + definitionLevel + ", which a null predicate on it named");
        }

        String[] path = new String[targetIndex];

        for (int i = 0; i < targetIndex; i++) {
            path[i] = elements.get(i);
        }

        return new NullFieldTarget(path, elements.get(targetIndex));
    }

    private static SchemaNode child(SchemaNode.GroupNode group, String name) {
        for (SchemaNode child : group.children()) {
            if (child.name().equals(name)) {
                return child;
            }
        }

        return null;
    }

    /// Walks the row through the captured intermediate struct path.
    /// Returns null if any intermediate struct is null. For top-level
    /// columns `path` is empty and the row itself is returned.
    static StructAccessor resolve(StructAccessor row, String[] path) {
        StructAccessor current = row;
        for (int i = 0; i < path.length; i++) {
            String segment = path[i];
            if (current.isNull(segment)) {
                return null;
            }
            current = current.getStruct(segment);
        }
        return current;
    }

    static String[] pathSegments(FileSchema schema, int columnIndex) {
        List<String> elements = schema.getColumn(columnIndex).fieldPath().elements();
        if (elements.size() <= 1) {
            return EMPTY_PATH;
        }
        String[] out = new String[elements.size() - 1];
        for (int i = 0; i < out.length; i++) {
            out[i] = elements.get(i);
        }
        return out;
    }

    static String leafName(FileSchema schema, int columnIndex) {
        return schema.getColumn(columnIndex).fieldPath().leafName();
    }
}
