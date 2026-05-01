/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.util.Arrays;
import java.util.List;
import java.util.function.IntUnaryOperator;

import dev.hardwood.reader.FilterPredicate.Operator;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.StructAccessor;
import dev.hardwood.schema.FileSchema;

/// Compiles a [ResolvedPredicate] into a [RowMatcher] tree once per reader.
///
/// All field-name lookups, struct-path resolutions, and operator decisions
/// are performed at compile time. The returned matcher only reads values
/// and runs comparisons per row, eliminating the type and operator
/// switches that [RecordFilterEvaluator] performs for every row.
public final class RecordFilterCompiler {

    private static final String[] EMPTY_PATH = new String[0];
    private static final int IN_LIST_BINARY_SEARCH_THRESHOLD = 16;

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
            case ResolvedPredicate.FloatPredicate p -> {
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
            case ResolvedPredicate.BinaryInPredicate p ->
                    binaryInLeaf(pathSegments(schema, p.columnIndex()), leafName(schema, p.columnIndex()), p.values());
            case ResolvedPredicate.IsNullPredicate p -> {
                int idx = indexedTopLevel(schema, p.columnIndex(), topLevelFieldIndex);
                yield idx >= 0
                        ? indexedIsNullLeaf(idx)
                        : isNullLeaf(pathSegments(schema, p.columnIndex()), leafName(schema, p.columnIndex()));
            }
            case ResolvedPredicate.IsNotNullPredicate p -> {
                int idx = indexedTopLevel(schema, p.columnIndex(), topLevelFieldIndex);
                yield idx >= 0
                        ? indexedIsNotNullLeaf(idx)
                        : isNotNullLeaf(pathSegments(schema, p.columnIndex()), leafName(schema, p.columnIndex()));
            }
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
    private static int indexedTopLevel(FileSchema schema, int columnIndex,
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
        if (compiled.length == 1) {
            return compiled[0];
        }
        return new AndNMatcher(compiled);
    }

    private static RowMatcher compileOr(List<ResolvedPredicate> children, FileSchema schema,
            IntUnaryOperator topLevelFieldIndex) {
        RowMatcher[] compiled = compileAll(children, schema, topLevelFieldIndex);
        if (compiled.length == 1) {
            return compiled[0];
        }
        return new OrNMatcher(compiled);
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

    // Float and Double use Float.compare / Double.compare to match the legacy
    // RecordFilterEvaluator semantics for NaN ordering.

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

    private static int compareBinary(byte[] left, byte[] right, boolean signed) {
        return signed
                ? BinaryComparator.compareSigned(left, right)
                : BinaryComparator.compareUnsigned(left, right);
    }

    private static RowMatcher intInLeaf(String[] path, String name, int[] values) {
        int[] sorted = values.clone();
        Arrays.sort(sorted);
        if (sorted.length >= IN_LIST_BINARY_SEARCH_THRESHOLD) {
            return row -> {
                StructAccessor a = resolve(row, path);
                if (a == null || a.isNull(name)) return false;
                return Arrays.binarySearch(sorted, a.getInt(name)) >= 0;
            };
        }
        return row -> {
            StructAccessor a = resolve(row, path);
            if (a == null || a.isNull(name)) return false;
            int val = a.getInt(name);
            for (int i = 0; i < sorted.length; i++) {
                if (sorted[i] == val) return true;
            }
            return false;
        };
    }

    private static RowMatcher longInLeaf(String[] path, String name, long[] values) {
        long[] sorted = values.clone();
        Arrays.sort(sorted);
        if (sorted.length >= IN_LIST_BINARY_SEARCH_THRESHOLD) {
            return row -> {
                StructAccessor a = resolve(row, path);
                if (a == null || a.isNull(name)) return false;
                return Arrays.binarySearch(sorted, a.getLong(name)) >= 0;
            };
        }
        return row -> {
            StructAccessor a = resolve(row, path);
            if (a == null || a.isNull(name)) return false;
            long val = a.getLong(name);
            for (int i = 0; i < sorted.length; i++) {
                if (sorted[i] == val) return true;
            }
            return false;
        };
    }

    private static RowMatcher binaryInLeaf(String[] path, String name, byte[][] values) {
        byte[][] copy = values.clone();
        return row -> {
            StructAccessor a = resolve(row, path);
            if (a == null || a.isNull(name)) return false;
            byte[] val = a.getBinary(name);
            for (int i = 0; i < copy.length; i++) {
                if (Arrays.equals(val, copy[i])) return true;
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
    // on a top-level column. The cast is safe by construction — only
    // [dev.hardwood.internal.reader.FilteredRowReader] invokes the matcher,
    // and it always passes a [RowReader] delegate. The compiler emits these
    // leaves only when the caller passes a `topLevelFieldIndex` callback,
    // which today is done by both [dev.hardwood.internal.reader.FlatRowReader]
    // and [dev.hardwood.internal.reader.NestedRowReader].

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

    /// Walks the row through the captured intermediate struct path.
    /// Returns null if any intermediate struct is null. For top-level
    /// columns `path` is empty and the row itself is returned.
    private static StructAccessor resolve(StructAccessor row, String[] path) {
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

    private static String[] pathSegments(FileSchema schema, int columnIndex) {
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

    private static String leafName(FileSchema schema, int columnIndex) {
        return schema.getColumn(columnIndex).fieldPath().leafName();
    }
}
