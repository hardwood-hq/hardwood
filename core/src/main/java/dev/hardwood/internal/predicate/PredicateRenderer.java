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
import dev.hardwood.reader.RowGroupPredicate;
import dev.hardwood.schema.FileSchema;

/// Renders a predicate as a structural string with every literal elided, for the
/// `predicate` field of the Filter JFR events (see #1053).
///
/// A comparison renders as its operator over the column name with the literal
/// replaced by `?` — `gt(id, ?)`, `eq(label, ?)` — and a membership test renders
/// its arity — `in(id, ?×5)` — never its values. `IsNull` / `IsNotNull` and the
/// geospatial predicate carry no literal to elide, so they render in full.
///
/// Literals are elided for two reasons, both pointing the same way. Sensitivity:
/// predicate literals would be the first data-domain values in a Hardwood
/// recording, which gets attached to bug reports and uploaded to profiling
/// services. Cardinality: with literals the distinct-string count is the number
/// of query parameterizations — unbounded in a service — while without them it is
/// the number of distinct query shapes, which is small and stable.
///
/// The switch is exhaustive over the sealed [ResolvedPredicate] on purpose: adding
/// a predicate type fails to compile here instead of silently rendering it wrong.
/// It renders from the records directly, never from their generated `toString()`,
/// which leaks every literal by construction — and renders `byte[]` values as
/// array identities, which is useless exactly where it matters.
///
/// Column names come from the read's reference schema, as dot-separated field
/// paths: the names are path-stable across the files of a multi-file read even
/// where their leaf ordinals differ, so one rendering covers every event of the
/// read. Callers render lazily, behind the event's `isEnabled()`, and cache the
/// string for the read.
public final class PredicateRenderer {

    private PredicateRenderer() {
    }

    /// Renders `predicate` with column names from `schema`, literals elided.
    public static String render(ResolvedPredicate predicate, FileSchema schema) {
        StringBuilder out = new StringBuilder();
        render(predicate, schema, out);
        return out.toString();
    }

    /// Renders a row-group predicate in full: its byte ranges are file offsets,
    /// neither user data nor schema, so there is nothing to elide.
    public static String render(RowGroupPredicate predicate) {
        StringBuilder out = new StringBuilder();
        render(predicate, out);
        return out.toString();
    }

    private static void render(ResolvedPredicate predicate, FileSchema schema, StringBuilder out) {
        switch (predicate) {
            case ResolvedPredicate.IntPredicate leaf -> comparison(leaf.columnIndex(), leaf.op(), schema, out);
            case ResolvedPredicate.LongPredicate leaf -> comparison(leaf.columnIndex(), leaf.op(), schema, out);
            case ResolvedPredicate.UnsignedIntPredicate leaf -> comparison(leaf.columnIndex(), leaf.op(), schema, out);
            case ResolvedPredicate.UnsignedLongPredicate leaf -> comparison(leaf.columnIndex(), leaf.op(), schema, out);
            case ResolvedPredicate.FloatPredicate leaf -> comparison(leaf.columnIndex(), leaf.op(), schema, out);
            case ResolvedPredicate.Float16Predicate leaf -> comparison(leaf.columnIndex(), leaf.op(), schema, out);
            case ResolvedPredicate.DoublePredicate leaf -> comparison(leaf.columnIndex(), leaf.op(), schema, out);
            case ResolvedPredicate.BooleanPredicate leaf -> comparison(leaf.columnIndex(), leaf.op(), schema, out);
            case ResolvedPredicate.BinaryPredicate leaf -> comparison(leaf.columnIndex(), leaf.op(), schema, out);
            case ResolvedPredicate.IntInPredicate leaf -> in(leaf.columnIndex(), leaf.values().length, schema, out);
            case ResolvedPredicate.LongInPredicate leaf -> in(leaf.columnIndex(), leaf.values().length, schema, out);
            case ResolvedPredicate.UnsignedIntInPredicate leaf -> in(leaf.columnIndex(), leaf.values().length,
                    schema, out);
            case ResolvedPredicate.UnsignedLongInPredicate leaf -> in(leaf.columnIndex(), leaf.values().length,
                    schema, out);
            case ResolvedPredicate.BinaryInPredicate leaf -> in(leaf.columnIndex(), leaf.values().length, schema, out);
            case ResolvedPredicate.FloatInPredicate leaf -> in(leaf.columnIndex(), leaf.values().length, schema, out);
            case ResolvedPredicate.Float16InPredicate leaf -> in(leaf.columnIndex(), leaf.values().length, schema, out);
            case ResolvedPredicate.DoubleInPredicate leaf -> in(leaf.columnIndex(), leaf.values().length, schema, out);
            case ResolvedPredicate.IsNullPredicate leaf -> {
                out.append("isNull(").append(name(leaf.columnIndex(), schema)).append(')');
            }
            case ResolvedPredicate.IsNotNullPredicate leaf -> {
                out.append("isNotNull(").append(name(leaf.columnIndex(), schema)).append(')');
            }
            case ResolvedPredicate.EveryNonNullRowPredicate leaf -> {
                out.append("everyNonNullRow(").append(name(leaf.columnIndex(), schema)).append(')');
            }
            case ResolvedPredicate.NoRowPredicate leaf -> {
                out.append("noRow(").append(name(leaf.columnIndex(), schema)).append(')');
            }
            case ResolvedPredicate.GeospatialPredicate leaf -> {
                out.append("intersects(").append(name(leaf.columnIndex(), schema))
                        .append(", ").append(leaf.xmin())
                        .append(", ").append(leaf.ymin())
                        .append(", ").append(leaf.xmax())
                        .append(", ").append(leaf.ymax())
                        .append(')');
            }
            case ResolvedPredicate.And and -> compound("and", and.children(), schema, out);
            case ResolvedPredicate.Or or -> compound("or", or.children(), schema, out);
        }
    }

    private static void render(RowGroupPredicate predicate, StringBuilder out) {
        switch (predicate) {
            case RowGroupPredicate.ByteRange range -> out.append("byteRange(")
                    .append(range.startInclusive()).append(", ").append(range.endExclusive()).append(')');
            case RowGroupPredicate.And and -> {
                out.append("and(");
                for (int i = 0; i < and.children().size(); i++) {
                    if (i > 0) {
                        out.append(", ");
                    }
                    render(and.children().get(i), out);
                }
                out.append(')');
            }
        }
    }

    private static void comparison(int columnIndex, FilterPredicate.Operator op, FileSchema schema,
                                   StringBuilder out) {
        out.append(operator(op)).append('(').append(name(columnIndex, schema)).append(", ?)");
    }

    private static void in(int columnIndex, int arity, FileSchema schema, StringBuilder out) {
        out.append("in(").append(name(columnIndex, schema)).append(", ?×").append(arity).append(')');
    }

    private static void compound(String name, List<ResolvedPredicate> children, FileSchema schema,
                                 StringBuilder out) {
        out.append(name).append('(');
        for (int i = 0; i < children.size(); i++) {
            if (i > 0) {
                out.append(", ");
            }
            render(children.get(i), schema, out);
        }
        out.append(')');
    }

    private static String name(int columnIndex, FileSchema schema) {
        return schema.getColumn(columnIndex).fieldPath().toString();
    }

    private static String operator(FilterPredicate.Operator op) {
        return switch (op) {
            case EQ -> "eq";
            case NOT_EQ -> "notEq";
            case LT -> "lt";
            case LT_EQ -> "ltEq";
            case GT -> "gt";
            case GT_EQ -> "gtEq";
        };
    }
}
