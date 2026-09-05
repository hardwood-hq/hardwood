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
import dev.hardwood.schema.FileSchema;

/// Renders an execution predicate for diagnostics without exposing its literal values.
///
/// Column names and predicate structure are retained so events from separate reads can be
/// attributed to a query shape. Data-domain values are replaced with `?`; `IN` predicates
/// additionally retain their arity. Geospatial bounds describe the queried region rather than
/// values read from the file and are rendered in full.
public final class ResolvedPredicateRenderer {

    private ResolvedPredicateRenderer() {
    }

    /// Renders `predicate` using the unambiguous field paths from `schema`.
    public static String render(ResolvedPredicate predicate, FileSchema schema) {
        StringBuilder rendered = new StringBuilder();
        append(rendered, predicate, schema);
        return rendered.toString();
    }

    private static void append(StringBuilder rendered, ResolvedPredicate predicate, FileSchema schema) {
        switch (predicate) {
            case ResolvedPredicate.IntPredicate p -> appendComparison(rendered, p.op(), column(schema, p.columnIndex()));
            case ResolvedPredicate.LongPredicate p -> appendComparison(rendered, p.op(), column(schema, p.columnIndex()));
            case ResolvedPredicate.FloatPredicate p -> appendComparison(rendered, p.op(), column(schema, p.columnIndex()));
            case ResolvedPredicate.Float16Predicate p -> appendComparison(rendered, p.op(), column(schema, p.columnIndex()));
            case ResolvedPredicate.DoublePredicate p -> appendComparison(rendered, p.op(), column(schema, p.columnIndex()));
            case ResolvedPredicate.BooleanPredicate p -> appendComparison(rendered, p.op(), column(schema, p.columnIndex()));
            case ResolvedPredicate.BinaryPredicate p -> appendComparison(rendered, p.op(), column(schema, p.columnIndex()));
            case ResolvedPredicate.IntInPredicate p -> appendIn(rendered, column(schema, p.columnIndex()), p.values().length);
            case ResolvedPredicate.LongInPredicate p -> appendIn(rendered, column(schema, p.columnIndex()), p.values().length);
            case ResolvedPredicate.BinaryInPredicate p -> appendIn(rendered, column(schema, p.columnIndex()), p.values().length);
            case ResolvedPredicate.IsNullPredicate p -> appendUnary(rendered, "isNull", column(schema, p.columnIndex()));
            case ResolvedPredicate.IsNotNullPredicate p -> appendUnary(rendered, "isNotNull", column(schema, p.columnIndex()));
            case ResolvedPredicate.And p -> appendCompound(rendered, "and", p.children(), schema);
            case ResolvedPredicate.Or p -> appendCompound(rendered, "or", p.children(), schema);
            case ResolvedPredicate.GeospatialPredicate p -> appendGeospatial(rendered, p, schema);
        }
    }

    private static String column(FileSchema schema, int columnIndex) {
        return schema.getColumn(columnIndex).fieldPath().toString();
    }

    private static void appendComparison(StringBuilder rendered, FilterPredicate.Operator operator, String column) {
        rendered.append(operatorName(operator)).append('(').append(column).append(", ?)");
    }

    private static String operatorName(FilterPredicate.Operator operator) {
        return switch (operator) {
            case EQ -> "eq";
            case NOT_EQ -> "notEq";
            case LT -> "lt";
            case LT_EQ -> "ltEq";
            case GT -> "gt";
            case GT_EQ -> "gtEq";
        };
    }

    private static void appendIn(StringBuilder rendered, String column, int arity) {
        rendered.append("in(").append(column).append(", ?×").append(arity).append(')');
    }

    private static void appendUnary(StringBuilder rendered, String operator, String column) {
        rendered.append(operator).append('(').append(column).append(')');
    }

    private static void appendCompound(StringBuilder rendered, String operator,
                                       List<ResolvedPredicate> children, FileSchema schema) {
        rendered.append(operator).append('(');
        for (int i = 0; i < children.size(); i++) {
            if (i > 0) {
                rendered.append(", ");
            }
            append(rendered, children.get(i), schema);
        }
        rendered.append(')');
    }

    private static void appendGeospatial(StringBuilder rendered, ResolvedPredicate.GeospatialPredicate predicate,
                                         FileSchema schema) {
        rendered.append("intersects(")
                .append(column(schema, predicate.columnIndex())).append(", ")
                .append(predicate.xmin()).append(", ")
                .append(predicate.ymin()).append(", ")
                .append(predicate.xmax()).append(", ")
                .append(predicate.ymax()).append(')');
    }
}
