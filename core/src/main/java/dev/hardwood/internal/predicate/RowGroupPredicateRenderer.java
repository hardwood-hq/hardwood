/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.util.List;

import dev.hardwood.reader.RowGroupPredicate;

/// Renders row-group predicates for diagnostics.
///
/// Byte offsets are retained because they describe split layout rather than values from the
/// data domain and are needed to diagnose uneven split assignment.
public final class RowGroupPredicateRenderer {

    private RowGroupPredicateRenderer() {
    }

    public static String render(RowGroupPredicate predicate) {
        StringBuilder rendered = new StringBuilder();
        append(rendered, predicate);
        return rendered.toString();
    }

    private static void append(StringBuilder rendered, RowGroupPredicate predicate) {
        switch (predicate) {
            case RowGroupPredicate.ByteRange p -> rendered.append("byteRange(")
                    .append(p.startInclusive()).append(", ")
                    .append(p.endExclusive()).append(')');
            case RowGroupPredicate.And p -> appendAnd(rendered, p.children());
        }
    }

    private static void appendAnd(StringBuilder rendered, List<RowGroupPredicate> children) {
        rendered.append("and(");
        for (int i = 0; i < children.size(); i++) {
            if (i > 0) {
                rendered.append(", ");
            }
            append(rendered, children.get(i));
        }
        rendered.append(')');
    }
}
