/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.util.List;

/// A predicate over row groups, used to select which row groups a reader scans.
///
/// Distinct from [FilterPredicate], which expresses constraints on **column values** and is
/// checked against per-column statistics. A `RowGroupPredicate` expresses constraints on the
/// **row group itself** (its byte position in the file, its ordinal index, the row range it
/// spans). The two are sibling, AND-combined inputs to a reader: a row group is read if and only if it
/// passes both.
///
/// Granularity: filtering happens at row-group resolution, not row resolution. A row group
/// passes [#byteRange(long, long)] when its midpoint falls in the given byte range — every
/// row in that row group is then read, even rows whose data extends outside the range. This
/// is the standard Hadoop-input-format split convention.
///
/// Usage:
/// ```java
/// // Single split: read row groups whose midpoint is in [start, end).
/// ColumnReader r = file.buildColumnReader("price")
///         .filter(RowGroupPredicate.byteRange(splitStart, splitEnd))
///         .build();
///
/// // Stacks with column-stats predicates — both apply, AND-combined.
/// ColumnReader r = file.buildColumnReader("price")
///         .filter(FilterPredicate.gt("price", 100))
///         .filter(RowGroupPredicate.byteRange(splitStart, splitEnd))
///         .build();
/// ```
public sealed interface RowGroupPredicate
        permits RowGroupPredicate.ByteRange,
                RowGroupPredicate.And {

    /// Keep row groups whose data midpoint — start of the first column chunk plus half of
    /// the on-disk compressed size — falls in `[startInclusive, endExclusive)`.
    ///
    /// This is the standard split convention: every row group lands in exactly one byte
    /// range across a partitioning of the file, regardless of where the boundary falls
    /// inside it.
    ///
    /// `endExclusive < startInclusive` is treated as an empty range. This matches callers
    /// that pass `splitStart + splitLength` and tolerate long overflow on tail splits.
    static RowGroupPredicate byteRange(long startInclusive, long endExclusive) {
        return new ByteRange(startInclusive, endExclusive);
    }

    /// Keep row groups that match every child predicate (intersection).
    static RowGroupPredicate and(RowGroupPredicate... children) {
        if (children == null || children.length == 0) {
            throw new IllegalArgumentException("AND predicate requires at least one child");
        }
        return new And(List.of(children));
    }

    /// Keep row groups whose midpoint falls in `[startInclusive, endExclusive)`.
    /// Constructed via [#byteRange(long, long)].
    record ByteRange(long startInclusive, long endExclusive) implements RowGroupPredicate {
    }

    /// Conjunction of row-group predicates — a row group passes if and only if every child passes.
    /// Constructed via [#and(RowGroupPredicate...)].
    record And(List<RowGroupPredicate> children) implements RowGroupPredicate {
    }
}
