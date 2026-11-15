/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.metadata;

/// One column of the order a row group's rows are sorted in: the column, its direction, and
/// where its nulls go.
///
/// A row group's sorting columns order its rows lexicographically: by the first column, rows
/// equal there by the second, and so on. A column's values compare in its column order, the
/// order its statistics use: signed or unsigned by its annotation, a `DECIMAL` by the number it
/// holds, a `STRING` byte by byte. A floating-point `NaN` sorts after every number, and `-0.0`
/// equals `0.0`.
///
/// Build one through [#ascending] or [#descending].
///
/// @param columnPath the leaf column's dotted path as the schema spells it, synthetic
///        `list.element` and `key_value.key` segments included
/// @param descending whether the column sorts from greatest to least
/// @param nullOrder where the column's nulls sort
/// @see <a href="https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift">parquet.thrift</a>
public record SortingColumn(String columnPath, boolean descending, NullOrder nullOrder) {

    /// Where a sorting column's nulls go relative to its values.
    public enum NullOrder {
        /// Nulls sort before every value.
        NULLS_FIRST,
        /// Nulls sort after every value.
        NULLS_LAST
    }

    /// @throws IllegalArgumentException if `columnPath` or `nullOrder` is `null`
    public SortingColumn {
        if (columnPath == null) {
            throw new IllegalArgumentException("columnPath must not be null");
        }
        if (nullOrder == null) {
            throw new IllegalArgumentException("nullOrder must not be null for sorting column " + columnPath);
        }
    }

    /// A column sorted from least to greatest.
    ///
    /// @param columnPath the leaf column's dotted path
    /// @param nullOrder where the column's nulls sort
    /// @return the sorting column
    /// @throws IllegalArgumentException if either argument is `null`
    public static SortingColumn ascending(String columnPath, NullOrder nullOrder) {
        return new SortingColumn(columnPath, false, nullOrder);
    }

    /// A column sorted from greatest to least.
    ///
    /// @param columnPath the leaf column's dotted path
    /// @param nullOrder where the column's nulls sort
    /// @return the sorting column
    /// @throws IllegalArgumentException if either argument is `null`
    public static SortingColumn descending(String columnPath, NullOrder nullOrder) {
        return new SortingColumn(columnPath, true, nullOrder);
    }

    /// Whether the column's nulls sort before its values.
    public boolean nullsFirst() {
        return nullOrder == NullOrder.NULLS_FIRST;
    }

    /// The column as SQL spells a sort key, such as `pickup_ts ASC NULLS FIRST`.
    @Override
    public String toString() {
        return columnPath + (descending ? " DESC" : " ASC")
                + (nullsFirst() ? " NULLS FIRST" : " NULLS LAST");
    }
}
