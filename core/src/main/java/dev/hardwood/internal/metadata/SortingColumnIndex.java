/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.metadata;

/// One entry of a row group's `sorting_columns`, as the footer holds it: the sorting column by
/// its leaf column index rather than its path.
///
/// @param columnIndex the leaf column's index in the schema
/// @param descending whether the column sorts from greatest to least
/// @param nullsFirst whether the column's nulls sort before its values
public record SortingColumnIndex(int columnIndex, boolean descending, boolean nullsFirst) {
}
