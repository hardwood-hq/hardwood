/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.schema;

import java.util.List;

/// Specifies which columns to read from a Parquet file.
///
/// Column projection allows reading only a subset of columns,
/// improving performance by skipping I/O, decoding, and memory allocation
/// for unneeded columns.
///
/// Usage examples:
/// ```java
/// // Read all columns (default)
/// ColumnProjection.all()
///
/// // Read specific columns
/// ColumnProjection.columns("id", "name", "address")
///
/// // For nested schemas, dot notation selects nested fields
/// ColumnProjection.columns("address.city")  // specific nested field
/// ColumnProjection.columns("address")       // parent group and all children
/// ```
public final class ColumnProjection {

    private static final ColumnProjection ALL = new ColumnProjection(null);

    private final List<String> projectedColumnNames;

    private ColumnProjection(List<String> projectedColumnNames) {
        this.projectedColumnNames = projectedColumnNames;
    }

    /// Returns a projection that includes all columns.
    public static ColumnProjection all() {
        return ALL;
    }

    /// Returns a projection that includes only the specified columns.
    ///
    /// Each name is a top-level field or the full dot-separated path to a nested field:
    ///
    /// - `"id"` - selects a top-level column
    /// - `"address"` - selects the parent group and all its children
    /// - `"address.city"` - selects only a specific nested field
    ///
    /// A nested field is reached by its full path only; `"city"` does not select `address.city`.
    /// Names may repeat or overlap, such as `"address"` and `"address.city"`. For the order in
    /// which readers expose the projected columns by index, see the column projection reference.
    ///
    /// @param names the column names to project
    /// @return a projection containing only the specified columns
    /// @throws IllegalArgumentException if no column names are provided, or a name is null or empty
    public static ColumnProjection columns(String... names) {
        if (names == null || names.length == 0) {
            throw new IllegalArgumentException("At least one column name must be specified");
        }
        for (String name : names) {
            if (name == null || name.isEmpty()) {
                throw new IllegalArgumentException("Column name cannot be null or empty");
            }
        }
        return new ColumnProjection(List.of(names));
    }

    /// Returns true if this projection includes all columns.
    public boolean projectsAll() {
        return projectedColumnNames == null;
    }

    /// Returns the column names to project, in the order they were requested and including
    /// repeats, or null if all columns are projected.
    public List<String> getProjectedColumnNames() {
        return projectedColumnNames;
    }
}
