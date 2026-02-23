/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.schema;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Specifies which columns to read from a Parquet file.
 *
 * <p>Column projection allows reading only a subset of columns,
 * improving performance by skipping I/O, decoding, and memory allocation
 * for unneeded columns.</p>
 *
 * <p>Usage examples:</p>
 * <pre>{@code
 * // Read all columns (default)
 * ColumnProjection.all()
 *
 * // Read specific columns
 * ColumnProjection.columns("id", "name", "address")
 *
 * // For nested schemas, dot notation selects nested fields
 * ColumnProjection.columns("address.city")  // specific nested field
 * ColumnProjection.columns("address")       // parent group and all children
 * }</pre>
 */
public final class ColumnProjection {

    private static final ColumnProjection ALL = new ColumnProjection(null);

    private final Set<String> projectedColumnNames;

    private ColumnProjection(Set<String> projectedColumnNames) {
        this.projectedColumnNames = projectedColumnNames;
    }

    /**
     * Returns a projection that includes all columns.
     */
    public static ColumnProjection all() {
        return ALL;
    }

    /**
     * Returns a projection that includes only the specified columns.
     *
     * <p>For flat schemas, use simple column names. For nested schemas:</p>
     * <ul>
     *   <li>{@code "address"} - selects the parent group and all its children</li>
     *   <li>{@code "address.city"} - selects only a specific nested field</li>
     * </ul>
     *
     * @param names the column names to project
     * @return a projection containing only the specified columns
     * @throws IllegalArgumentException if no column names are provided
     */
    public static ColumnProjection columns(String... names) {
        if (names == null || names.length == 0) {
            throw new IllegalArgumentException("At least one column name must be specified");
        }
        Set<String> nameSet = new LinkedHashSet<>();
        for (String name : names) {
            if (name == null || name.isEmpty()) {
                throw new IllegalArgumentException("Column name cannot be null or empty");
            }
            nameSet.add(name);
        }
        return new ColumnProjection(Collections.unmodifiableSet(nameSet));
    }

    /**
     * Returns true if this projection includes all columns.
     */
    public boolean projectsAll() {
        return projectedColumnNames == null;
    }

    /**
     * Returns the set of column names to project, or null if all columns are projected.
     */
    public Set<String> getProjectedColumnNames() {
        return projectedColumnNames;
    }
}
