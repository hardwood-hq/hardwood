/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.parquet.hadoop.metadata;

/// Minimal shim for parquet-java's `ColumnPath`.
///
/// Stores a dot-delimited column path string. Used by the filter API's
/// `Column` types to identify which column a predicate applies to.
public final class ColumnPath {

    private final String[] parts;

    private ColumnPath(String[] parts) {
        this.parts = parts;
    }

    /// Create a ColumnPath from dot-separated string (e.g. "address.city").
    ///
    /// @param path the dot-delimited path
    /// @return the column path
    public static ColumnPath fromDotString(String path) {
        return new ColumnPath(path.split("\\."));
    }

    /// Create a ColumnPath from path components.
    ///
    /// @param parts the path components
    /// @return the column path
    public static ColumnPath get(String... parts) {
        return new ColumnPath(parts.clone());
    }

    /// Get the dot-delimited string representation.
    ///
    /// @return the path as a dot-separated string
    public String toDotString() {
        return String.join(".", parts);
    }

    @Override
    public String toString() {
        return toDotString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof ColumnPath other))
            return false;
        return java.util.Arrays.equals(parts, other.parts);
    }

    @Override
    public int hashCode() {
        return java.util.Arrays.hashCode(parts);
    }
}
