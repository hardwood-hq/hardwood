/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.metadata;

import java.util.List;

/// Path from the root schema to a leaf column, represented as a list of field names.
///
/// For flat schemas, the path has a single element (the column name).
/// For nested schemas, it contains each intermediate group name
/// (e.g. `["address", "zip"]` for a `zip` field inside an `address` struct).
///
/// @param elements the path components from root to leaf
public record FieldPath(List<String> elements) {

    /// Creates a FieldPath from individual path components.
    public static FieldPath of(String... elements) {
        return new FieldPath(List.of(elements));
    }

    /// Returns the leaf (last) element of the path.
    public String leafName() {
        return elements.getLast();
    }

    /// Returns the first (top-level) element of the path.
    public String topLevelName() {
        return elements.getFirst();
    }

    /// Returns true if this path matches the given dotted name.
    ///
    /// A dotted name like `"address.zip"` matches the path `["address", "zip"]`.
    /// A prefix match is allowed: `"address"` matches `["address", "zip"]`
    /// (useful for filtering on a top-level field that contains nested columns).
    ///
    /// @param dottedName a dot-separated field reference (e.g. `"address.zip"`)
    /// @return true if this path matches the dotted name
    public boolean matchesDottedName(String dottedName) {
        int pathIndex = 0;
        int nameStart = 0;
        while (nameStart < dottedName.length() && pathIndex < elements.size()) {
            int dot = dottedName.indexOf('.', nameStart);
            String segment = dot < 0 ? dottedName.substring(nameStart) : dottedName.substring(nameStart, dot);
            if (!segment.equals(elements.get(pathIndex))) {
                return false;
            }
            pathIndex++;
            nameStart = dot < 0 ? dottedName.length() : dot + 1;
        }
        return nameStart >= dottedName.length() && pathIndex <= elements.size();
    }

    /// Returns the dot-separated string representation of this path.
    @Override
    public String toString() {
        return String.join(".", elements);
    }

    /// Whether this path is empty (has no elements) or not.
    public boolean isEmpty() {
        return elements.isEmpty();
    }
}
