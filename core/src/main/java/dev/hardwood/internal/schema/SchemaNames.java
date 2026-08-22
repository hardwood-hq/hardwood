/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.schema;

/// Mapping of arbitrary Parquet names onto the name grammars used by Hardwood's
/// schema converters and emitters.
///
/// Avro names and proto identifiers are both `[A-Za-z_][A-Za-z0-9_]*`, while Parquet
/// permits any UTF-8 string. A schema carrying a name outside that grammar is rejected
/// by every parser of either format, so names are rewritten to fit.
public final class SchemaNames {

    private SchemaNames() {
    }

    /// Rewrites `name` into a legal schema name: every character outside
    /// `[A-Za-z0-9_]` becomes `_`, and a leading `_` is prepended when the result
    /// would otherwise start with a digit. An empty name maps to `_`.
    ///
    /// The mapping is not injective — two distinct Parquet names can collide, so
    /// callers emitting several names into the same scope must disambiguate the
    /// results themselves.
    ///
    /// @throws NullPointerException if `name` is `null`
    public static String sanitize(String name) {
        if (name.isEmpty()) {
            return "_";
        }

        int length = name.length();
        StringBuilder sb = new StringBuilder(length + 1);
        // Every illegal character maps to '_', which is itself a legal first character,
        // so only a leading digit still needs the prefix.
        if (isDigit(name.charAt(0))) {
            sb.append('_');
        }
        for (int i = 0; i < length; i++) {
            char c = name.charAt(i);
            sb.append(isNamePart(c) ? c : '_');
        }
        return sb.toString();
    }

    /// Returns whether `name` matches `[A-Za-z_][A-Za-z0-9_]*`.
    public static boolean isLegal(String name) {
        if (name.isEmpty() || isDigit(name.charAt(0))) {
            return false;
        }
        for (int i = 0; i < name.length(); i++) {
            if (!isNamePart(name.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean isNamePart(char c) {
        return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_' || isDigit(c);
    }

    private static boolean isDigit(char c) {
        return c >= '0' && c <= '9';
    }
}
