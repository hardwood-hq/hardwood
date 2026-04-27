/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.internal;

import java.util.Locale;

/// Ensures desired formatting
public final class Fmt {
    private Fmt() {}
    /// Wrapper for `String.format()` to ensure locale-independent formatting with `Locale.ROOT`
    public static String fmt(String pattern, Object... args) {
        return String.format(Locale.ROOT, pattern, args);
    }
}
