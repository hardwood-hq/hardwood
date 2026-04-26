/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.dive.internal;

/// Small string helpers shared by `dive` screens. Kept here so the
/// truncation/padding behavior — including the ellipsis character — stays
/// consistent across every screen that draws columnar content.
public final class Strings {

    /// The character used to mark visually-truncated content. Centralised
    /// here so changes propagate to every screen at once.
    public static final char ELLIPSIS = '…';

    private Strings() {
    }

    /// Pads `s` on the right with spaces to at least `width` columns. Strings
    /// already at or above `width` are returned unchanged (no truncation).
    public static String padRight(String s, int width) {
        if (s.length() >= width) {
            return s;
        }
        return s + " ".repeat(width - s.length());
    }

    /// Truncates `s` from the left so the suffix stays visible (e.g. for
    /// long column paths, where the trailing leaf name is the distinctive
    /// part). Strings within `maxWidth` are returned unchanged.
    public static String truncateLeft(String s, int maxWidth) {
        if (s.length() <= maxWidth) {
            return s;
        }
        return ELLIPSIS + s.substring(s.length() - maxWidth + 1);
    }
}
