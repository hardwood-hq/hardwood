/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.dive.internal;

import dev.hardwood.cli.internal.Fmt;

/// Renders `count + noun` strings consistently across the `dive` TUI: picks
/// singular vs plural form based on the count, and formats the number with
/// the locale-independent grouping separator (comma). Handles irregular
/// plurals ("entry / entries", "leaf / leaves") by requiring both forms from
/// the caller. Zero takes the plural form (standard English convention).
public final class Plurals {

    private Plurals() {
    }

    public static String format(long count, String singular, String plural) {
        return Fmt.fmt("%,d", count) + " " + (count == 1 ? singular : plural);
    }

    /// Renders an approximate "X-Y of Z" range string for a list-shaped
    /// screen that paints into a bounded viewport. Mirrors how Data
    /// preview shows "rows N-M of total" so other list screens can do
    /// the same. The visible window is bottom-pinned to the cursor:
    /// `end = max(viewport, selection + 1)`; `start = end - viewport + 1`.
    /// This matches what tamboui's TableState does on first
    /// scroll-past-viewport (selection ends up at the bottom row), so
    /// for the common downward-navigation case the displayed range
    /// matches what's actually visible.
    public static String rangeOf(int selection, int total, int viewport) {
        if (total <= 0) {
            return "0";
        }
        int v = Math.max(1, viewport);
        if (total <= v) {
            return total == 1 ? "1 of 1"
                    : "1-" + Fmt.fmt("%,d", total) + " of " + Fmt.fmt("%,d", total);
        }
        int sel = Math.max(0, Math.min(selection, total - 1));
        int end = Math.min(total, Math.max(v, sel + 1));
        int start = Math.max(1, end - v + 1);
        return Fmt.fmt("%,d-%,d of %,d", start, end, total);
    }
}
