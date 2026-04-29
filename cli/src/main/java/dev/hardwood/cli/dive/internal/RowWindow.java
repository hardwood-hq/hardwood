/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.dive.internal;

/// Visible row window for a list-shaped dive screen. List screens that build
/// a `Row` per item must build only the visible slice each frame, otherwise
/// per-keystroke navigation is O(N) in formatting + Row/Cell allocation —
/// which is invisible on small files but becomes the dominant cost on
/// dictionaries with hundreds of thousands of entries, page lists with
/// thousands of pages, or wide-schema files.
///
/// The window is bottom-pinned on the selection: when the cursor sits at or
/// past the bottom of the viewport, the window scrolls so the cursor stays
/// on the last row. This mirrors what `TableState.scrollToSelected` produces
/// when the state is constructed fresh each frame, so swapping in the slice
/// gives the same on-screen result.
///
/// `selectionInWindow` is the selection's row index relative to the slice
/// (i.e. `selection - start`), suitable for `TableState.select(...)` after
/// the slice is handed to the Table widget.
public record RowWindow(int start, int end, int selectionInWindow) {

    public int size() {
        return end - start;
    }

    public boolean isEmpty() {
        return start >= end;
    }

    /// Compute the visible window for a list of `total` rows, bottom-pinned
    /// on `selection`, given the viewport's row capacity. `viewport` may be
    /// non-positive (very narrow terminals): callers get a single-row
    /// window in that case so the table still draws the cursor.
    public static RowWindow bottomPinned(int selection, int total, int viewport) {
        if (total <= 0) {
            return new RowWindow(0, 0, 0);
        }
        int v = Math.max(1, viewport);
        int sel = Math.min(Math.max(0, selection), total - 1);
        int start = sel >= v ? sel - v + 1 : 0;
        int end = Math.min(total, start + v);
        return new RowWindow(start, end, sel - start);
    }
}
