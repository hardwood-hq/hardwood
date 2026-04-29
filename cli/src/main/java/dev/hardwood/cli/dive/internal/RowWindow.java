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
/// Scrolling is direction-aware via a persisted `scrollTop` (the absolute row
/// index of the first visible row): step / page navigation only adjusts
/// `scrollTop` enough to keep `selection` in view. So a `PgUp` lands the
/// cursor at the top of the new viewport, a `PgDn` past the bottom lands it
/// at the bottom, and motion that stays inside the existing viewport leaves
/// `scrollTop` alone (the cursor moves, the rows do not).
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

    /// Compute the visible window for a list of `total` rows, given the
    /// caller's current `scrollTop` and `selection`. The window starts at
    /// `scrollTop` clamped so `selection` is visible — if the caller fed in a
    /// stale `scrollTop` (e.g. the row count shrank, or `selection` jumped),
    /// the window slides to the closest position that keeps `selection` in
    /// view. `viewport` may be non-positive (very narrow terminals): callers
    /// get a single-row window in that case so the table still draws the
    /// cursor.
    public static RowWindow from(int scrollTop, int selection, int total, int viewport) {
        if (total <= 0) {
            return new RowWindow(0, 0, 0);
        }
        int v = Math.max(1, viewport);
        int sel = Math.min(Math.max(0, selection), total - 1);
        int top = Math.max(0, Math.min(scrollTop, Math.max(0, total - 1)));
        if (sel < top) {
            top = sel;
        }
        else if (sel >= top + v) {
            top = sel - v + 1;
        }
        if (top + v > total) {
            top = Math.max(0, total - v);
        }
        int end = Math.min(total, top + v);
        return new RowWindow(top, end, sel - top);
    }

    /// Update a stored `scrollTop` so that `selection` is visible inside a
    /// window of `viewport` rows, scrolling as little as possible. Callers
    /// thread the result through their state record so the next frame's
    /// [#from] keeps the same viewport when the selection stays in range.
    public static int adjustTop(int prevTop, int selection, int viewport) {
        int v = Math.max(1, viewport);
        int sel = Math.max(0, selection);
        int top = Math.max(0, prevTop);
        if (sel < top) {
            return sel;
        }
        if (sel >= top + v) {
            return Math.max(0, sel - v + 1);
        }
        return top;
    }
}
