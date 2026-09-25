/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.dive.internal;

import dev.tamboui.tui.event.KeyEvent;

/// Navigation for a pane whose content is a list of rows: every list screen,
/// every menu, and the record modal. A cursor selects one row and the
/// viewport follows it, so the three strides all move the cursor — `↑`/`↓`
/// by a row, `PgUp`/`PgDn` by a viewport, `g`/`G` to the ends.
///
/// The cursor stops on every row, including rows `Enter` cannot act on. A
/// row that is not actionable is still worth reading, and skipping it puts
/// content out of reach of the only keys that reach content — which is what
/// the marker is for: see [#marker(boolean,boolean,boolean)].
///
/// Screens hold the selection in their [dev.hardwood.cli.dive.ScreenState]
/// and pass it back on every keypress; nothing is retained between calls.
/// Turning a new selection into a scroll offset is the screen's own job, via
/// [RowWindow#adjustTop(int,int,int)], because only the screen knows which
/// state record to rebuild.
public final class CursorPane {

    /// Returned by [#select(KeyEvent,int,int)] when the event does not move
    /// the cursor, so the caller can let it fall through to the rest of its
    /// handler.
    public static final int UNHANDLED = -1;

    private CursorPane() {
    }

    /// The row to select after `event`, clamped to `count`, or [#UNHANDLED]
    /// if `event` does not move the cursor. A navigation key on a cursor
    /// already against its end returns the unchanged selection: it is
    /// handled, it simply has nowhere to go.
    ///
    /// An empty list has nothing to select, so every key is [#UNHANDLED]
    /// there and screens do not each need to guard on the count.
    public static int select(KeyEvent event, int selection, int count) {
        if (count <= 0) {
            return UNHANDLED;
        }
        int current = Math.max(0, Math.min(selection, count - 1));
        int stride = Keys.viewportStride();
        if (Keys.isStepUp(event)) {
            return Math.max(0, current - 1);
        }
        if (Keys.isStepDown(event)) {
            return Math.min(count - 1, current + 1);
        }
        if (Keys.isPageUp(event)) {
            return Math.max(0, current - stride);
        }
        if (Keys.isPageDown(event)) {
            return Math.min(count - 1, current + stride);
        }
        if (Keys.isJumpTop(event)) {
            return 0;
        }
        if (Keys.isJumpBottom(event)) {
            return count - 1;
        }
        return UNHANDLED;
    }

    /// The two cells a row reserves for its actionability marker.
    ///
    /// `▶` marks what `Enter` can act on; the cursor is marked by colour, not
    /// by the glyph. It is drawn on the cursor row whenever that row is
    /// actionable, and on other rows only when `mixed` — when the pane holds
    /// some row `Enter` cannot act on, and the marker therefore distinguishes
    /// one row from another. A pane whose rows are uniformly actionable shows
    /// a single marker travelling with the cursor rather than a column of
    /// identical ones.
    ///
    /// A pane whose rows are uniformly actionable can leave this to the
    /// table's own highlight symbol, which draws on the cursor row already.
    public static String marker(boolean actionable, boolean cursor, boolean mixed) {
        return actionable && (cursor || mixed) ? MARKER : NO_MARKER;
    }

    /// The marker glyph and the blank that keeps unmarked rows aligned with
    /// marked ones.
    public static final String MARKER = "▶ ";

    public static final String NO_MARKER = "  ";

    /// The keybar fragment for a list of `count` rows: the movement keys that
    /// have somewhere to go. Screens append their own `Enter` and per-screen
    /// bindings around it so the wording of the movement keys is stated in
    /// one place.
    public static String hints(int count) {
        return new Keys.Hints()
                .add(count > 1, "[↑↓] move")
                .add(count > Keys.viewportStride(), "[PgDn/PgUp or Shift+↓↑] page")
                .add(count > 1, "[g/G] first/last")
                .build();
    }
}
