/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.dive.internal;

import dev.tamboui.tui.event.KeyCode;
import dev.tamboui.tui.event.KeyEvent;

/// Shared keyboard-event predicates for per-screen handlers.
public final class Keys {

    private Keys() {
    }

    /// `g` (no modifiers): jump to first visible row / page boundary.
    public static boolean isJumpTop(KeyEvent event) {
        return event.code() == KeyCode.CHAR
                && event.character() == 'g'
                && !event.hasCtrl()
                && !event.hasAlt();
    }

    /// `G` (shift+g): jump to last visible row / page boundary.
    public static boolean isJumpBottom(KeyEvent event) {
        return event.code() == KeyCode.CHAR
                && event.character() == 'G'
                && !event.hasCtrl()
                && !event.hasAlt();
    }

    /// PgDn or Shift+↓ — page-stride forward navigation. The Shift+↓ alias is
    /// the macOS-laptop chord since most don't have a dedicated PgDn key.
    public static boolean isPageDown(KeyEvent event) {
        return event.code() == KeyCode.PAGE_DOWN
                || (event.hasShift() && event.isDown());
    }

    /// PgUp or Shift+↑ — page-stride backward navigation. Shift+↑ alias as for
    /// `isPageDown`.
    public static boolean isPageUp(KeyEvent event) {
        return event.code() == KeyCode.PAGE_UP
                || (event.hasShift() && event.isUp());
    }

    /// Recommended stride for PgDn/PgUp on list-shaped screens.
    public static final int PAGE_STRIDE = 20;
}
