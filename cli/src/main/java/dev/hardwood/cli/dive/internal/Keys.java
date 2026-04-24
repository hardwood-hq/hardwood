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
}
