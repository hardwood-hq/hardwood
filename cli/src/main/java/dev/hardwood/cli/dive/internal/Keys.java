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
                || (event.hasShift() && event.code() == KeyCode.DOWN);
    }

    /// PgUp or Shift+↑ — page-stride backward navigation. Shift+↑ alias as for
    /// `isPageDown`.
    public static boolean isPageUp(KeyEvent event) {
        return event.code() == KeyCode.PAGE_UP
                || (event.hasShift() && event.code() == KeyCode.UP);
    }

    /// Single-step `↓` without Shift — distinct from `event.isDown()` which
    /// also matches `Shift+↓` because tamboui's standard moveDown binding
    /// doesn't require the Shift modifier to be off. Use this in screens
    /// that want plain ↓ to mean "single step" and reserve `Shift+↓` for
    /// page navigation.
    public static boolean isStepDown(KeyEvent event) {
        return event.isDown() && !event.hasShift();
    }

    /// Single-step `↑` without Shift — see [#isStepDown(KeyEvent)].
    public static boolean isStepUp(KeyEvent event) {
        return event.isUp() && !event.hasShift();
    }

    /// Fallback stride when no screen has yet rendered (no viewport observed).
    public static final int PAGE_STRIDE = 20;

    /// Side channel from a list screen's render → its handle: the visible
    /// row count the screen settled on. Used to size PgDn/PgUp jumps so
    /// they advance by exactly one viewport instead of a hard-coded 20.
    private static int observedViewportRows = -1;

    /// Called by a list screen's `render` to record the body row count it
    /// can show. The next `handle` will use this as the PgDn/PgUp stride.
    public static void observeViewport(int rows) {
        observedViewportRows = Math.max(1, rows);
    }

    /// Effective PgDn/PgUp stride: the most recently observed viewport row
    /// count, or `PAGE_STRIDE` before any screen has rendered.
    public static int viewportStride() {
        return observedViewportRows > 0 ? observedViewportRows : PAGE_STRIDE;
    }

    /// True iff a screen has rendered and recorded a viewport size — used
    /// by Data preview to gate viewport-driven page resizing so unit tests
    /// that supply an explicit page size aren't overridden.
    public static boolean hasObservedViewport() {
        return observedViewportRows > 0;
    }

    /// Test hook — clears the observed viewport so handler-only tests
    /// that ran after a render-path test don't see a viewport seeded
    /// by that render and trigger unwanted auto-resize.
    public static void resetObservedViewport() {
        observedViewportRows = -1;
    }

    /// Conditional-keybar builder. Each `add(enabled, binding)` appends the
    /// binding to the keybar only when `enabled` — so the resulting string
    /// lists exactly the keys that have a meaningful effect in the current
    /// screen state. Callers should phrase enablement at the
    /// "would-pressing-this-do-something-visible" level.
    public static final class Hints {
        private final StringBuilder sb = new StringBuilder();

        public Hints add(boolean enabled, String binding) {
            if (enabled) {
                if (!sb.isEmpty()) {
                    sb.append("  ");
                }
                sb.append(binding);
            }
            return this;
        }

        public String build() {
            return sb.toString();
        }
    }
}
