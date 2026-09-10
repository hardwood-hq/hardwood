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
    /// `d` opens the Data preview at the row group under the cursor. Modifier
    /// free, like the other single-letter keys.
    public static boolean isOpenDataPreview(KeyEvent event) {
        return event.code() == KeyCode.CHAR && event.character() == 'd'
                && !event.hasCtrl() && !event.hasAlt();
    }

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

    /// Fallback terminal width before the first frame renders.
    private static final int DEFAULT_VIEWPORT_COLUMNS = 120;

    /// Fallback terminal height before the first frame renders.
    private static final int DEFAULT_VIEWPORT_ROWS = 40;

    /// Side channel from a list screen's render → its handle: the visible
    /// row count the screen settled on. Used to size PgDn/PgUp jumps so
    /// they advance by exactly one viewport instead of a hard-coded 20.
    private static int observedViewportRows = -1;

    /// Side channel used by horizontally scrolling screens to keep key
    /// handling consistent with the most recently rendered column window.
    private static int observedViewportColumns = -1;

    /// Side channel from Data preview's render → its handle. The record modal
    /// derives its wrapping budget and viewport height from the area it is
    /// drawn into, and the key handler has to reach the same answers as the
    /// drawing code — which field is expandable, how far a page scrolls.
    /// Scoped to the one screen that owns it: a second writer would silently
    /// change how the modal navigates.
    private static int observedDataPreviewWidth = -1;
    private static int observedDataPreviewHeight = -1;

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

    /// Content width of the modal the last frame rendered. Modals wrap their
    /// content, so the key handler needs the same width the renderer used to
    /// agree with it on the line count.
    private static int observedModalWidth = -1;

    /// Called by [ScrollPane#renderModal] with the width it wrapped to.
    public static void observeModalWidth(int columns) {
        observedModalWidth = Math.max(1, columns);
    }

    /// Most recently observed modal content width, or a pre-render fallback.
    public static int modalWidth() {
        return observedModalWidth > 0 ? observedModalWidth : DEFAULT_VIEWPORT_COLUMNS;
    }

    /// Records the terminal width used by horizontally scrolling screens.
    public static void observeViewportWidth(int columns) {
        observedViewportColumns = Math.max(1, columns);
    }

    /// Most recently observed terminal width, or a pre-render fallback.
    public static int viewportWidth() {
        return observedViewportColumns > 0 ? observedViewportColumns : DEFAULT_VIEWPORT_COLUMNS;
    }

    /// Called by Data preview's `render` to record the area it was drawn into.
    public static void observeDataPreviewArea(int width, int height) {
        observedDataPreviewWidth = Math.max(1, width);
        observedDataPreviewHeight = Math.max(1, height);
    }

    /// Width of the area Data preview last rendered into, or a pre-render
    /// fallback.
    public static int dataPreviewAreaWidth() {
        return observedDataPreviewWidth > 0 ? observedDataPreviewWidth : DEFAULT_VIEWPORT_COLUMNS;
    }

    /// Height of the area Data preview last rendered into, or a pre-render
    /// fallback.
    public static int dataPreviewAreaHeight() {
        return observedDataPreviewHeight > 0 ? observedDataPreviewHeight : DEFAULT_VIEWPORT_ROWS;
    }

    /// Test hook — clears every observed geometry so handler-only tests that
    /// ran after a render-path test don't see a viewport or area seeded by
    /// that render and trigger unwanted auto-resize.
    public static void resetObservedGeometry() {
        observedViewportRows = -1;
        observedViewportColumns = -1;
        observedModalWidth = -1;
        observedDataPreviewWidth = -1;
        observedDataPreviewHeight = -1;
    }

    /// Conditional-keybar builder. Each `add(enabled, binding)` appends the
    /// binding to the keybar only when `enabled` — so the resulting string
    /// lists exactly the keys that have a meaningful effect in the current
    /// screen state. Callers should phrase enablement at the
    /// "would-pressing-this-do-something-visible" level.
    ///
    /// An empty binding is dropped rather than separated, so a fragment
    /// produced elsewhere — [ScrollPane#hints(int,int)], say — can be added
    /// unconditionally and contribute nothing when it is empty.
    public static final class Hints {
        private final StringBuilder sb = new StringBuilder();

        public Hints add(boolean enabled, String binding) {
            if (enabled && !binding.isEmpty()) {
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
