/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.dive.internal;

import dev.tamboui.style.Color;

/// Centralised colour palette for the `dive` TUI. No visual change from the
/// ad-hoc `Color.CYAN` / `Color.GRAY` screens used previously — this just
/// puts the two colours behind named constants so a future retheme
/// (light-terminal variant, `--no-color`, etc.) doesn't have to grep every
/// screen.
public final class Theme {

    /// Accent colour for focused borders, active titles, and `/` search
    /// prompt. Renders as bright cyan on a dark terminal.
    public static final Color ACCENT = Color.CYAN;

    /// Dim colour for secondary text (breadcrumb non-head, keybar,
    /// help-overlay trailer, absent-value markers) and unfocused borders.
    public static final Color DIM = Color.GRAY;

    private Theme() {
    }
}
