/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.dive.internal;

import dev.tamboui.style.Color;
import dev.tamboui.style.Style;

/// Centralised visual hierarchy for the `dive` TUI. Four roles named
/// for what they mark rather than for a specific colour, so call
/// sites stay stable when the implementations are retargeted.
///
/// - [#selection] — bold yellow. Active row in a navigable list:
///   table-row highlight, selected drill-into menu row, selected
///   schema name, footer active anchor, modal cursor.
/// - [#accent] ± `.bold()` — blue. Structural captions: section
///   headings, table column headers, app brand.
/// - [#primary] — bold default fg. Labels, breadcrumb leaf, enabled
///   drill-into menu labels, the `/` search prompt, "you-are-here"
///   markers.
/// - [#dim] — faint default fg. Persistent chrome: keybar, modal
///   hint bars, breadcrumb non-head, ` › ` separators, empty-state
///   messages, search-result counts, unfocused pane borders.
///
/// Body content (kv values, schema rows, top-bar facts, table data)
/// uses `Style.EMPTY` and reads as the user's terminal default fg.
///
/// See `_designs/DIVE_THEME.md` for the full decision tree authors
/// should follow when adding new content. Direct use of `Color.*`
/// constants or literal modifier styles outside this class is
/// reserved for `Theme` itself; everything else routes through one
/// of the four methods or stays unstyled.
public final class Theme {

    private Theme() {
    }

    /// Solarized's accent slots specified as truecolor RGB. Used
    /// when the terminal advertises truecolor support via
    /// `$COLORTERM`. Truecolor escapes bypass iTerm2's
    /// "Use bright colors for bold text" remapping (which would
    /// otherwise turn `bold + ANSI 4` into Solarized's `base0`,
    /// the default body fg on Dark) and render the exact hex on
    /// every variant.
    private static final Color SOLARIZED_BLUE = Color.rgb(38, 139, 210);
    private static final Color SOLARIZED_YELLOW = Color.rgb(181, 137, 0);

    /// Whether the host terminal advertises 24-bit truecolor support
    /// via `$COLORTERM`. Set by iTerm2, kitty, alacritty, WezTerm,
    /// Ghostty, modern gnome-terminal / xterm, VS Code's terminal,
    /// foot, and most other terminals released in the last decade
    /// that support truecolor. Probed once at class-load time —
    /// the value is fixed for the lifetime of the JVM.
    private static final boolean TRUECOLOR =
            "truecolor".equals(System.getenv("COLORTERM"))
                    || "24bit".equals(System.getenv("COLORTERM"));

    /// Bold default foreground — labels, breadcrumb leaf, enabled
    /// drill-into menu labels, the `/` search prompt, and other
    /// "you-are-here" markers. Adds visual weight without imposing
    /// a hue, so the tone tracks the user's terminal palette.
    public static Style primary() {
        return Style.EMPTY.bold();
    }

    /// Auxiliary text — keybar, modal hint bars, breadcrumb
    /// non-head, menu-row hints, empty-state messages, search-result
    /// counts. Uses the ANSI faint attribute (`ESC [ 2 m`) so the
    /// terminal renders the existing foreground colour in a fainter
    /// shade rather than overriding it with a specific grey. Tracks
    /// the user's terminal palette: faded `base0` on Solarized Dark,
    /// faded `base00` on Solarized Light, faded whatever-fg-is on
    /// every other palette. On terminals that don't honour the faint
    /// attribute the text falls back to default fg unchanged — a
    /// graceful degradation rather than the inversion / invisibility
    /// the named-grey approaches would produce.
    public static Style dim() {
        return Style.EMPTY.dim();
    }

    /// Structural-caption tone — section headings, table column
    /// headers, top-bar brand. Composed with `.bold()` at every
    /// such call site (`Theme.accent().bold()`). The bare,
    /// non-bold form is used for the schema tree's `▼` / `▶`
    /// expand/collapse marker.
    ///
    /// On truecolor terminals this is Solarized blue (`#268bd2`)
    /// pinned via RGB so iTerm2's "Use bright colors for bold
    /// text" remapping cannot turn `bold + ANSI 4` into Solarized's
    /// default body fg. On legacy terminals named `Color.BLUE`
    /// (ANSI 4) is used.
    public static Style accent() {
        return Style.EMPTY.fg(TRUECOLOR ? SOLARIZED_BLUE : Color.BLUE);
    }

    /// Active-row indicator for navigable tables and menus —
    /// distinct from [#accent] so that selected rows don't blur
    /// with section titles or focused borders. Bold yellow on
    /// truecolor terminals (Solarized yellow `#b58900`); bold
    /// named `Color.YELLOW` otherwise.
    public static Style selection() {
        return Style.EMPTY.bold().fg(TRUECOLOR ? SOLARIZED_YELLOW : Color.YELLOW);
    }
}
