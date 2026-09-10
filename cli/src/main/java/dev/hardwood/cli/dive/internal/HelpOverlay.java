/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.dive.internal;

import java.util.ArrayList;
import java.util.List;

import dev.hardwood.cli.internal.Strings;
import dev.hardwood.cli.internal.Version;
import dev.tamboui.buffer.Buffer;
import dev.tamboui.layout.Rect;
import dev.tamboui.style.Style;
import dev.tamboui.text.Line;
import dev.tamboui.text.Span;

/// Modal dialog listing all keybindings. Rendered on top of the active screen when
/// the user presses `?`; dismissed with `Esc` or `?` again.
public final class HelpOverlay {

    private HelpOverlay() {
    }

    public static void render(Buffer buffer, Rect screenArea, int scroll) {
        List<Line> lines = lines(screenArea);
        Rect area = ScrollPane.modalArea(screenArea, WIDTH, lines.size() + 4);
        ScrollPane.renderModal(buffer, area, "hardwood dive — help", lines, scroll,
                "Press ? or Esc to close");
    }

    /// Line count at this screen size, so the key handler knows how far the
    /// overlay can scroll.
    public static int lineCount(Rect screenArea) {
        return lines(screenArea).size();
    }

    /// Widest the overlay is allowed to get. Narrow enough to read, wide
    /// enough for the longest description without wrapping every row.
    private static final int WIDTH = 60;

    private static List<Line> lines(Rect screenArea) {
        int width = Math.min(WIDTH, Math.max(1, screenArea.width() - 4));
        int descBudget = Math.max(1, (width - 2) - 20);

        List<Line> lines = new ArrayList<>();
        // First, not last: this is where a TUI user finds out which build
        // they are on, and the overlay is longer than a short terminal shows.
        lines.add(Line.from(new Span("Version: " + Version.getVersion(), Theme.dim())));
        lines.add(Line.empty());
        lines.add(Line.from(new Span("Navigation", Theme.accent().bold())));
        lines.addAll(kv("↑ / ↓", "move one row or line", descBudget));
        lines.addAll(kv("PgDn / PgUp", "move one screenful (Shift+↓/↑ on macOS)", descBudget));
        lines.addAll(kv("g / G", "jump to first / last", descBudget));
        lines.addAll(kv("Enter", "drill into selected item", descBudget));
        lines.addAll(kv("Esc / Backspace", "go back one level", descBudget));
        lines.addAll(kv("Tab / Shift-Tab", "switch focused pane", descBudget));
        lines.addAll(kv("o", "return to Overview", descBudget));
        lines.add(Line.empty());
        lines.addAll(kv("▶", "Enter does something on this row", descBudget));
        lines.addAll(kv("colour", "where the cursor is", descBudget));
        lines.add(Line.empty());
        lines.add(Line.from(new Span("Schema tree", Theme.accent().bold())));
        lines.addAll(kv("→ / Enter", "expand group · drill leaf", descBudget));
        lines.addAll(kv("←", "collapse group", descBudget));
        lines.addAll(kv("e / c", "expand / collapse all groups", descBudget));
        lines.add(Line.empty());
        lines.add(Line.from(new Span("Inline search", Theme.accent().bold())));
        lines.addAll(kv("/", "enter filter mode (Schema, Column index, Dictionary)", descBudget));
        lines.addAll(kv("Enter", "commit filter", descBudget));
        lines.addAll(kv("Esc", "clear filter", descBudget));
        lines.add(Line.empty());
        lines.add(Line.from(new Span("Global", Theme.accent().bold())));
        lines.addAll(kv("?", "toggle this help", descBudget));
        lines.addAll(kv("q / Ctrl-C", "quit", descBudget));
        lines.add(Line.empty());
        lines.add(Line.from(new Span("Data preview", Theme.accent().bold())));
        lines.addAll(kv("← / →", "scroll visible columns", descBudget));
        lines.addAll(kv("Enter", "open / expand the record field", descBudget));
        lines.addAll(kv("e / c", "expand / collapse all fields", descBudget));
        lines.addAll(kv(":", "jump to a row, or rg N for a row group", descBudget));
        lines.add(Line.empty());
        lines.add(Line.from(new Span("Row groups", Theme.accent().bold())));
        lines.addAll(kv("d", "open the Data preview at this row group", descBudget));
        return lines;
    }

    private static List<Line> kv(String key, String description, int descBudget) {
        List<Line> result = new ArrayList<>();
        List<String> wrappedDescription = Strings.wordWrap(description, descBudget);

        result.add(Line.from(
                Span.raw("  "),
                new Span(Strings.padRight(key, 18), Theme.primary()),
                new Span(wrappedDescription.isEmpty() ? "" : wrappedDescription.get(0), Style.EMPTY)
        ));

        for (int i = 1; i < wrappedDescription.size(); i++) {
            result.add(Line.from(
                    Span.raw(" ".repeat(20)),
                    new Span(wrappedDescription.get(i), Style.EMPTY)
            ));
        }

        return result;
    }
}
