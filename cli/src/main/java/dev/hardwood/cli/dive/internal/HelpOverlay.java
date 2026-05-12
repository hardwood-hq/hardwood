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

import dev.hardwood.cli.internal.Version;
import dev.tamboui.buffer.Buffer;
import dev.tamboui.layout.Rect;
import dev.tamboui.style.Style;
import dev.tamboui.text.Line;
import dev.tamboui.text.Span;
import dev.tamboui.text.Text;
import dev.tamboui.widgets.block.Block;
import dev.tamboui.widgets.block.BorderType;
import dev.tamboui.widgets.block.Borders;
import dev.tamboui.widgets.paragraph.Paragraph;

/// Modal dialog listing all keybindings. Rendered on top of the active screen when
/// the user presses `?`; dismissed with `Esc` or `?` again.
public final class HelpOverlay {

    private HelpOverlay() {
    }

    public static void render(Buffer buffer, Rect screenArea) {
        int width = Math.min(60, screenArea.width() - 4);
        int descBudget = Math.max(1, (width - 2) - 20);

        List<Line> lines = new ArrayList<>();
        lines.add(Line.from(new Span("Navigation", Theme.accent().bold())));
        lines.addAll(kv("↑ / ↓", "move selection", descBudget));
        lines.addAll(kv("g / G", "jump to first / last row", descBudget));
        lines.addAll(kv("Enter", "drill into selected item", descBudget));
        lines.addAll(kv("Esc / Backspace", "go back one level", descBudget));
        lines.addAll(kv("Tab / Shift-Tab", "switch focused pane", descBudget));
        lines.addAll(kv("o", "return to Overview", descBudget));
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
        lines.addAll(kv("PgDn / PgUp", "page forward / back (Shift+↓/↑ on macOS)", descBudget));
        lines.addAll(kv("← / →", "scroll visible columns", descBudget));
        lines.addAll(kv("g / G", "jump to first / last row of file", descBudget));
        lines.add(Line.empty());
        lines.add(Line.from(new Span("Version: " + Version.getVersion(), Theme.dim())));
        lines.add(Line.from(new Span("Press ? or Esc to close", Theme.dim())));

        // Height is derived from the produced line count (plus 2 for the borders)
        // so the overlay grows with the wrapped content. Capped to the screen so
        // we don't draw outside the buffer.
        int height = Math.min(lines.size() + 2, screenArea.height() - 2);
        int x = screenArea.left() + (screenArea.width() - width) / 2;
        int y = screenArea.top() + (screenArea.height() - height) / 2;
        Rect area = new Rect(x, y, width, height);
        // Wipe the area so the underlying screen doesn't bleed through cells
        // that the Paragraph doesn't paint.
        dev.tamboui.widgets.Clear.INSTANCE.render(area, buffer);

        Block block = Block.builder()
                .title(" hardwood dive — help ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .build();
        Paragraph.builder().block(block).text(Text.from(lines)).left().build().render(area, buffer);
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
