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
        int height = Math.min(33, screenArea.height() - 2);
        int x = screenArea.left() + (screenArea.width() - width) / 2;
        int y = screenArea.top() + (screenArea.height() - height) / 2;
        Rect area = new Rect(x, y, width, height);
        // Wipe the area so the underlying screen doesn't bleed through cells
        // that the Paragraph doesn't paint.
        dev.tamboui.widgets.Clear.INSTANCE.render(area, buffer);

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


        Block block = Block.builder()
                .title(" hardwood dive — help ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .build();
        Paragraph.builder().block(block).text(Text.from(lines)).left().build().render(area, buffer);
    }

    private static List<Line> kv(String key, String description, int descBudget) {
        List<Line> result = new ArrayList<>();
        List<String> wrappedDescription = wrapValue(description, Math.max(1, descBudget - 1));

        result.add(Line.from(
                Span.raw("  "),
                new Span(padRight(key, 18), Theme.primary()),
                new Span(wrappedDescription.isEmpty() ? "" : wrappedDescription.get(0), Style.EMPTY)
        ));

        for (int i = 1; i < wrappedDescription.size(); i++){
            result.add(Line.from(
                    Span.raw(" ".repeat(20)),
                    new Span(wrappedDescription.get(i), Style.EMPTY)
            ));
        }

        return result;
    }

    private static String padRight(String s, int width) {
        return Strings.padRight(s, width);
    }

    private static List<String> wrapValue(String value, int width) {
        List<String> out = new ArrayList<>();
        if (width <= 0) {
            out.add(value);
            return out;
        }
        for (String line : value.split("\n", -1)) {
            if (line.isEmpty()) {
                out.add("");
                continue;
            }

            String[] words = line.split(" ", -1);
            StringBuilder currentLine = new StringBuilder();

            for (String word : words) {
                // If a single word is strictly longer than the whole width,
                // we are forced to character-wrap it so it doesn't break the UI boundary.
                while (word.length() > width) {
                    if (!currentLine.isEmpty()) {
                        out.add(currentLine.toString());
                        currentLine.setLength(0);
                    }
                    out.add(word.substring(0, width));
                    word = word.substring(width);
                }

                if (currentLine.isEmpty()) {
                    currentLine.append(word);
                } else if (currentLine.length() + 1 + word.length() <= width) {
                    currentLine.append(" ").append(word);
                } else {
                    out.add(currentLine.toString());
                    currentLine.setLength(0);
                    currentLine.append(word);
                }

            }

            if (!currentLine.isEmpty()) {
                out.add(currentLine.toString());
            }

        }
        return out;
    }
}
