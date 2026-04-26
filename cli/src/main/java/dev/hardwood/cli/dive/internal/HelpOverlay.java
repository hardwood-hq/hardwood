/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.dive.internal;

import java.util.List;

import dev.tamboui.buffer.Buffer;
import dev.tamboui.layout.Rect;
import dev.tamboui.style.Color;
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
        int height = Math.min(22, screenArea.height() - 2);
        int x = screenArea.left() + (screenArea.width() - width) / 2;
        int y = screenArea.top() + (screenArea.height() - height) / 2;
        Rect area = new Rect(x, y, width, height);

        List<Line> lines = List.of(
                Line.from(new Span("Navigation", Style.EMPTY.bold())),
                kv("↑ / ↓", "move selection"),
                kv("Enter", "drill into selected item"),
                kv("Esc / Backspace", "go back one level"),
                kv("Tab / Shift-Tab", "switch focused pane"),
                kv("g", "return to Overview"),
                Line.empty(),
                Line.from(new Span("Global", Style.EMPTY.bold())),
                kv("?", "toggle this help"),
                kv("q / Ctrl-C", "quit"),
                Line.empty(),
                Line.from(new Span("Modals", Style.EMPTY.bold())),
                kv("Enter", "open (Pages / Dictionary)"),
                kv("Esc / Enter", "close modal"),
                Line.empty(),
                Line.from(new Span("Press ? or Esc to close", Style.EMPTY.fg(Color.GRAY))));

        Block block = Block.builder()
                .title(" hardwood dive — help ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .borderColor(Color.CYAN)
                .build();
        Paragraph.builder().block(block).text(Text.from(lines)).left().build().render(area, buffer);
    }

    private static Line kv(String key, String description) {
        return Line.from(
                Span.raw("  "),
                new Span(padRight(key, 18), Style.EMPTY.fg(Color.CYAN)),
                new Span(description, Style.EMPTY));
    }

    private static String padRight(String s, int width) {
        if (s.length() >= width) {
            return s;
        }
        return s + " ".repeat(width - s.length());
    }
}
