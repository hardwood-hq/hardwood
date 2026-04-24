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
import java.util.Map;

import dev.hardwood.cli.dive.NavigationStack;
import dev.hardwood.cli.dive.ParquetModel;
import dev.hardwood.cli.dive.ScreenState;
import dev.hardwood.cli.internal.Sizes;
import dev.tamboui.buffer.Buffer;
import dev.tamboui.layout.Constraint;
import dev.tamboui.layout.Layout;
import dev.tamboui.layout.Rect;
import dev.tamboui.style.Color;
import dev.tamboui.style.Style;
import dev.tamboui.text.Line;
import dev.tamboui.text.Span;
import dev.tamboui.text.Text;
import dev.tamboui.tui.event.KeyEvent;
import dev.tamboui.widgets.block.Block;
import dev.tamboui.widgets.block.BorderType;
import dev.tamboui.widgets.block.Borders;
import dev.tamboui.widgets.paragraph.Paragraph;

/// The root screen of `hardwood dive`. Two panes: file facts (left, read-only) and
/// a drill-into menu (right, selectable). Phase 1 only implements Schema and Row
/// groups drills — Footer and Data preview entries are shown disabled.
public final class OverviewScreen {

    /// Menu entries in display order. Enabled items drill into their screen when
    /// the user presses Enter; disabled items are rendered dimmed and ignored.
    public enum MenuItem {
        SCHEMA("Schema", true),
        ROW_GROUPS("Row groups", true),
        FOOTER("Footer & indexes", true),
        DATA_PREVIEW("Data preview", true);

        final String label;
        final boolean enabled;

        MenuItem(String label, boolean enabled) {
            this.label = label;
            this.enabled = enabled;
        }
    }

    static final int MENU_SIZE = MenuItem.values().length;

    private OverviewScreen() {
    }

    public static boolean handle(KeyEvent event, ParquetModel model, NavigationStack stack) {
        ScreenState.Overview state = (ScreenState.Overview) stack.top();
        if (event.isFocusNext() || event.isFocusPrevious()) {
            ScreenState.Overview.Pane next = state.focus() == ScreenState.Overview.Pane.FACTS
                    ? ScreenState.Overview.Pane.MENU
                    : ScreenState.Overview.Pane.FACTS;
            stack.replaceTop(new ScreenState.Overview(next, state.menuSelection()));
            return true;
        }
        if (state.focus() == ScreenState.Overview.Pane.MENU) {
            if (event.isUp()) {
                stack.replaceTop(new ScreenState.Overview(state.focus(),
                        Math.max(0, state.menuSelection() - 1)));
                return true;
            }
            if (event.isDown()) {
                stack.replaceTop(new ScreenState.Overview(state.focus(),
                        Math.min(MENU_SIZE - 1, state.menuSelection() + 1)));
                return true;
            }
            if (event.isConfirm()) {
                MenuItem item = MenuItem.values()[state.menuSelection()];
                if (!item.enabled) {
                    return false;
                }
                switch (item) {
                    case SCHEMA -> stack.push(new ScreenState.Schema(0));
                    case ROW_GROUPS -> stack.push(new ScreenState.RowGroups(0));
                    case FOOTER -> stack.push(new ScreenState.Footer());
                    case DATA_PREVIEW -> stack.push(
                            DataPreviewScreen.initialState(model, model.previewPageSize()));
                }
                return true;
            }
        }
        return false;
    }

    public static void render(Buffer buffer, Rect area, ParquetModel model, ScreenState.Overview state) {
        List<Rect> cols = Layout.horizontal()
                .constraints(new Constraint.Percentage(50), new Constraint.Percentage(50))
                .split(area);
        renderFactsPane(buffer, cols.get(0), model, state.focus() == ScreenState.Overview.Pane.FACTS);
        renderMenuPane(buffer, cols.get(1), model, state);
    }

    public static String keybarKeys() {
        return "[Tab] pane  [↑↓] move  [Enter] drill";
    }

    private static void renderFactsPane(Buffer buffer, Rect area, ParquetModel model, boolean focused) {
        Block block = paneBlock("File facts", focused);
        ParquetModel.Facts f = model.facts();
        List<Line> lines = new ArrayList<>();
        lines.add(factsLine("Format version", String.valueOf(f.formatVersion())));
        lines.add(factsLine("Created by", f.createdBy() != null ? f.createdBy() : "unknown"));
        lines.add(factsLine("Uncompressed", Sizes.format(f.uncompressedBytes())));
        lines.add(factsLine("Compressed", Sizes.format(f.compressedBytes())));
        lines.add(factsLine("Ratio", String.format("%.1f×", f.compressionRatio())));
        List<Map.Entry<String, String>> kv = f.keyValueMetadata();
        if (!kv.isEmpty()) {
            lines.add(Line.empty());
            lines.add(Line.from(new Span("key/value meta (" + kv.size() + ")", Style.EMPTY.bold())));
            for (Map.Entry<String, String> entry : kv) {
                lines.add(factsLine("  " + entry.getKey(), trim(entry.getValue(), 32)));
            }
        }
        renderParagraph(buffer, area, block, Text.from(lines));
    }

    private static void renderMenuPane(Buffer buffer, Rect area, ParquetModel model, ScreenState.Overview state) {
        boolean focused = state.focus() == ScreenState.Overview.Pane.MENU;
        Block block = paneBlock("Drill into", focused);
        List<Line> lines = new ArrayList<>();
        MenuItem[] items = MenuItem.values();
        for (int i = 0; i < items.length; i++) {
            MenuItem item = items[i];
            boolean selected = focused && i == state.menuSelection();
            String cursor = selected ? "▶ " : "  ";
            String hint = menuHint(item, model);
            Style labelStyle = !item.enabled
                    ? Style.EMPTY.fg(Color.GRAY)
                    : selected ? Style.EMPTY.bold() : Style.EMPTY;
            Style hintStyle = Style.EMPTY.fg(Color.GRAY);
            lines.add(Line.from(
                    new Span(cursor, labelStyle),
                    new Span(padRight(item.label, 20), labelStyle),
                    new Span(hint, hintStyle)));
        }
        renderParagraph(buffer, area, block, Text.from(lines));
    }

    private static String menuHint(MenuItem item, ParquetModel model) {
        return switch (item) {
            case SCHEMA -> model.columnCount() + " cols";
            case ROW_GROUPS -> model.rowGroupCount() + " RGs";
            case FOOTER -> Sizes.format(model.fileSizeBytes());
            case DATA_PREVIEW -> String.format("%,d rows", model.facts().totalRows());
        };
    }

    private static Line factsLine(String key, String value) {
        return Line.from(
                new Span(padRight(key, 16), Style.EMPTY),
                new Span(value, Style.EMPTY));
    }

    private static Block paneBlock(String title, boolean focused) {
        Block.Builder b = Block.builder()
                .title(" " + title + " ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED);
        if (focused) {
            b.borderColor(Color.CYAN);
        }
        else {
            b.borderColor(Color.GRAY);
        }
        return b.build();
    }

    private static void renderParagraph(Buffer buffer, Rect area, Block block, Text text) {
        Paragraph.builder().block(block).text(text).left().build().render(area, buffer);
    }

    private static String padRight(String s, int width) {
        if (s.length() >= width) {
            return s;
        }
        return s + " ".repeat(width - s.length());
    }

    private static String trim(String s, int max) {
        if (s == null) {
            return "";
        }
        if (s.length() <= max) {
            return s;
        }
        return s.substring(0, max - 1) + "…";
    }
}
