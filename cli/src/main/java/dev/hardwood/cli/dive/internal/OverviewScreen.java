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
        if (state.kvModalOpen()) {
            if (event.isCancel() || event.isConfirm()) {
                stack.replaceTop(withKvModal(state, false));
                return true;
            }
            int totalLines = kvModalLineCount(model, state);
            // Plain ↑/↓ scroll one line; Shift+↑/↓ scroll a page. The Mac
            // chord (no PgUp/PgDn dedicated keys) and the line variant share
            // the same model — we don't know the modal viewport height here,
            // so a "page" is approximated as 10 lines, matching the visible
            // height the renderer settles on for modest terminals.
            if (event.isUp() && !event.hasShift()) {
                if (state.kvModalScroll() == 0) {
                    return false;
                }
                stack.replaceTop(withKvScroll(state, state.kvModalScroll() - 1));
                return true;
            }
            if (event.isDown() && !event.hasShift()) {
                if (state.kvModalScroll() >= Math.max(0, totalLines - 1)) {
                    return false;
                }
                stack.replaceTop(withKvScroll(state, state.kvModalScroll() + 1));
                return true;
            }
            if (event.isUp() && event.hasShift()) {
                stack.replaceTop(withKvScroll(state, Math.max(0, state.kvModalScroll() - 10)));
                return true;
            }
            if (event.isDown() && event.hasShift()) {
                int max = Math.max(0, totalLines - 1);
                stack.replaceTop(withKvScroll(state, Math.min(max, state.kvModalScroll() + 10)));
                return true;
            }
            return false;
        }
        if (event.isFocusNext() || event.isFocusPrevious()) {
            ScreenState.Overview.Pane next = state.focus() == ScreenState.Overview.Pane.FACTS
                    ? ScreenState.Overview.Pane.MENU
                    : ScreenState.Overview.Pane.FACTS;
            stack.replaceTop(withFocus(state, next));
            return true;
        }
        if (state.focus() == ScreenState.Overview.Pane.FACTS) {
            int kvCount = model.facts().keyValueMetadata().size();
            if (kvCount == 0) {
                return false;
            }
            if (event.isUp()) {
                stack.replaceTop(withKvSelection(state, Math.max(0, state.kvSelection() - 1)));
                return true;
            }
            if (event.isDown()) {
                stack.replaceTop(withKvSelection(state, Math.min(kvCount - 1, state.kvSelection() + 1)));
                return true;
            }
            if (event.isConfirm()) {
                stack.replaceTop(withKvModal(state, true));
                return true;
            }
            return false;
        }
        if (event.isUp()) {
            stack.replaceTop(withMenuSelection(state, Math.max(0, state.menuSelection() - 1)));
            return true;
        }
        if (event.isDown()) {
            stack.replaceTop(withMenuSelection(state, Math.min(MENU_SIZE - 1, state.menuSelection() + 1)));
            return true;
        }
        if (event.isConfirm()) {
            MenuItem item = MenuItem.values()[state.menuSelection()];
            if (!item.enabled) {
                return false;
            }
            switch (item) {
                case SCHEMA -> stack.push(ScreenState.Schema.initial());
                case ROW_GROUPS -> stack.push(new ScreenState.RowGroups(0));
                case FOOTER -> stack.push(new ScreenState.Footer());
                case DATA_PREVIEW -> stack.push(
                        DataPreviewScreen.initialState(model, model.previewPageSize()));
            }
            return true;
        }
        return false;
    }

    private static ScreenState.Overview withFocus(ScreenState.Overview s, ScreenState.Overview.Pane next) {
        return new ScreenState.Overview(next, s.menuSelection(), s.kvSelection(),
                s.kvModalOpen(), s.kvModalScroll());
    }

    private static ScreenState.Overview withMenuSelection(ScreenState.Overview s, int sel) {
        return new ScreenState.Overview(s.focus(), sel, s.kvSelection(),
                s.kvModalOpen(), s.kvModalScroll());
    }

    private static ScreenState.Overview withKvSelection(ScreenState.Overview s, int sel) {
        return new ScreenState.Overview(s.focus(), s.menuSelection(), sel,
                s.kvModalOpen(), 0);
    }

    private static ScreenState.Overview withKvModal(ScreenState.Overview s, boolean open) {
        return new ScreenState.Overview(s.focus(), s.menuSelection(), s.kvSelection(), open, 0);
    }

    private static ScreenState.Overview withKvScroll(ScreenState.Overview s, int scroll) {
        return new ScreenState.Overview(s.focus(), s.menuSelection(), s.kvSelection(),
                s.kvModalOpen(), scroll);
    }

    private static int kvModalLineCount(ParquetModel model, ScreenState.Overview state) {
        java.util.List<java.util.Map.Entry<String, String>> kv = model.facts().keyValueMetadata();
        if (kv.isEmpty()) {
            return 0;
        }
        int idx = Math.min(state.kvSelection(), kv.size() - 1);
        java.util.Map.Entry<String, String> entry = kv.get(idx);
        return KvMetadataFormatter.format(entry.getKey(), entry.getValue()).split("\n", -1).length;
    }

    public static void render(Buffer buffer, Rect area, ParquetModel model, ScreenState.Overview state) {
        List<Rect> cols = Layout.horizontal()
                .constraints(new Constraint.Percentage(50), new Constraint.Percentage(50))
                .split(area);
        renderFactsPane(buffer, cols.get(0), model, state);
        renderMenuPane(buffer, cols.get(1), model, state);
        if (state.kvModalOpen()) {
            renderKvModal(buffer, area, model, state);
        }
    }

    public static String keybarKeys() {
        return "[Tab] pane  [↑↓] move  [Enter] view entry";
    }

    private static void renderFactsPane(Buffer buffer, Rect area, ParquetModel model, ScreenState.Overview state) {
        boolean focused = state.focus() == ScreenState.Overview.Pane.FACTS;
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
            for (int i = 0; i < kv.size(); i++) {
                Map.Entry<String, String> entry = kv.get(i);
                boolean selected = focused && i == state.kvSelection();
                String marker = selected ? "▶ " : "  ";
                Style keyStyle = selected ? Style.EMPTY.bold() : Style.EMPTY;
                lines.add(Line.from(
                        new Span(marker, keyStyle),
                        new Span(padRight(entry.getKey(), 16), keyStyle),
                        new Span(trim(entry.getValue(), 32), keyStyle)));
            }
        }
        renderParagraph(buffer, area, block, Text.from(lines));
    }

    private static void renderKvModal(Buffer buffer, Rect screenArea, ParquetModel model, ScreenState.Overview state) {
        List<Map.Entry<String, String>> kv = model.facts().keyValueMetadata();
        if (kv.isEmpty()) {
            return;
        }
        int idx = Math.min(state.kvSelection(), kv.size() - 1);
        Map.Entry<String, String> entry = kv.get(idx);
        // Grow the modal to fill the available area (leaving a 2-cell margin),
        // not a fixed 30 lines — for ARROW:schema the formatted hex dump can
        // be hundreds of lines and needs the room.
        int width = Math.min(120, screenArea.width() - 4);
        int height = Math.max(8, screenArea.height() - 2);
        int x = screenArea.left() + (screenArea.width() - width) / 2;
        int y = screenArea.top() + (screenArea.height() - height) / 2;
        Rect area = new Rect(x, y, width, height);
        dev.tamboui.widgets.Clear.INSTANCE.render(area, buffer);

        String[] all = KvMetadataFormatter.format(entry.getKey(), entry.getValue()).split("\n", -1);
        // Reserve 2 rows for borders + 2 rows for the close hint and a blank
        // separator. The remaining inner height is the content viewport.
        int viewport = Math.max(1, height - 4);
        int scroll = Math.max(0, Math.min(state.kvModalScroll(), Math.max(0, all.length - 1)));
        int end = Math.min(all.length, scroll + viewport);

        List<Line> lines = new ArrayList<>();
        for (int i = scroll; i < end; i++) {
            lines.add(Line.from(Span.raw(" " + all[i])));
        }
        lines.add(Line.empty());
        String hint = scroll + viewport < all.length
                ? " ↓ " + (all.length - end) + " more lines · Esc / Enter close · ↑↓ scroll · Shift+↑↓ page"
                : (scroll > 0
                        ? " ↑ " + scroll + " lines above · Esc / Enter close · ↑↓ scroll · Shift+↑↓ page"
                        : " Press Esc or Enter to close");
        lines.add(Line.from(new Span(hint, Style.EMPTY.fg(Theme.DIM))));
        Block block = Block.builder()
                .title(" " + entry.getKey() + " ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .borderColor(Theme.ACCENT)
                .build();
        Paragraph.builder().block(block).text(Text.from(lines)).left().build().render(area, buffer);
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
                    ? Style.EMPTY.fg(Theme.DIM)
                    : selected ? Style.EMPTY.bold() : Style.EMPTY;
            Style hintStyle = Style.EMPTY.fg(Theme.DIM);
            lines.add(Line.from(
                    new Span(cursor, labelStyle),
                    new Span(padRight(item.label, 20), labelStyle),
                    new Span(hint, hintStyle)));
        }
        renderParagraph(buffer, area, block, Text.from(lines));
    }

    private static String menuHint(MenuItem item, ParquetModel model) {
        return switch (item) {
            case SCHEMA -> Plurals.format(model.columnCount(), "column", "columns")
                    + "   · browse by column";
            case ROW_GROUPS -> Plurals.format(model.rowGroupCount(), "group", "groups")
                    + "    · browse by row group";
            case FOOTER -> Sizes.format(model.fileSizeBytes());
            case DATA_PREVIEW -> Plurals.format(model.facts().totalRows(), "row", "rows");
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
            b.borderColor(Theme.ACCENT);
        }
        else {
            b.borderColor(Theme.DIM);
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
