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
import dev.hardwood.cli.internal.Strings;
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
/// a drill-into menu (right, selectable).
public final class OverviewScreen {

    /// Menu entries in display order. Enter drills into the selected item's screen.
    public enum MenuItem {
        SCHEMA("Schema"),
        ROW_GROUPS("Row groups"),
        FOOTER("Footer & indexes"),
        DATA_PREVIEW("Data preview");

        final String label;

        MenuItem(String label) {
            this.label = label;
        }
    }

    static final int MENU_SIZE = MenuItem.values().length;

    /// Rows the facts pane holds before its key/value list: the five facts.
    /// The heading and the blank above it are decoration, so they do not
    /// count — an entry's row is this plus its index.
    private static final int FACTS_HEADER_ROWS = 5;

    /// Rows in the facts pane, which is what the cursor moves over.
    private static int factsRowCount(ParquetModel model) {
        return FACTS_HEADER_ROWS + model.facts().keyValueMetadata().size();
    }

    /// The key/value entry `row` shows, or `-1` for one of the facts above
    /// the list — a row the cursor rests on but `Enter` cannot act on.
    private static int kvIndexForRow(int row) {
        return row < FACTS_HEADER_ROWS ? -1 : row - FACTS_HEADER_ROWS;
    }

    /// The pane row showing key/value entry `index` — the inverse of
    /// [#kvIndexForRow(int)], for callers that know which entry they want.
    public static int kvEntryRow(int index) {
        return FACTS_HEADER_ROWS + index;
    }

    private OverviewScreen() {
    }

    public static boolean handle(KeyEvent event, ParquetModel model, NavigationStack stack) {
        ScreenState.Overview state = (ScreenState.Overview) stack.top();
        if (state.kvModalOpen()) {
            if (event.isCancel() || event.isConfirm()) {
                stack.replaceTop(withKvModal(state, false));
                return true;
            }
            int next = ScrollPane.scroll(event, state.kvModalScroll(),
                    kvModalLineCount(model, state), Keys.viewportStride());
            if (next == ScrollPane.UNHANDLED) {
                return false;
            }
            if (next != state.kvModalScroll()) {
                stack.replaceTop(withKvScroll(state, next));
            }
            return true;
        }
        if (event.isFocusNext() || event.isFocusPrevious()) {
            ScreenState.Overview.Pane next = state.focus() == ScreenState.Overview.Pane.FACTS
                    ? ScreenState.Overview.Pane.MENU
                    : ScreenState.Overview.Pane.FACTS;
            stack.replaceTop(withFocus(state, next, model));
            return true;
        }
        if (state.focus() == ScreenState.Overview.Pane.FACTS) {
            int selected = factsDocument(model, state, true)
                    .select(event, state.kvSelection(), Keys.viewportStride());
            if (selected != CursorPane.UNHANDLED) {
                stack.replaceTop(withKvSelection(state, selected, model));
                return true;
            }
            if (event.isConfirm() && kvIndexForRow(state.kvSelection()) >= 0) {
                stack.replaceTop(withKvModal(state, true));
                return true;
            }
            return false;
        }
        int onMenu = CursorPane.select(event, state.menuSelection(), MENU_SIZE);
        if (onMenu != CursorPane.UNHANDLED) {
            stack.replaceTop(withMenuSelection(state, onMenu));
            return true;
        }
        if (event.isConfirm()) {
            MenuItem item = MenuItem.values()[state.menuSelection()];
            switch (item) {
                case SCHEMA -> stack.push(ScreenState.Schema.initial());
                case ROW_GROUPS -> stack.push(new ScreenState.RowGroups(0));
                case FOOTER -> stack.push(FooterScreen.initialState(model));
                case DATA_PREVIEW -> stack.push(DataPreviewScreen.initialState(model));
            }
            return true;
        }
        return false;
    }

    /// Switching into the facts pane puts the cursor on the first key/value
    /// entry rather than the first fact: the entries are what `Enter` acts
    /// on, and the facts above them are read without being visited.
    private static ScreenState.Overview withFocus(ScreenState.Overview s,
                                                  ScreenState.Overview.Pane next,
                                                  ParquetModel model) {
        int cursor = s.kvSelection();
        int top = s.factsTop();
        if (next == ScreenState.Overview.Pane.FACTS && cursor == 0
                && !model.facts().keyValueMetadata().isEmpty()) {
            cursor = FACTS_HEADER_ROWS;
        }
        return new ScreenState.Overview(next, s.menuSelection(), cursor,
                s.kvModalOpen(), s.kvModalScroll(), top);
    }

    private static ScreenState.Overview withMenuSelection(ScreenState.Overview s, int sel) {
        return new ScreenState.Overview(s.focus(), sel, s.kvSelection(),
                s.kvModalOpen(), s.kvModalScroll(), s.factsTop());
    }

    /// Moving the key/value cursor resets the modal scroll — a new entry is
    /// a new document — and slides the pane's window the least it can to keep
    /// the cursor on screen.
    private static ScreenState.Overview withKvSelection(ScreenState.Overview s, int sel,
                                                       ParquetModel model) {
        Document facts = factsDocument(model, s, true);
        return new ScreenState.Overview(s.focus(), s.menuSelection(), sel, s.kvModalOpen(), 0,
                facts.windowTopAfterMove(s.factsTop(), s.kvSelection(), sel,
                        Keys.viewportStride()));
    }

    private static ScreenState.Overview withKvModal(ScreenState.Overview s, boolean open) {
        return new ScreenState.Overview(s.focus(), s.menuSelection(), s.kvSelection(), open, 0, s.factsTop());
    }

    private static ScreenState.Overview withKvScroll(ScreenState.Overview s, int scroll) {
        return new ScreenState.Overview(s.focus(), s.menuSelection(), s.kvSelection(),
                s.kvModalOpen(), scroll, s.factsTop());
    }

    private static int kvModalLineCount(ParquetModel model, ScreenState.Overview state) {
        java.util.List<java.util.Map.Entry<String, String>> kv = model.facts().keyValueMetadata();
        if (kv.isEmpty()) {
            return 0;
        }
        int idx = Math.max(0, Math.min(kvIndexForRow(state.kvSelection()), kv.size() - 1));
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
            buffer.setStyle(area, Theme.dim());
            renderKvModal(buffer, area, model, state);
        }
    }

    public static String keybarKeys(ScreenState.Overview state, ParquetModel model) {
        if (state.kvModalOpen()) {
            return "";
        }
        boolean onFacts = state.focus() == ScreenState.Overview.Pane.FACTS;
        int kvCount = model.facts().keyValueMetadata().size();
        boolean factsHasKv = kvCount > 0;
        return new Keys.Hints()
                .add(factsHasKv, "[Tab] pane")
                .add(true, onFacts
                        ? factsDocument(model, state, true).hints(Keys.viewportStride())
                        : CursorPane.hints(MENU_SIZE))
                .add(onFacts && kvIndexForRow(state.kvSelection()) >= 0, "[Enter] view entry")
                .add(!onFacts, "[Enter] open")
                .build();
    }

    /// The facts pane's content: the facts, then the key/value entries.
    /// Section headings and the blank above them are decoration — the cursor
    /// passes over them, since there is nothing there to be at.
    private static Document factsDocument(ParquetModel model, ScreenState.Overview state,
                                          boolean focused) {
        ParquetModel.Facts f = model.facts();
        int cursorRow = focused ? state.kvSelection() : -1;
        List<Map.Entry<String, String>> kv = f.keyValueMetadata();
        Document.Builder doc = Document.builder();
        doc.row(factsLine("Format version", String.valueOf(f.formatVersion()), cursorRow == 0));
        doc.row(factsLine("Created by", f.createdBy() != null ? f.createdBy() : "unknown",
                cursorRow == 1));
        doc.row(factsLine("Uncompressed", Sizes.format(f.uncompressedBytes()), cursorRow == 2));
        doc.row(factsLine("Compressed", Sizes.format(f.compressedBytes()), cursorRow == 3));
        doc.row(factsLine("Compression",
                Sizes.compression(f.compressedBytes(), f.uncompressedBytes(), "—"), cursorRow == 4));
        if (!kv.isEmpty()) {
            doc.blank();
            doc.decoration(Line.from(new Span("  key/value meta (" + kv.size() + ")",
                    Theme.accent().bold())));
            for (int i = 0; i < kv.size(); i++) {
                Map.Entry<String, String> entry = kv.get(i);
                boolean selected = FACTS_HEADER_ROWS + i == cursorRow;
                // The facts above are not actionable, so this is a mixed
                // pane: every entry is marked, not only the one under the
                // cursor.
                String marker = CursorPane.marker(true, selected, true);
                Style rowStyle = selected ? Theme.selection() : null;
                Style keyStyle = rowStyle != null ? rowStyle : Theme.primary();
                Style valueStyle = rowStyle != null ? rowStyle : Style.EMPTY;
                doc.row(Line.from(
                        new Span(marker, keyStyle),
                        new Span(padRight(entry.getKey(), 16), keyStyle),
                        new Span(trim(entry.getValue(), 32), valueStyle)));
            }
        }
        return doc.build();
    }

    private static void renderFactsPane(Buffer buffer, Rect area, ParquetModel model, ScreenState.Overview state) {
        boolean focused = state.focus() == ScreenState.Overview.Pane.FACTS;
        Document facts = factsDocument(model, state, focused);
        int viewport = Math.max(1, area.height() - 2);
        Keys.observeViewport(viewport);
        int cursorLine = Math.max(0, facts.lineOfRow(state.kvSelection()));
        RowWindow window = RowWindow.from(
                facts.windowTop(state.factsTop(), state.kvSelection(), viewport),
                cursorLine, facts.lineCount(), viewport);
        renderParagraph(buffer, area, paneBlock("File facts", focused),
                Text.from(facts.lines().subList(window.start(), window.end())));
    }

    private static void renderKvModal(Buffer buffer, Rect screenArea, ParquetModel model, ScreenState.Overview state) {
        List<Map.Entry<String, String>> kv = model.facts().keyValueMetadata();
        if (kv.isEmpty()) {
            return;
        }
        int idx = Math.max(0, Math.min(kvIndexForRow(state.kvSelection()), kv.size() - 1));
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
        // The modal is the only scrollable thing on screen while it is open,
        // so its inner height is the stride the key handler should use.
        Keys.observeViewport(viewport);
        int maxScroll = ScrollPane.maxScroll(all.length, viewport);
        int scroll = Math.max(0, Math.min(state.kvModalScroll(), maxScroll));
        int end = Math.min(all.length, scroll + viewport);

        List<Line> lines = new ArrayList<>();
        for (int i = scroll; i < end; i++) {
            lines.add(Line.from(Span.raw(" " + all[i])));
        }
        lines.add(Line.empty());
        String hint = scroll + viewport < all.length
                ? " ↓ " + (all.length - end) + " more lines · Esc / Enter close · "
                        + ScrollPane.hints(all.length, viewport)
                : (scroll > 0
                        ? " ↑ " + scroll + " lines above · Esc / Enter close · "
                                + ScrollPane.hints(all.length, viewport)
                        : " Press Esc or Enter to close");
        lines.add(Line.from(new Span(hint, Theme.dim())));
        Block block = Block.builder()
                .title(" " + entry.getKey() + " ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
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
            String cursor = CursorPane.marker(true, selected, false);
            MenuHint hint = menuHint(item, model);
            Style labelStyle = selected
                    ? Theme.selection()
                    : Theme.primary();
            lines.add(Line.from(
                    new Span(cursor, labelStyle),
                    new Span(padRight(item.label, 20), labelStyle),
                    new Span(hint.value(), Style.EMPTY),
                    new Span(hint.suffix(), Theme.dim())));
        }
        renderParagraph(buffer, area, block, Text.from(lines));
    }

    /// Right-column annotation for a drill-into menu row: the count
    /// `value` (rendered in default fg, e.g. "4 columns"), and an
    /// optional dim `suffix` (e.g. " · browse by column"). Built so
    /// the count reads as a fact while the descriptor sits behind it
    /// at a quieter weight.
    private record MenuHint(String value, String suffix) {
    }

    private static MenuHint menuHint(MenuItem item, ParquetModel model) {
        return switch (item) {
            case SCHEMA -> new MenuHint(
                    padRight(Plurals.format(model.columnCount(), "column", "columns"), AXIS_HINT_WIDTH),
                    " · browse by column");
            case ROW_GROUPS -> new MenuHint(
                    padRight(Plurals.format(model.rowGroupCount(), "group", "groups"), AXIS_HINT_WIDTH),
                    " · browse by row group");
            case FOOTER -> new MenuHint(
                    Sizes.format(FooterScreen.footerAndIndexBytes(model)),
                    "");
            case DATA_PREVIEW -> new MenuHint(
                    Plurals.format(model.facts().totalRows(), "row", "rows"),
                    "");
        };
    }

    /// Width to pad the count+noun fragment so the trailing
    /// "· browse by ..." text lines up across the Schema and Row groups
    /// menu rows regardless of count length.
    private static final int AXIS_HINT_WIDTH = 14;

    /// One fact row. The cursor stops on these as it does on the key/value
    /// entries below them, but `Enter` does nothing here, so they carry the
    /// selection colour without a marker.
    private static Line factsLine(String key, String value, boolean selected) {
        Style keyStyle = selected ? Theme.selection() : Theme.primary();
        return Line.from(
                new Span(CursorPane.marker(false, selected, true), keyStyle),
                new Span(padRight(key, 16), keyStyle),
                new Span(value, selected ? Theme.selection() : Style.EMPTY));
    }

    private static Block paneBlock(String title, boolean focused) {
        Block.Builder b = Block.builder()
                .title(" " + title + " ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED);
        if (!focused) {
            b.borderStyle(Theme.dim());
        }
        return b.build();
    }

    private static void renderParagraph(Buffer buffer, Rect area, Block block, Text text) {
        Paragraph.builder().block(block).text(text).left().build().render(area, buffer);
    }

    private static String padRight(String s, int width) {
        return Strings.padRight(s, width);
    }

    private static String trim(String s, int max) {
        if (s == null) {
            return "";
        }
        return Strings.truncateRight(s, max);
    }
}
