/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.dive.internal;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import dev.hardwood.cli.dive.NavigationStack;
import dev.hardwood.cli.dive.ParquetModel;
import dev.hardwood.cli.dive.ScreenState;
import dev.hardwood.internal.reader.Dictionary;
import dev.tamboui.buffer.Buffer;
import dev.tamboui.layout.Constraint;
import dev.tamboui.layout.Layout;
import dev.tamboui.layout.Rect;
import dev.tamboui.style.Color;
import dev.tamboui.style.Style;
import dev.tamboui.text.Line;
import dev.tamboui.text.Span;
import dev.tamboui.text.Text;
import dev.tamboui.tui.event.KeyCode;
import dev.tamboui.tui.event.KeyEvent;
import dev.tamboui.widgets.block.Block;
import dev.tamboui.widgets.block.BorderType;
import dev.tamboui.widgets.block.Borders;
import dev.tamboui.widgets.paragraph.Paragraph;
import dev.tamboui.widgets.table.Row;
import dev.tamboui.widgets.table.Table;
import dev.tamboui.widgets.table.TableState;

/// Dictionary entries for one column chunk. Supports:
///
/// - Enter: open a modal with the full un-truncated value.
/// - `/`: enter inline search mode. Typed characters extend the filter,
///   Backspace trims, Enter commits (keep filter), Esc clears filter.
/// - Up/Down: move selection within the filtered view.
public final class DictionaryScreen {

    private static final int VALUE_PREVIEW_MAX = 60;

    private DictionaryScreen() {
    }

    /// Used by [DiveApp] to decide whether the screen should receive printable
    /// chars instead of the global keymap (e.g. `g`, `q`).
    public static boolean isInInputMode(ScreenState.DictionaryView state) {
        return state.searching();
    }

    public static boolean handle(KeyEvent event, ParquetModel model, NavigationStack stack) {
        ScreenState.DictionaryView state = (ScreenState.DictionaryView) stack.top();
        if (state.searching()) {
            return handleSearching(event, state, stack);
        }
        if (state.modalOpen()) {
            if (event.isCancel() || event.isConfirm()) {
                stack.replaceTop(with(state, state.selection(), false, state.filter(), false));
                return true;
            }
            return false;
        }
        if (event.code() == KeyCode.CHAR && event.character() == '/') {
            stack.replaceTop(with(state, 0, false, state.filter(), true));
            return true;
        }
        Dictionary dict = model.dictionary(state.rowGroupIndex(), state.columnIndex());
        List<Integer> filtered = filteredIndices(dict, state.filter());
        if (event.isUp()) {
            stack.replaceTop(with(state, Math.max(0, state.selection() - 1), false, state.filter(), false));
            return true;
        }
        if (event.isDown()) {
            int max = filtered.isEmpty() ? 0 : filtered.size() - 1;
            stack.replaceTop(with(state, Math.min(max, state.selection() + 1), false, state.filter(), false));
            return true;
        }
        if (event.isConfirm() && !filtered.isEmpty()) {
            stack.replaceTop(with(state, state.selection(), true, state.filter(), false));
            return true;
        }
        return false;
    }

    private static boolean handleSearching(KeyEvent event, ScreenState.DictionaryView state, NavigationStack stack) {
        if (event.isCancel()) {
            stack.replaceTop(with(state, 0, false, "", false));
            return true;
        }
        if (event.isConfirm()) {
            stack.replaceTop(with(state, 0, false, state.filter(), false));
            return true;
        }
        if (event.isDeleteBackward()) {
            String f = state.filter();
            String next = f.isEmpty() ? f : f.substring(0, f.length() - 1);
            stack.replaceTop(with(state, 0, false, next, true));
            return true;
        }
        if (event.code() == KeyCode.CHAR) {
            char c = event.character();
            if (c >= ' ' && c != 127) {
                stack.replaceTop(with(state, 0, false, state.filter() + c, true));
                return true;
            }
        }
        return false;
    }

    public static void render(Buffer buffer, Rect area, ParquetModel model, ScreenState.DictionaryView state) {
        Dictionary dict = model.dictionary(state.rowGroupIndex(), state.columnIndex());
        if (dict == null) {
            renderEmpty(buffer, area);
            return;
        }

        List<Integer> filtered = filteredIndices(dict, state.filter());
        List<Rect> split = Layout.vertical()
                .constraints(new Constraint.Length(1), new Constraint.Fill(1))
                .split(area);

        renderSearchBar(buffer, split.get(0), state, dict.size(), filtered.size());

        List<Row> rows = new ArrayList<>();
        for (int idx : filtered) {
            rows.add(Row.from(
                    "[" + idx + "]",
                    formatValue(dict, idx, VALUE_PREVIEW_MAX)));
        }
        Row header = Row.from("#", "Value").style(Style.EMPTY.bold());
        Block block = Block.builder()
                .title(" Dictionary (" + dict.size() + " entries"
                        + (state.filter().isEmpty() ? "" : "; " + filtered.size() + " matching") + ") ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .borderColor(Color.CYAN)
                .build();
        Table table = Table.builder()
                .header(header)
                .rows(rows)
                .widths(new Constraint.Length(8), new Constraint.Fill(1))
                .columnSpacing(2)
                .block(block)
                .highlightSymbol("▶ ")
                .highlightStyle(Style.EMPTY.bold())
                .build();
        TableState tableState = new TableState();
        if (!filtered.isEmpty()) {
            tableState.select(Math.min(state.selection(), filtered.size() - 1));
        }
        table.render(split.get(1), buffer, tableState);

        if (state.modalOpen() && !filtered.isEmpty()) {
            int dictIdx = filtered.get(Math.min(state.selection(), filtered.size() - 1));
            renderValueModal(buffer, area, dict, dictIdx);
        }
    }

    public static String keybarKeys() {
        return "[↑↓] move  [Enter] full value  [/] search  [Esc] back";
    }

    private static void renderSearchBar(Buffer buffer, Rect area, ScreenState.DictionaryView state,
                                        int totalSize, int filteredSize) {
        if (!state.searching() && state.filter().isEmpty()) {
            Paragraph.builder()
                    .text(Text.from(Line.from(new Span(
                            " " + totalSize + " entries. Press / to filter.", Style.EMPTY.fg(Color.GRAY)))))
                    .left()
                    .build()
                    .render(area, buffer);
            return;
        }
        String cursor = state.searching() ? "█" : "";
        Line line = Line.from(
                new Span(" / ", Style.EMPTY.fg(Color.CYAN).bold()),
                new Span(state.filter() + cursor, Style.EMPTY.bold()),
                new Span("  (" + filteredSize + " / " + totalSize + ")", Style.EMPTY.fg(Color.GRAY)));
        Paragraph.builder().text(Text.from(line)).left().build().render(area, buffer);
    }

    private static List<Integer> filteredIndices(Dictionary dict, String filter) {
        List<Integer> out = new ArrayList<>();
        if (dict == null) {
            return out;
        }
        String needle = filter.toLowerCase();
        for (int i = 0; i < dict.size(); i++) {
            if (needle.isEmpty() || fullValue(dict, i).toLowerCase().contains(needle)) {
                out.add(i);
            }
        }
        return out;
    }

    private static ScreenState.DictionaryView with(ScreenState.DictionaryView state,
                                                    int selection, boolean modalOpen, String filter, boolean searching) {
        return new ScreenState.DictionaryView(
                state.rowGroupIndex(), state.columnIndex(), selection, modalOpen, filter, searching);
    }

    private static String formatValue(Dictionary dict, int index, int max) {
        String full = fullValue(dict, index);
        if (full.length() <= max) {
            return full;
        }
        return full.substring(0, max - 1) + "…";
    }

    private static String fullValue(Dictionary dict, int index) {
        return switch (dict) {
            case Dictionary.IntDictionary d -> Integer.toString(d.values()[index]);
            case Dictionary.LongDictionary d -> Long.toString(d.values()[index]);
            case Dictionary.FloatDictionary d -> Float.toString(d.values()[index]);
            case Dictionary.DoubleDictionary d -> Double.toString(d.values()[index]);
            case Dictionary.ByteArrayDictionary d -> formatBytes(d.values()[index]);
        };
    }

    private static String formatBytes(byte[] bytes) {
        if (bytes == null) {
            return "null";
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static void renderEmpty(Buffer buffer, Rect area) {
        Block block = Block.builder()
                .title(" Dictionary ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .borderColor(Color.GRAY)
                .build();
        Paragraph.builder()
                .block(block)
                .text(Text.from(Line.from(new Span(
                        " This chunk is not dictionary-encoded.",
                        Style.EMPTY.fg(Color.GRAY)))))
                .left()
                .build()
                .render(area, buffer);
    }

    private static void renderValueModal(Buffer buffer, Rect screenArea, Dictionary dict, int index) {
        int width = Math.min(80, screenArea.width() - 4);
        int height = Math.min(16, screenArea.height() - 2);
        int x = screenArea.left() + (screenArea.width() - width) / 2;
        int y = screenArea.top() + (screenArea.height() - height) / 2;
        Rect area = new Rect(x, y, width, height);

        String full = fullValue(dict, index);
        List<Line> lines = new ArrayList<>();
        lines.add(Line.empty());
        lines.add(Line.from(Span.raw(" " + full)));
        lines.add(Line.empty());
        lines.add(Line.from(new Span(" Press Esc or Enter to close", Style.EMPTY.fg(Color.GRAY))));

        Block block = Block.builder()
                .title(" Entry #" + index + " ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .borderColor(Color.CYAN)
                .build();
        Paragraph.builder().block(block).text(Text.from(lines)).left().build().render(area, buffer);
    }
}
