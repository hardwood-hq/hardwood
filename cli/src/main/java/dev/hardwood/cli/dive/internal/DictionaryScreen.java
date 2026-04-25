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

import dev.hardwood.cli.dive.NavigationStack;
import dev.hardwood.cli.dive.ParquetModel;
import dev.hardwood.cli.dive.ScreenState;
import dev.hardwood.cli.internal.RowValueFormatter;
import dev.hardwood.internal.reader.Dictionary;
import dev.hardwood.schema.ColumnSchema;
import dev.tamboui.buffer.Buffer;
import dev.tamboui.layout.Constraint;
import dev.tamboui.layout.Layout;
import dev.tamboui.layout.Rect;
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

    /// One-slot memoisation for filteredIndices. A dictionary with hundreds of
    /// thousands of entries re-filters twice per navigation keystroke (once in
    /// handle, once in render); caching the most recent (dict, filter) result
    /// turns navigation into O(1) lookups while still invalidating when the
    /// user types / deletes a character.
    private static Dictionary cachedDict;
    private static String cachedFilter;
    private static List<Integer> cachedFiltered;

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
        if (needsConfirmation(model, state)) {
            // Confirm-to-load prompt: Enter opts in, Esc falls through to the
            // default DiveApp.Esc-pops-back behaviour.
            if (event.isConfirm()) {
                stack.replaceTop(withConfirmed(state, true));
                return true;
            }
            return false;
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
        Dictionary dict = loadDictionary(model, state);
        ColumnSchema col = model.schema().getColumn(state.columnIndex());
        List<Integer> filtered = filteredIndices(dict, col, state.filter());
        // Plain Up/Down step one entry; PgDn/PgUp (and the Shift+↓/↑ Mac
        // chord since most macOS laptops have no dedicated PgDn/PgUp keys)
        // move Keys.viewportStride() entries via the shared helper.
        if (Keys.isPageDown(event)) {
            int max = filtered.isEmpty() ? 0 : filtered.size() - 1;
            stack.replaceTop(with(state, Math.min(max, state.selection() + Keys.viewportStride()),
                    false, state.filter(), false));
            return true;
        }
        if (Keys.isPageUp(event)) {
            stack.replaceTop(with(state, Math.max(0, state.selection() - Keys.viewportStride()),
                    false, state.filter(), false));
            return true;
        }
        if (Keys.isStepUp(event)) {
            stack.replaceTop(with(state, Math.max(0, state.selection() - 1), false, state.filter(), false));
            return true;
        }
        if (Keys.isStepDown(event)) {
            int max = filtered.isEmpty() ? 0 : filtered.size() - 1;
            stack.replaceTop(with(state, Math.min(max, state.selection() + 1), false, state.filter(), false));
            return true;
        }
        if (Keys.isJumpTop(event) && !filtered.isEmpty()) {
            stack.replaceTop(with(state, 0, false, state.filter(), false));
            return true;
        }
        if (Keys.isJumpBottom(event) && !filtered.isEmpty()) {
            stack.replaceTop(with(state, filtered.size() - 1, false, state.filter(), false));
            return true;
        }
        if (event.isConfirm() && !filtered.isEmpty()) {
            // Only open the modal if the displayed value was actually truncated.
            // For numeric dictionaries like VendorID=1, the row already shows the
            // full value, so a modal would just redraw the same character in a
            // bigger frame.
            int idx = filtered.get(Math.min(state.selection(), filtered.size() - 1));
            String full = fullValue(dict, idx, col);
            if (full.length() <= VALUE_PREVIEW_MAX) {
                return false;
            }
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
        // Search bar + block borders + header = 4 chrome rows.
        Keys.observeViewport(area.height() - 4);
        if (needsConfirmation(model, state)) {
            renderConfirmPrompt(buffer, area, model, state);
            return;
        }
        Dictionary dict = loadDictionary(model, state);
        if (dict == null) {
            renderEmpty(buffer, area);
            return;
        }

        ColumnSchema col = model.schema().getColumn(state.columnIndex());
        List<Integer> filtered = filteredIndices(dict, col, state.filter());
        List<Rect> split = Layout.vertical()
                .constraints(new Constraint.Length(1), new Constraint.Fill(1))
                .split(area);

        renderSearchBar(buffer, split.get(0), state, dict.size(), filtered.size());

        List<Row> rows = new ArrayList<>();
        for (int idx : filtered) {
            rows.add(Row.from(
                    "[" + idx + "]",
                    formatValue(dict, idx, col, VALUE_PREVIEW_MAX)));
        }
        Row header = Row.from("#", "Value").style(Style.EMPTY.bold());
        Block block = Block.builder()
                .title(" Dictionary (" + Plurals.format(dict.size(), "entry", "entries")
                        + (state.filter().isEmpty() ? "" : "; " + filtered.size() + " matching") + ") ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .borderColor(Theme.ACCENT)
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
            renderValueModal(buffer, area, dict, col, dictIdx);
        }
    }

    public static String keybarKeys(ScreenState.DictionaryView state, ParquetModel model) {
        Dictionary dict = needsConfirmation(model, state) ? null : loadDictionary(model, state);
        ColumnSchema col = model.schema().getColumn(state.columnIndex());
        java.util.List<Integer> filtered = dict == null ? java.util.List.of()
                : filteredIndices(dict, col, state.filter());
        int count = filtered.size();
        // Enter opens the full-value modal only when the selected entry
        // would actually reveal more than the row already shows.
        boolean canExpand = false;
        if (dict != null && count > 0) {
            int idx = filtered.get(Math.min(state.selection(), count - 1));
            canExpand = fullValue(dict, idx, col).length() > VALUE_PREVIEW_MAX;
        }
        return new Keys.Hints()
                .add(count > 1, "[↑↓] move")
                .add(count > Keys.viewportStride(), "[PgDn/PgUp or Shift+↓↑] page")
                .add(canExpand, "[Enter] full value")
                .add(dict != null, "[/] search")
                .add(true, "[Esc] back")
                .build();
    }

    private static void renderSearchBar(Buffer buffer, Rect area, ScreenState.DictionaryView state,
                                        int totalSize, int filteredSize) {
        if (!state.searching() && state.filter().isEmpty()) {
            Paragraph.builder()
                    .text(Text.from(Line.from(new Span(
                            " " + Plurals.format(totalSize, "entry", "entries") + ". Press / to filter.",
                            Style.EMPTY.fg(Theme.DIM)))))
                    .left()
                    .build()
                    .render(area, buffer);
            return;
        }
        String cursor = state.searching() ? "█" : "";
        Line line = Line.from(
                new Span(" / ", Style.EMPTY.fg(Theme.ACCENT).bold()),
                new Span(state.filter() + cursor, Style.EMPTY.bold()),
                new Span("  (" + String.format("%,d", filteredSize) + " / "
                        + Plurals.format(totalSize, "entry", "entries") + ")", Style.EMPTY.fg(Theme.DIM)));
        Paragraph.builder().text(Text.from(line)).left().build().render(area, buffer);
    }

    private static List<Integer> filteredIndices(Dictionary dict, ColumnSchema col, String filter) {
        if (dict == null) {
            return List.of();
        }
        if (dict == cachedDict && filter.equals(cachedFilter)) {
            return cachedFiltered;
        }
        List<Integer> out = new ArrayList<>();
        String needle = filter.toLowerCase();
        for (int i = 0; i < dict.size(); i++) {
            if (needle.isEmpty() || fullValue(dict, i, col).toLowerCase().contains(needle)) {
                out.add(i);
            }
        }
        List<Integer> result = List.copyOf(out);
        cachedDict = dict;
        cachedFilter = filter;
        cachedFiltered = result;
        return result;
    }

    private static ScreenState.DictionaryView with(ScreenState.DictionaryView state,
                                                    int selection, boolean modalOpen, String filter, boolean searching) {
        return new ScreenState.DictionaryView(
                state.rowGroupIndex(), state.columnIndex(), selection, modalOpen, filter, searching,
                state.loadConfirmed());
    }

    private static ScreenState.DictionaryView withConfirmed(ScreenState.DictionaryView state, boolean confirmed) {
        return new ScreenState.DictionaryView(
                state.rowGroupIndex(), state.columnIndex(), state.selection(), state.modalOpen(),
                state.filter(), state.searching(), confirmed);
    }

    private static boolean needsConfirmation(ParquetModel model, ScreenState.DictionaryView state) {
        return !state.loadConfirmed()
                && model.dictionaryChunkBytes(state.rowGroupIndex(), state.columnIndex())
                > model.dictionaryReadCapBytes();
    }

    private static Dictionary loadDictionary(ParquetModel model, ScreenState.DictionaryView state) {
        return state.loadConfirmed()
                ? model.dictionaryForced(state.rowGroupIndex(), state.columnIndex())
                : model.dictionary(state.rowGroupIndex(), state.columnIndex());
    }

    private static String formatValue(Dictionary dict, int index, ColumnSchema col, int max) {
        String full = fullValue(dict, index, col);
        if (full.length() <= max) {
            return full;
        }
        return full.substring(0, max - 1) + "…";
    }

    private static String fullValue(Dictionary dict, int index, ColumnSchema col) {
        Object raw = switch (dict) {
            case Dictionary.IntDictionary d -> d.values()[index];
            case Dictionary.LongDictionary d -> d.values()[index];
            case Dictionary.FloatDictionary d -> d.values()[index];
            case Dictionary.DoubleDictionary d -> d.values()[index];
            case Dictionary.ByteArrayDictionary d -> d.values()[index];
        };
        return RowValueFormatter.formatDictionaryValue(raw, col);
    }

    private static void renderConfirmPrompt(Buffer buffer, Rect area, ParquetModel model,
                                            ScreenState.DictionaryView state) {
        long chunkBytes = model.dictionaryChunkBytes(state.rowGroupIndex(), state.columnIndex());
        int capBytes = model.dictionaryReadCapBytes();
        List<Line> lines = new ArrayList<>();
        lines.add(Line.empty());
        lines.add(Line.from(Span.raw(" This chunk would need to load "
                + dev.hardwood.cli.internal.Sizes.format(chunkBytes)
                + " of dictionary bytes,")));
        lines.add(Line.from(Span.raw(" exceeding the current "
                + dev.hardwood.cli.internal.Sizes.format(capBytes) + " cap.")));
        lines.add(Line.empty());
        lines.add(Line.from(Span.raw(" Press Enter to load anyway,  Esc to cancel.")));
        lines.add(Line.empty());
        lines.add(Line.from(new Span(
                " (raise the cap with --max-dict-bytes on the next launch)",
                Style.EMPTY.fg(Theme.DIM))));
        Block block = Block.builder()
                .title(" Dictionary — load confirmation ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .borderColor(Theme.ACCENT)
                .build();
        Paragraph.builder().block(block).text(Text.from(lines)).left().build().render(area, buffer);
    }

    private static void renderEmpty(Buffer buffer, Rect area) {
        Block block = Block.builder()
                .title(" Dictionary ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .borderColor(Theme.DIM)
                .build();
        Paragraph.builder()
                .block(block)
                .text(Text.from(Line.from(new Span(
                        " This chunk is not dictionary-encoded.",
                        Style.EMPTY.fg(Theme.DIM)))))
                .left()
                .build()
                .render(area, buffer);
    }

    private static void renderValueModal(Buffer buffer, Rect screenArea, Dictionary dict, ColumnSchema col, int index) {
        int width = Math.min(80, screenArea.width() - 4);
        int height = Math.min(16, screenArea.height() - 2);
        int x = screenArea.left() + (screenArea.width() - width) / 2;
        int y = screenArea.top() + (screenArea.height() - height) / 2;
        Rect area = new Rect(x, y, width, height);
        // Wipe the area so the underlying dictionary table doesn't bleed through
        // cells that the Paragraph doesn't paint.
        dev.tamboui.widgets.Clear.INSTANCE.render(area, buffer);

        String full = fullValue(dict, index, col);
        List<Line> lines = new ArrayList<>();
        lines.add(Line.empty());
        lines.add(Line.from(Span.raw(" " + full)));
        lines.add(Line.empty());
        lines.add(Line.from(new Span(" Press Esc or Enter to close", Style.EMPTY.fg(Theme.DIM))));

        Block block = Block.builder()
                .title(" Entry #" + index + " ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .borderColor(Theme.ACCENT)
                .build();
        Paragraph.builder().block(block).text(Text.from(lines)).left().build().render(area, buffer);
    }
}
