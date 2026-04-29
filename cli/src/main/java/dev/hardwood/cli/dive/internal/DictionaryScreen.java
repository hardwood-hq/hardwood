/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.dive.internal;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import dev.hardwood.cli.dive.NavigationStack;
import dev.hardwood.cli.dive.ParquetModel;
import dev.hardwood.cli.dive.ScreenState;
import dev.hardwood.cli.internal.Fmt;
import dev.hardwood.cli.internal.RowValueFormatter;
import dev.hardwood.internal.reader.Dictionary;
import dev.hardwood.schema.ColumnSchema;
import dev.tamboui.buffer.Buffer;
import dev.tamboui.layout.Constraint;
import dev.tamboui.layout.Layout;
import dev.tamboui.layout.Rect;
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
    /// user types / deletes a character. The dictionary is held via
    /// `WeakReference` so an evicted entry from the model's bounded
    /// `dictionaryCache` is not pinned for the lifetime of the JVM.
    private static WeakReference<Dictionary> cachedDictRef;
    private static String cachedFilter;
    private static boolean cachedLogical;
    private static int cachedSize;
    private static int[] cachedFiltered;

    /// Result of filtering a dictionary by the user's `/ filter` string. When
    /// `indices == null` the view is the identity mapping `[0, size)` — used
    /// for the (very common) empty-filter case so the cache hit path
    /// allocates nothing per entry. Otherwise `indices[i]` is the dictionary
    /// index of the `i`-th visible entry.
    private record FilterIndex(int size, int[] indices) {
        boolean isEmpty() {
            return size == 0;
        }
        int at(int i) {
            return indices == null ? i : indices[i];
        }
    }

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
        // `t` toggles logical-type rendering at any time, including while
        // the full-value modal is open. The modal value re-renders with
        // the new flag.
        if (event.code() == KeyCode.CHAR && event.character() == 't'
                && !event.hasCtrl() && !event.hasAlt()) {
            stack.replaceTop(withLogical(state, !state.logicalTypes()));
            return true;
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
        FilterIndex filtered = filteredIndices(dict, col, state.filter(), state.logicalTypes());
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
            int idx = filtered.at(Math.min(state.selection(), filtered.size() - 1));
            String full = fullValue(dict, idx, col, state.logicalTypes());
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
        FilterIndex filtered = filteredIndices(dict, col, state.filter(), state.logicalTypes());
        List<Rect> split = Layout.vertical()
                .constraints(new Constraint.Length(1), new Constraint.Fill(1))
                .split(area);

        renderSearchBar(buffer, split.get(0), state, dict.size(), filtered.size());

        // Build Row objects only for the visible window — see RowWindow for
        // why list screens must do this.
        int total = filtered.size();
        RowWindow window = RowWindow.bottomPinned(state.selection(), total, area.height() - 4);
        List<Row> rows = new ArrayList<>(window.size());
        for (int i = window.start(); i < window.end(); i++) {
            int idx = filtered.at(i);
            rows.add(Row.from(
                    "[" + idx + "]",
                    formatValue(dict, idx, col, VALUE_PREVIEW_MAX, state.logicalTypes())));
        }
        Row header = Row.from("#", "Value").style(Theme.accent().bold());
        String typeMode = state.logicalTypes() ? "" : " · physical";
        Block block = Block.builder()
                .title(" Dictionary entries "
                        + Plurals.rangeOf(state.selection(), total, Keys.viewportStride())
                        + (state.filter().isEmpty()
                                ? ""
                                : " · " + Plurals.format(dict.size(), "entry", "entries") + " total")
                        + typeMode + " ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .build();
        Table table = Table.builder()
                .header(header)
                .rows(rows)
                .widths(new Constraint.Length(8), new Constraint.Fill(1))
                .columnSpacing(2)
                .block(block)
                .highlightSymbol("▶ ")
                .highlightStyle(Theme.selection())
                .build();
        TableState tableState = new TableState();
        if (total > 0) {
            tableState.select(window.selectionInWindow());
        }
        table.render(split.get(1), buffer, tableState);

        if (state.modalOpen() && !filtered.isEmpty()) {
            int dictIdx = filtered.at(Math.min(state.selection(), filtered.size() - 1));
            buffer.setStyle(area, Theme.dim());
            renderValueModal(buffer, area, dict, col, dictIdx, state.logicalTypes());
        }
    }

    public static String keybarKeys(ScreenState.DictionaryView state, ParquetModel model) {
        Dictionary dict = needsConfirmation(model, state) ? null : loadDictionary(model, state);
        ColumnSchema col = model.schema().getColumn(state.columnIndex());
        FilterIndex filtered = dict == null ? new FilterIndex(0, null)
                : filteredIndices(dict, col, state.filter(), state.logicalTypes());
        int count = filtered.size();
        boolean canExpand = false;
        if (dict != null && count > 0) {
            int idx = filtered.at(Math.min(state.selection(), count - 1));
            canExpand = fullValue(dict, idx, col, state.logicalTypes()).length() > VALUE_PREVIEW_MAX;
        }
        boolean hasLogical = col.logicalType() != null;
        return new Keys.Hints()
                .add(count > 1, "[↑↓] move")
                .add(count > Keys.viewportStride(), "[PgDn/PgUp or Shift+↓↑] page")
                .add(count > 1, "[g/G] first/last")
                .add(canExpand, "[Enter] view full value")
                .add(dict != null, "[/] search")
                .add(hasLogical, "[t] logical types")
                .add(true, "[Esc] back")
                .build();
    }

    private static void renderSearchBar(Buffer buffer, Rect area, ScreenState.DictionaryView state,
                                        int totalSize, int filteredSize) {
        if (!state.searching() && state.filter().isEmpty()) {
            Paragraph.builder()
                    .text(Text.from(Line.from(new Span(
                            " " + Plurals.format(totalSize, "entry", "entries") + ". Press / to filter.",
                            Theme.dim()))))
                    .left()
                    .build()
                    .render(area, buffer);
            return;
        }
        String cursor = state.searching() ? "█" : "";
        Line line = Line.from(
                new Span(" / ", Theme.primary()),
                new Span(state.filter() + cursor, Theme.primary()),
                new Span("  (" + Fmt.fmt("%,d", filteredSize) + " / "
                        + Plurals.format(totalSize, "entry", "entries") + ")", Theme.dim()));
        Paragraph.builder().text(Text.from(line)).left().build().render(area, buffer);
    }

    private static FilterIndex filteredIndices(Dictionary dict, ColumnSchema col, String filter,
                                                  boolean useLogicalType) {
        if (dict == null) {
            return new FilterIndex(0, null);
        }
        Dictionary cached = cachedDictRef == null ? null : cachedDictRef.get();
        if (dict == cached && filter.equals(cachedFilter) && useLogicalType == cachedLogical) {
            return new FilterIndex(cachedSize, cachedFiltered);
        }
        int n = dict.size();
        int size;
        int[] indices;
        if (filter.isEmpty()) {
            size = n;
            indices = null;
        } else {
            String needle = filter.toLowerCase(Locale.ROOT);
            int[] tmp = new int[n];
            int k = 0;
            for (int i = 0; i < n; i++) {
                if (fullValue(dict, i, col, useLogicalType).toLowerCase(Locale.ROOT).contains(needle)) {
                    tmp[k++] = i;
                }
            }
            size = k;
            indices = (k == n) ? null : Arrays.copyOf(tmp, k);
        }
        cachedDictRef = new WeakReference<>(dict);
        cachedFilter = filter;
        cachedLogical = useLogicalType;
        cachedSize = size;
        cachedFiltered = indices;
        return new FilterIndex(size, indices);
    }

    private static ScreenState.DictionaryView with(ScreenState.DictionaryView state,
                                                    int selection, boolean modalOpen, String filter, boolean searching) {
        return new ScreenState.DictionaryView(
                state.rowGroupIndex(), state.columnIndex(), selection, modalOpen, filter, searching,
                state.loadConfirmed(), state.logicalTypes());
    }

    private static ScreenState.DictionaryView withConfirmed(ScreenState.DictionaryView state, boolean confirmed) {
        return new ScreenState.DictionaryView(
                state.rowGroupIndex(), state.columnIndex(), state.selection(), state.modalOpen(),
                state.filter(), state.searching(), confirmed, state.logicalTypes());
    }

    private static ScreenState.DictionaryView withLogical(ScreenState.DictionaryView state, boolean logical) {
        return new ScreenState.DictionaryView(
                state.rowGroupIndex(), state.columnIndex(), state.selection(), state.modalOpen(),
                state.filter(), state.searching(), state.loadConfirmed(), logical);
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

    private static String formatValue(Dictionary dict, int index, ColumnSchema col, int max,
                                      boolean useLogicalType) {
        String full = fullValue(dict, index, col, useLogicalType);
        if (full.length() <= max) {
            return full;
        }
        return full.substring(0, max - 1) + "…";
    }

    private static String fullValue(Dictionary dict, int index, ColumnSchema col,
                                    boolean useLogicalType) {
        Object raw = switch (dict) {
            case Dictionary.IntDictionary d -> d.values()[index];
            case Dictionary.LongDictionary d -> d.values()[index];
            case Dictionary.FloatDictionary d -> d.values()[index];
            case Dictionary.DoubleDictionary d -> d.values()[index];
            case Dictionary.ByteArrayDictionary d -> d.values()[index];
        };
        return RowValueFormatter.formatDictionaryValue(raw, col, useLogicalType);
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
                Theme.dim())));
        Block block = Block.builder()
                .title(" Dictionary — load confirmation ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .build();
        Paragraph.builder().block(block).text(Text.from(lines)).left().build().render(area, buffer);
    }

    private static void renderEmpty(Buffer buffer, Rect area) {
        Block block = Block.builder()
                .title(" Dictionary ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .build();
        Paragraph.builder()
                .block(block)
                .text(Text.from(Line.from(new Span(
                        " This chunk is not dictionary-encoded.",
                        Theme.dim()))))
                .left()
                .build()
                .render(area, buffer);
    }

    private static void renderValueModal(Buffer buffer, Rect screenArea, Dictionary dict, ColumnSchema col,
                                         int index, boolean useLogicalType) {
        int width = Math.min(80, screenArea.width() - 4);
        int height = Math.min(16, screenArea.height() - 2);
        int x = screenArea.left() + (screenArea.width() - width) / 2;
        int y = screenArea.top() + (screenArea.height() - height) / 2;
        Rect area = new Rect(x, y, width, height);
        dev.tamboui.widgets.Clear.INSTANCE.render(area, buffer);

        String full = fullValue(dict, index, col, useLogicalType);
        List<Line> lines = new ArrayList<>();
        lines.add(Line.empty());
        lines.add(Line.from(Span.raw(" " + full)));
        lines.add(Line.empty());
        boolean hasLogical = col.logicalType() != null;
        String hint = " Esc / Enter close" + (hasLogical ? " · t logical types" : "");
        lines.add(Line.from(new Span(hint, Theme.dim())));

        Block block = Block.builder()
                .title(" Entry #" + index + " ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .build();
        Paragraph.builder().block(block).text(Text.from(lines)).left().build().render(area, buffer);
    }
}
