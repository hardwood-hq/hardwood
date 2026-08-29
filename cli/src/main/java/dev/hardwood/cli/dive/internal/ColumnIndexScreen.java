/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.dive.internal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import dev.hardwood.cli.dive.NavigationStack;
import dev.hardwood.cli.dive.ParquetModel;
import dev.hardwood.cli.dive.ScreenState;
import dev.hardwood.cli.internal.Fmt;
import dev.hardwood.cli.internal.Strings;
import dev.hardwood.cli.internal.ValueFormatter;
import dev.hardwood.metadata.ColumnIndex;
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

/// Per-page statistics for one column chunk: null_pages, null counts, min, max.
/// Boundary order is shown above the table.
///
/// `/` enters inline search: the filter matches against each page's formatted
/// min or max value (case-insensitive substring).
public final class ColumnIndexScreen {

    /// The fixed-width columns, in display order. Header label and cell width
    /// travel together so they cannot drift, and the widths feed [#statBudget]
    /// rather than being restated there as literals.
    private enum Col {
        NUM("  #", 7),
        NULL_PAGE("Null page", 10),
        NULLS("Nulls", 10);

        private final String label;
        private final int width;

        Col(String label, int width) {
            this.label = label;
            this.width = width;
        }
    }

    /// Cached so rendering a frame does not clone the constant pool array.
    private static final Col[] COLUMNS = Col.values();
    /// Min and Max. They fill the width the fixed columns leave, so they are
    /// not part of [Col].
    private static final String[] STAT_LABELS = {"Min", "Max"};
    private static final int COLUMN_SPACING = 2;
    private static final int TABLE_BORDER_WIDTH = 2;

    private ColumnIndexScreen() {
    }

    /// Used by [DiveApp] to decide whether the screen should receive printable
    /// chars instead of the global keymap.
    public static boolean isInInputMode(ScreenState.ColumnIndexView state) {
        return state.searching();
    }

    public static boolean handle(KeyEvent event, ParquetModel model, NavigationStack stack) {
        ScreenState.ColumnIndexView state = (ScreenState.ColumnIndexView) stack.top();
        // `t` toggles logical-type rendering at any time, including while
        // the Min/Max modal is open. The modal renders its values via the
        // same logical/physical flag, so the toggle has visible effect
        // without needing to close first.
        if (event.code() == KeyCode.CHAR && event.character() == 't'
                && !event.hasCtrl() && !event.hasAlt()) {
            stack.replaceTop(new ScreenState.ColumnIndexView(
                    state.rowGroupIndex(), state.columnIndex(), state.selection(),
                    state.filter(), false, !state.logicalTypes(), state.modalOpen(),
                    state.scrollTop()));
            return true;
        }
        if (state.modalOpen()) {
            if (event.isCancel() || event.isConfirm()) {
                stack.replaceTop(withModal(state, false));
                return true;
            }
            int next = ScrollPane.scroll(event, state.modalScroll(),
                    modalLineCount(model, state), Keys.viewportStride());
            if (next == ScrollPane.UNHANDLED) {
                return false;
            }
            if (next != state.modalScroll()) {
                stack.replaceTop(new ScreenState.ColumnIndexView(
                        state.rowGroupIndex(), state.columnIndex(), state.selection(),
                        state.filter(), state.searching(), state.logicalTypes(),
                        state.modalOpen(), state.scrollTop(), next));
            }
            return true;
        }
        if (state.searching()) {
            return handleSearching(event, state, stack);
        }
        ColumnIndex ci = model.columnIndex(state.rowGroupIndex(), state.columnIndex());
        if (ci == null) {
            return false;
        }
        if (event.code() == KeyCode.CHAR && event.character() == '/') {
            stack.replaceTop(with(state, 0, state.filter(), true));
            return true;
        }
        ColumnSchema col = model.schema().getColumn(state.columnIndex());
        List<Integer> filtered = filteredPages(ci, col, state.filter());
        int selected = CursorPane.select(event, state.selection(), filtered.size());
        if (selected != CursorPane.UNHANDLED) {
            stack.replaceTop(with(state, selected, state.filter(), false));
            return true;
        }
        // Open the full Min/Max modal on Enter only when the cell withheld
        // something the modal would reveal — the same gate the `▶` marker and
        // the keybar hint read.
        if (event.isConfirm() && !filtered.isEmpty()) {
            int idx = filtered.get(Math.min(state.selection(), filtered.size() - 1));
            if (isExpandable(ci, idx, col, state.logicalTypes(), statBudget(Keys.viewportWidth()))) {
                stack.replaceTop(withModal(state, true));
                return true;
            }
        }
        return false;
    }

    private static boolean handleSearching(KeyEvent event, ScreenState.ColumnIndexView state,
                                           NavigationStack stack) {
        if (event.isCancel()) {
            stack.replaceTop(with(state, 0, "", false));
            return true;
        }
        if (event.isConfirm()) {
            stack.replaceTop(with(state, 0, state.filter(), false));
            return true;
        }
        if (event.isDeleteBackward()) {
            String f = state.filter();
            String next = f.isEmpty() ? f : f.substring(0, f.length() - 1);
            stack.replaceTop(with(state, 0, next, true));
            return true;
        }
        if (event.code() == KeyCode.CHAR) {
            char c = event.character();
            if (c >= ' ' && c != 127) {
                stack.replaceTop(with(state, 0, state.filter() + c, true));
                return true;
            }
        }
        return false;
    }

    public static void render(Buffer buffer, Rect area, ParquetModel model, ScreenState.ColumnIndexView state) {
        // Boundary-order line + search bar + block borders + header = 5 chrome rows.
        Keys.observeViewport(area.height() - 5);
        // The Enter gate and the keybar hint compute the same budget as the
        // cells, and they run outside the render pass.
        Keys.observeViewportWidth(area.width());
        ColumnIndex ci = model.columnIndex(state.rowGroupIndex(), state.columnIndex());
        if (ci == null) {
            renderEmpty(buffer, area, "No column index for this chunk.");
            return;
        }
        ColumnSchema col = model.schema().getColumn(state.columnIndex());
        List<Integer> filtered = filteredPages(ci, col, state.filter());

        List<Rect> split = Layout.vertical()
                .constraints(
                        new Constraint.Length(1),
                        new Constraint.Length(1),
                        new Constraint.Fill(1))
                .split(area);

        Paragraph.builder()
                .text(Text.from(Line.from(
                        new Span(" Boundary order: ", Theme.primary()),
                        Span.raw(ci.boundaryOrder().name()))))
                .left()
                .build()
                .render(split.get(0), buffer);

        renderSearchBar(buffer, split.get(1), state, ci.getPageCount(), filtered.size());

        // Build Row objects only for the visible window — see RowWindow.
        RowWindow window = RowWindow.from(state.scrollTop(), state.selection(),
                filtered.size(), area.height() - 5);
        // Enter opens the full min/max only for pages whose bounds the row
        // had to truncate, so the marker column carries a fact here rather
        // than repeating the cursor.
        int statBudget = statBudget(area.width());
        boolean mixed = false;
        for (int i = window.start(); i < window.end() && !mixed; i++) {
            mixed = !isExpandable(ci, filtered.get(i), col, state.logicalTypes(), statBudget);
        }
        List<Row> rows = new ArrayList<>(window.size());
        for (int i = window.start(); i < window.end(); i++) {
            int idx = filtered.get(i);
            String nulls = ci.nullCounts() != null && idx < ci.nullCounts().length
                    ? Fmt.fmt("%,d", ci.nullCounts()[idx])
                    : Strings.ABSENT_VALUE;
            rows.add(Row.from(
                    CursorPane.marker(isExpandable(ci, idx, col, state.logicalTypes(), statBudget),
                            i == state.selection(), mixed) + idx,
                    ci.nullPages()[idx] ? "yes" : "no",
                    nulls,
                    formatStat(ci.minValues().get(idx), col, state.logicalTypes(), statBudget),
                    formatStat(ci.maxValues().get(idx), col, state.logicalTypes(), statBudget)));
        }
        Row header = header();
        String typeMode = state.logicalTypes() ? "" : " · physical";
        Block block = Block.builder()
                .title(" Column index "
                        + Plurals.rangeOf(window, filtered.size())
                        + (state.filter().isEmpty()
                                ? ""
                                : " · " + Plurals.format(ci.getPageCount(), "page", "pages") + " total")
                        + typeMode + " ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .build();
        Table table = Table.builder()
                .header(header)
                .rows(rows)
                .widths(widths())
                .columnSpacing(COLUMN_SPACING)
                .block(block)
                .highlightSymbol("")
                .highlightStyle(Theme.selection())
                .build();
        TableState tableState = new TableState();
        if (!filtered.isEmpty()) {
            tableState.select(window.selectionInWindow());
        }
        table.render(split.get(2), buffer, tableState);

        if (state.modalOpen() && !filtered.isEmpty()) {
            int idx = filtered.get(Math.min(state.selection(), filtered.size() - 1));
            buffer.setStyle(area, Theme.dim());
            renderMinMaxModal(buffer, area, idx,
                    ci.minValues().get(idx), ci.maxValues().get(idx),
                    col, state.logicalTypes(), state.modalScroll());
        }
    }

    private static void renderMinMaxModal(Buffer buffer, Rect screenArea, int pageIndex,
                                          byte[] minBytes, byte[] maxBytes,
                                          ColumnSchema col, boolean logical, int scroll) {
        Rect area = ScrollPane.modalArea(screenArea, 100, screenArea.height());
        boolean hasLogical = col.logicalType() != null;
        ScrollPane.renderModal(buffer, area, "Page #" + pageIndex + " min / max",
                minMaxLines(minBytes, maxBytes, col, logical, ScrollPane.modalWidth(area)), scroll,
                "Esc / Enter close" + (hasLogical ? " · t logical types" : ""));
    }

    /// Line count of the open modal at the width the last frame wrapped to,
    /// so the key handler and the renderer agree on how far it can scroll.
    private static int modalLineCount(ParquetModel model, ScreenState.ColumnIndexView state) {
        ColumnIndex ci = model.columnIndex(state.rowGroupIndex(), state.columnIndex());
        if (ci == null) {
            return 0;
        }
        ColumnSchema col = model.schema().getColumn(state.columnIndex());
        List<Integer> filtered = filteredPages(ci, col, state.filter());
        if (filtered.isEmpty()) {
            return 0;
        }
        int idx = filtered.get(Math.min(state.selection(), filtered.size() - 1));
        return minMaxLines(ci.minValues().get(idx), ci.maxValues().get(idx),
                col, state.logicalTypes(), Keys.modalWidth()).size();
    }

    /// The modal's content, wrapped to its width. Min and max are arbitrary
    /// values — a long UTF-8 string bound ran off the right edge of the modal
    /// that exists to show it in full.
    private static List<Line> minMaxLines(byte[] minBytes, byte[] maxBytes, ColumnSchema col,
                                          boolean logical, int width) {
        List<Line> lines = new ArrayList<>();
        appendBound(lines, "Min", minBytes, col, logical, width);
        appendBound(lines, "Max", maxBytes, col, logical, width);
        return lines;
    }

    private static void appendBound(List<Line> lines, String label, byte[] bytes, ColumnSchema col,
                                    boolean logical, int width) {
        // Modal has space — render whole value
        String value = bytes == null ? Strings.ABSENT_VALUE : ValueFormatter.formatBytes(bytes, col, logical);
        // The label occupies the first five cells; continuation lines are
        // indented to match so a wrapped value reads as one field.
        List<String> wrapped = Strings.hardWrap(value, Math.max(1, width - 5));
        for (int i = 0; i < wrapped.size(); i++) {
            lines.add(i == 0
                    ? Line.from(new Span(" " + label + " ", Theme.primary()), Span.raw(wrapped.get(i)))
                    : Line.from(Span.raw("      " + wrapped.get(i))));
        }
    }

    public static String keybarKeys(ScreenState.ColumnIndexView state, ParquetModel model) {
        if (state.modalOpen()) {
            return "";
        }
        ColumnIndex ci = model.columnIndex(state.rowGroupIndex(), state.columnIndex());
        ColumnSchema col = model.schema().getColumn(state.columnIndex());
        List<Integer> filtered = filteredPages(ci, col, state.filter());
        int count = filtered.size();
        boolean hasLogical = col.logicalType() != null;
        // Enter opens the modal only when the selected row's Min or Max shows
        // less in the cell than it would in the modal — the same gate the `▶`
        // marker and the key handler use, so the three cannot disagree.
        boolean canExpand = false;
        if (count > 0) {
            canExpand = isExpandable(ci, filtered.get(Math.min(state.selection(), count - 1)),
                    col, state.logicalTypes(), statBudget(Keys.viewportWidth()));
        }
        return new Keys.Hints()
                .add(true, CursorPane.hints(count))
                .add(canExpand, "[Enter] view min/max")
                .add(count > 0, "[/] search")
                .add(hasLogical, "[t] logical types")
                .add(true, "[Esc] back")
                .build();
    }

    private static List<Integer> filteredPages(ColumnIndex ci, ColumnSchema col, String filter) {
        List<Integer> out = new ArrayList<>();
        if (ci == null) {
            return out;
        }
        String needle = filter.toLowerCase(Locale.ROOT);
        for (int i = 0; i < ci.getPageCount(); i++) {
            if (needle.isEmpty()) {
                out.add(i);
                continue;
            }
            // Matched against the full rendering, not the cell: a search for
            // part of a long bound still finds its page, even though the cell
            // shows only a marked prefix of it.
            String min = formatStatFull(ci.minValues().get(i), col, true).toLowerCase(Locale.ROOT);
            String max = formatStatFull(ci.maxValues().get(i), col, true).toLowerCase(Locale.ROOT);
            if (min.contains(needle) || max.contains(needle)) {
                out.add(i);
            }
        }
        return out;
    }

    private static void renderSearchBar(Buffer buffer, Rect area, ScreenState.ColumnIndexView state,
                                        int totalPages, int matchCount) {
        if (!state.searching() && state.filter().isEmpty()) {
            Paragraph.builder()
                    .text(Text.from(Line.from(new Span(
                            " " + Plurals.format(totalPages, "page", "pages")
                                    + ". Press / to filter by min/max.",
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
                new Span("  (" + Fmt.fmt("%,d", matchCount) + " / "
                        + Plurals.format(totalPages, "page", "pages") + ")", Theme.dim()));
        Paragraph.builder().text(Text.from(line)).left().build().render(area, buffer);
    }

    private static ScreenState.ColumnIndexView with(ScreenState.ColumnIndexView state,
                                                     int selection, String filter, boolean searching) {
        int newTop = selection == state.selection()
                ? state.scrollTop()
                : RowWindow.adjustTop(state.scrollTop(), selection, Keys.viewportStride());
        return new ScreenState.ColumnIndexView(
                state.rowGroupIndex(), state.columnIndex(), selection, filter, searching,
                state.logicalTypes(), state.modalOpen(), newTop);
    }

    private static ScreenState.ColumnIndexView withModal(ScreenState.ColumnIndexView s, boolean modal) {
        return new ScreenState.ColumnIndexView(s.rowGroupIndex(), s.columnIndex(), s.selection(),
                s.filter(), s.searching(), s.logicalTypes(), modal, s.scrollTop());
    }

    private static Row header() {
        String[] labels = new String[COLUMNS.length + STAT_LABELS.length];
        // The header is two label sources laid end to end: the fixed columns
        // first, then Min / Max. The index alone tells which one to read from.
        Arrays.setAll(labels, i -> i < COLUMNS.length
                ? COLUMNS[i].label
                : STAT_LABELS[i - COLUMNS.length]);
        return Row.from(labels).style(Theme.accent().bold());
    }

    private static Constraint[] widths() {
        Constraint[] widths = new Constraint[COLUMNS.length + STAT_LABELS.length];
        // Same two sources as [#header]: fixed widths first, then Min / Max,
        // which take equal shares of what is left.
        Arrays.setAll(widths, i -> i < COLUMNS.length
                ? new Constraint.Length(COLUMNS[i].width)
                : new Constraint.Fill(1));
        return widths;
    }

    /// Min and Max share the width the fixed columns leave. tamboui's Table
    /// clips silently at the column edge, so a cap wider than the cell takes
    /// the `…` with it and the value reads as complete. This screen has no
    /// highlight symbol, so the table gives the columns everything except its
    /// border and one gap between each pair.
    private static int statBudget(int viewportWidth) {
        int fixedColumns = Arrays.stream(COLUMNS).mapToInt(column -> column.width).sum();
        int gaps = (COLUMNS.length + STAT_LABELS.length - 1) * COLUMN_SPACING;
        return Math.max(1, (viewportWidth - fixedColumns - gaps - TABLE_BORDER_WIDTH) / STAT_LABELS.length);
    }

    /// Whether `Enter` would do anything on this page: the modal only earns
    /// its place when one of the bounds shows less in the cell than it would
    /// in the modal.
    private static boolean isExpandable(ColumnIndex ci, int page, ColumnSchema col, boolean logical,
                                        int budget) {
        return isAbbreviated(ci.minValues().get(page), col, logical, budget)
                || isAbbreviated(ci.maxValues().get(page), col, logical, budget);
    }

    /// Whether the cell rendering withholds anything the modal would reveal.
    ///
    /// Compared against a rendering bounded to the cell rather than the whole
    /// value: this runs for every visible row on every redraw, and a column of
    /// large payloads would otherwise be rendered in full on each keystroke
    /// just to compare two strings. A value the cell has to cut renders longer
    /// than the cell either way, so the bounded comparison gives the same
    /// answer.
    private static boolean isAbbreviated(byte[] bytes, ColumnSchema col, boolean logical, int budget) {
        if (bytes == null) {
            return false;
        }
        return !formatStat(bytes, col, logical, budget)
                .equals(ValueFormatter.formatBytes(bytes, col, logical, budget));
    }

    private static String formatStat(byte[] bytes, ColumnSchema col, boolean logical, int budget) {
        if (bytes == null) {
            return Strings.ABSENT_VALUE;
        }
        String full = ValueFormatter.formatBytes(bytes, col, logical, budget);
        return Strings.truncateRight(full, budget);
    }

    private static String formatStatFull(byte[] bytes, ColumnSchema col, boolean logical) {
        return bytes == null ? Strings.ABSENT_VALUE : ValueFormatter.formatBytes(bytes, col, logical);
    }

    private static void renderEmpty(Buffer buffer, Rect area, String message) {
        Block block = Block.builder()
                .title(" Column index ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .build();
        Paragraph.builder()
                .block(block)
                .text(Text.from(Line.from(new Span(" " + message, Theme.dim()))))
                .left()
                .build()
                .render(area, buffer);
    }
}
