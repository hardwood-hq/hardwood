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
import dev.hardwood.cli.internal.IndexValueFormatter;
import dev.hardwood.metadata.ColumnIndex;
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

/// Per-page statistics for one column chunk: null_pages, null counts, min, max.
/// Boundary order is shown above the table.
///
/// `/` enters inline search: the filter matches against each page's formatted
/// min or max value (case-insensitive substring).
public final class ColumnIndexScreen {

    private ColumnIndexScreen() {
    }

    /// Used by [DiveApp] to decide whether the screen should receive printable
    /// chars instead of the global keymap.
    public static boolean isInInputMode(ScreenState.ColumnIndexView state) {
        return state.searching();
    }

    public static boolean handle(KeyEvent event, ParquetModel model, NavigationStack stack) {
        ScreenState.ColumnIndexView state = (ScreenState.ColumnIndexView) stack.top();
        if (state.modalOpen()) {
            if (event.isCancel() || event.isConfirm()) {
                stack.replaceTop(withModal(state, false));
                return true;
            }
            return false;
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
        if (Keys.isPageDown(event)) {
            int max = filtered.isEmpty() ? 0 : filtered.size() - 1;
            stack.replaceTop(with(state, Math.min(max, state.selection() + Keys.viewportStride()),
                    state.filter(), false));
            return true;
        }
        if (Keys.isPageUp(event)) {
            stack.replaceTop(with(state, Math.max(0, state.selection() - Keys.viewportStride()),
                    state.filter(), false));
            return true;
        }
        if (Keys.isStepUp(event)) {
            stack.replaceTop(with(state, Math.max(0, state.selection() - 1), state.filter(), false));
            return true;
        }
        if (Keys.isStepDown(event)) {
            int max = filtered.isEmpty() ? 0 : filtered.size() - 1;
            stack.replaceTop(with(state, Math.min(max, state.selection() + 1), state.filter(), false));
            return true;
        }
        if (Keys.isJumpTop(event) && !filtered.isEmpty()) {
            stack.replaceTop(with(state, 0, state.filter(), false));
            return true;
        }
        if (Keys.isJumpBottom(event) && !filtered.isEmpty()) {
            stack.replaceTop(with(state, filtered.size() - 1, state.filter(), false));
            return true;
        }
        if (event.code() == KeyCode.CHAR && event.character() == 't'
                && !event.hasCtrl() && !event.hasAlt()) {
            stack.replaceTop(new ScreenState.ColumnIndexView(
                    state.rowGroupIndex(), state.columnIndex(), state.selection(),
                    state.filter(), false, !state.logicalTypes(), false));
            return true;
        }
        // Open the full Min/Max modal on Enter. The earlier gate looked for an
        // ellipsis suffix in the formatted string, but tamboui's Table clips
        // cells at column width without rewriting the underlying value, so
        // the gate never fired for the visible truncation. Opening
        // unconditionally is also fine UX-wise — the modal is harmless when
        // the value already fits, and Esc dismisses it.
        if (event.isConfirm() && !filtered.isEmpty()) {
            stack.replaceTop(withModal(state, true));
            return true;
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
                        new Span(" Boundary order: ", Style.EMPTY.fg(Theme.DIM)),
                        new Span(ci.boundaryOrder().name(), Style.EMPTY.bold()))))
                .left()
                .build()
                .render(split.get(0), buffer);

        renderSearchBar(buffer, split.get(1), state, ci.getPageCount(), filtered.size());

        List<Row> rows = new ArrayList<>();
        for (int idx : filtered) {
            String nulls = ci.nullCounts() != null && idx < ci.nullCounts().size()
                    ? String.format("%,d", ci.nullCounts().get(idx))
                    : "—";
            rows.add(Row.from(
                    String.valueOf(idx),
                    Boolean.TRUE.equals(ci.nullPages().get(idx)) ? "yes" : "no",
                    nulls,
                    formatStat(ci.minValues().get(idx), col, state.logicalTypes()),
                    formatStat(ci.maxValues().get(idx), col, state.logicalTypes())));
        }
        Row header = Row.from("#", "NullPg", "Nulls", "Min", "Max").style(Style.EMPTY.bold());
        String typeMode = state.logicalTypes() ? "" : " · physical";
        Block block = Block.builder()
                .title(" Column index (" + Plurals.format(ci.getPageCount(), "page", "pages")
                        + (state.filter().isEmpty() ? "" : "; " + filtered.size() + " matching")
                        + typeMode + ") ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .borderColor(Theme.ACCENT)
                .build();
        Table table = Table.builder()
                .header(header)
                .rows(rows)
                .widths(new Constraint.Length(5),
                        new Constraint.Length(8),
                        new Constraint.Length(10),
                        new Constraint.Fill(1),
                        new Constraint.Fill(1))
                .columnSpacing(2)
                .block(block)
                .highlightSymbol("▶ ")
                .highlightStyle(Style.EMPTY.bold())
                .build();
        TableState tableState = new TableState();
        if (!filtered.isEmpty()) {
            tableState.select(Math.min(state.selection(), filtered.size() - 1));
        }
        table.render(split.get(2), buffer, tableState);

        if (state.modalOpen() && !filtered.isEmpty()) {
            int idx = filtered.get(Math.min(state.selection(), filtered.size() - 1));
            renderMinMaxModal(buffer, area, idx,
                    ci.minValues().get(idx), ci.maxValues().get(idx),
                    col, state.logicalTypes());
        }
    }

    private static void renderMinMaxModal(Buffer buffer, Rect screenArea, int pageIndex,
                                          byte[] minBytes, byte[] maxBytes,
                                          ColumnSchema col, boolean logical) {
        // Modal has space — bypass the per-string 20-char cap.
        String min = minBytes == null ? "—" : IndexValueFormatter.format(minBytes, col, logical, false);
        String max = maxBytes == null ? "—" : IndexValueFormatter.format(maxBytes, col, logical, false);

        int width = Math.min(100, screenArea.width() - 4);
        int height = Math.min(screenArea.height() - 2, 8);
        int x = screenArea.left() + (screenArea.width() - width) / 2;
        int y = screenArea.top() + (screenArea.height() - height) / 2;
        Rect modal = new Rect(x, y, width, height);
        dev.tamboui.widgets.Clear.INSTANCE.render(modal, buffer);

        List<Line> lines = new ArrayList<>();
        lines.add(Line.from(new Span(" Min ", Style.EMPTY.bold()), Span.raw(min)));
        lines.add(Line.from(new Span(" Max ", Style.EMPTY.bold()), Span.raw(max)));
        lines.add(Line.empty());
        lines.add(Line.from(new Span(" Press Esc or Enter to close", Style.EMPTY.fg(Theme.DIM))));
        Block block = Block.builder()
                .title(" Page #" + pageIndex + " min / max ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .borderColor(Theme.ACCENT)
                .build();
        Paragraph.builder().block(block).text(Text.from(lines)).left().build().render(modal, buffer);
    }

    public static String keybarKeys() {
        return "[↑↓] move  [PgDn/PgUp or Shift+↓↑] page  [Enter] view min/max  [/] search  [t] logical types  [Esc] back";
    }

    private static List<Integer> filteredPages(ColumnIndex ci, ColumnSchema col, String filter) {
        List<Integer> out = new ArrayList<>();
        if (ci == null) {
            return out;
        }
        String needle = filter.toLowerCase();
        for (int i = 0; i < ci.getPageCount(); i++) {
            if (needle.isEmpty()) {
                out.add(i);
                continue;
            }
            String min = formatStat(ci.minValues().get(i), col, true).toLowerCase();
            String max = formatStat(ci.maxValues().get(i), col, true).toLowerCase();
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
                new Span("  (" + String.format("%,d", matchCount) + " / "
                        + Plurals.format(totalPages, "page", "pages") + ")", Style.EMPTY.fg(Theme.DIM)));
        Paragraph.builder().text(Text.from(line)).left().build().render(area, buffer);
    }

    private static ScreenState.ColumnIndexView with(ScreenState.ColumnIndexView state,
                                                     int selection, String filter, boolean searching) {
        return new ScreenState.ColumnIndexView(
                state.rowGroupIndex(), state.columnIndex(), selection, filter, searching,
                state.logicalTypes(), state.modalOpen());
    }

    private static ScreenState.ColumnIndexView withModal(ScreenState.ColumnIndexView s, boolean modal) {
        return new ScreenState.ColumnIndexView(s.rowGroupIndex(), s.columnIndex(), s.selection(),
                s.filter(), s.searching(), s.logicalTypes(), modal);
    }

    private static String formatStat(byte[] bytes, ColumnSchema col, boolean logical) {
        if (bytes == null) {
            return "—";
        }
        return IndexValueFormatter.format(bytes, col, logical);
    }

    private static void renderEmpty(Buffer buffer, Rect area, String message) {
        Block block = Block.builder()
                .title(" Column index ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .borderColor(Theme.DIM)
                .build();
        Paragraph.builder()
                .block(block)
                .text(Text.from(Line.from(new Span(" " + message, Style.EMPTY.fg(Theme.DIM)))))
                .left()
                .build()
                .render(area, buffer);
    }
}
