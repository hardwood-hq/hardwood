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

import dev.hardwood.cli.dive.ParquetModel;
import dev.hardwood.cli.dive.ScreenState;
import dev.hardwood.cli.internal.RowValueFormatter;
import dev.hardwood.schema.SchemaNode;
import dev.tamboui.buffer.Buffer;
import dev.tamboui.layout.Constraint;
import dev.tamboui.layout.Rect;
import dev.tamboui.style.Style;
import dev.tamboui.text.Line;
import dev.tamboui.text.Span;
import dev.tamboui.text.Text;
import dev.tamboui.tui.event.KeyCode;
import dev.tamboui.tui.event.KeyEvent;
import dev.tamboui.widgets.Clear;
import dev.tamboui.widgets.block.Block;
import dev.tamboui.widgets.block.BorderType;
import dev.tamboui.widgets.block.Borders;
import dev.tamboui.widgets.paragraph.Paragraph;
import dev.tamboui.widgets.table.Row;
import dev.tamboui.widgets.table.Table;
import dev.tamboui.widgets.table.TableState;

/// Projected-row preview. `firstRow` / `pageSize` define which rows are currently
/// loaded; `←/→` scrolls the visible column window for wide schemas; `PgDn`/`PgUp`
/// (or `Shift+↓/↑`) flip pages. [ParquetModel#readPreviewPage] maintains a
/// forward-only cursor across calls, so stepping forward never re-iterates from
/// row 0 — only backward moves (`PgUp`, `g` jump-to-top) recreate the reader.
public final class DataPreviewScreen {

    private static final int VISIBLE_COLUMNS = 5;
    private static final int VALUE_TRUNCATE = 32;

    private DataPreviewScreen() {
    }

    /// Loads the first page of rows for the given page size; used when the screen
    /// is first pushed onto the navigation stack.
    public static ScreenState.DataPreview initialState(ParquetModel model, int pageSize) {
        return loadPage(model, 0, pageSize, 0, true);
    }

    public static boolean handle(KeyEvent event, ParquetModel model, dev.hardwood.cli.dive.NavigationStack stack) {
        ScreenState.DataPreview state = (ScreenState.DataPreview) stack.top();
        long total = model.facts().totalRows();
        // columnNames already carries the top-level-field count (see loadPage —
        // the reader indexes into fields, not leaves, so leaf count would overshoot).
        int columnCount = state.columnNames().size();
        if (state.modalRow() >= 0) {
            return handleModal(event, state, stack);
        }
        // Plain ↑/↓ moves the selected-row cursor inside the current page; Shift+↑/↓
        // pages, handled below. Enter opens the full-record modal at the cursor.
        if (event.isUp() && !event.hasShift()) {
            int rowsLoaded = state.rows().size();
            if (rowsLoaded == 0) {
                return false;
            }
            stack.replaceTop(withSelectedRow(state, Math.max(0, state.selectedRow() - 1)));
            return true;
        }
        if (event.isDown() && !event.hasShift()) {
            int rowsLoaded = state.rows().size();
            if (rowsLoaded == 0) {
                return false;
            }
            stack.replaceTop(withSelectedRow(state, Math.min(rowsLoaded - 1, state.selectedRow() + 1)));
            return true;
        }
        if (event.isConfirm() && !state.rows().isEmpty()) {
            stack.replaceTop(withModalRow(state, state.selectedRow()));
            return true;
        }
        // Page forward: PgDn, or Shift+↓ — the Shift alias is a one-hand chord
        // on macOS laptops where PgDn is Fn+↓.
        if (event.code() == KeyCode.PAGE_DOWN || (event.hasShift() && event.isDown())) {
            long nextFirst = Math.min(total, state.firstRow() + state.pageSize());
            if (nextFirst >= total) {
                return false;
            }
            stack.replaceTop(loadPage(model, nextFirst, state.pageSize(), state.columnScroll(),
                    state.logicalTypes()));
            return true;
        }
        if (event.code() == KeyCode.PAGE_UP || (event.hasShift() && event.isUp())) {
            long prevFirst = Math.max(0, state.firstRow() - state.pageSize());
            if (prevFirst == state.firstRow()) {
                return false;
            }
            stack.replaceTop(loadPage(model, prevFirst, state.pageSize(), state.columnScroll(),
                    state.logicalTypes()));
            return true;
        }
        if (event.isLeft()) {
            if (state.columnScroll() == 0) {
                return false;
            }
            stack.replaceTop(withColumnScroll(state, Math.max(0, state.columnScroll() - 1)));
            return true;
        }
        if (event.isRight()) {
            int maxScroll = Math.max(0, columnCount - VISIBLE_COLUMNS);
            if (state.columnScroll() >= maxScroll) {
                return false;
            }
            stack.replaceTop(withColumnScroll(state, state.columnScroll() + 1));
            return true;
        }
        if (Keys.isJumpTop(event)) {
            if (state.firstRow() == 0) {
                return false;
            }
            stack.replaceTop(loadPage(model, 0, state.pageSize(), state.columnScroll(),
                    state.logicalTypes()));
            return true;
        }
        if (Keys.isJumpBottom(event)) {
            long lastPageFirst = Math.max(0, total - state.pageSize());
            if (state.firstRow() == lastPageFirst) {
                return false;
            }
            stack.replaceTop(loadPage(model, lastPageFirst, state.pageSize(), state.columnScroll(),
                    state.logicalTypes()));
            return true;
        }
        // Toggle logical-type rendering. Modifier-free `t` (avoid clobbering
        // typed text in any future search-mode here).
        if (event.code() == KeyCode.CHAR && event.character() == 't'
                && !event.hasCtrl() && !event.hasAlt()) {
            stack.replaceTop(loadPage(model, state.firstRow(), state.pageSize(),
                    state.columnScroll(), !state.logicalTypes()));
            return true;
        }
        return false;
    }

    public static void render(Buffer buffer, Rect area, ParquetModel model, ScreenState.DataPreview state) {
        int columnCount = state.columnNames().size();
        int windowEnd = Math.min(columnCount, state.columnScroll() + VISIBLE_COLUMNS);
        List<String> visible = state.columnNames().subList(state.columnScroll(), windowEnd);

        List<Row> rows = new ArrayList<>();
        for (List<String> row : state.rows()) {
            List<String> sliced = row.subList(state.columnScroll(), windowEnd);
            String[] truncated = new String[sliced.size()];
            for (int i = 0; i < sliced.size(); i++) {
                truncated[i] = truncate(sliced.get(i), VALUE_TRUNCATE);
            }
            rows.add(Row.from(truncated));
        }
        Row header = Row.from(visible.toArray(new String[0])).style(Style.EMPTY.bold());

        long total = model.facts().totalRows();
        long lastRow = state.firstRow() + state.rows().size();
        String typeMode = state.logicalTypes() ? "" : " · physical";
        String title = String.format(" Data preview (rows %,d–%,d of %,d · cols %d–%d of %d%s) ",
                state.firstRow() + 1, lastRow, total,
                state.columnScroll() + 1, windowEnd, columnCount, typeMode);

        Block block = Block.builder()
                .title(title)
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .borderColor(Theme.ACCENT)
                .build();
        List<Constraint> widths = new ArrayList<>();
        for (int i = 0; i < visible.size(); i++) {
            widths.add(new Constraint.Fill(1));
        }
        Table table = Table.builder()
                .header(header)
                .rows(rows)
                .widths(widths)
                .columnSpacing(2)
                .block(block)
                .highlightSymbol("▶ ")
                .highlightStyle(Style.EMPTY.bold())
                .build();
        TableState tableState = new TableState();
        if (!state.rows().isEmpty()) {
            tableState.select(Math.min(state.selectedRow(), state.rows().size() - 1));
        }
        table.render(area, buffer, tableState);
        if (state.modalRow() >= 0 && state.modalRow() < state.rows().size()) {
            renderRecordModal(buffer, area, state);
        }
    }

    private static void renderRecordModal(Buffer buffer, Rect screenArea, ScreenState.DataPreview state) {
        List<String> values = state.rows().get(state.modalRow());
        List<String> expanded = state.expandedRows().get(state.modalRow());
        List<String> names = state.columnNames();
        // Grow the modal to fill the available area (minus 2-cell margin) so
        // long values get the full screen width instead of being clipped to
        // the narrow window the table cells lived in.
        int width = Math.max(40, screenArea.width() - 4);
        int height = Math.max(8, screenArea.height() - 2);
        int x = screenArea.left() + (screenArea.width() - width) / 2;
        int y = screenArea.top() + (screenArea.height() - height) / 2;
        Rect area = new Rect(x, y, width, height);
        Clear.INSTANCE.render(area, buffer);

        int maxKeyWidth = 0;
        for (String name : names) {
            maxKeyWidth = Math.max(maxKeyWidth, name.length());
        }
        // " name<pad> : value" — borders + " " + name + " : " + trailing
        int valueBudget = Math.max(8, width - 2 - 1 - maxKeyWidth - 3 - 1);
        // Continuation lines use the same indent as where the value started
        // so wrapped multi-line values line up under the leading column.
        String continuationIndent = " ".repeat(1 + maxKeyWidth + 3);

        // Build the full content list first, then slice by modalScroll into
        // the viewport so expanded values can scroll past the visible area.
        List<Line> all = new ArrayList<>();
        for (int i = 0; i < names.size(); i++) {
            String name = names.get(i);
            String pad = " ".repeat(maxKeyWidth - name.length());
            boolean selected = i == state.modalColumn();
            boolean isExpanded = i == state.expandedColumn();
            String cursor = selected ? "▶" : " ";
            Style nameStyle = selected ? Style.EMPTY.bold().fg(Theme.ACCENT) : Style.EMPTY.bold();
            String value = i < values.size() ? values.get(i) : "";
            if (isExpanded) {
                String fullValue = i < expanded.size() ? expanded.get(i) : value;
                List<String> wrapped = wrapValue(fullValue, valueBudget);
                if (wrapped.isEmpty()) {
                    wrapped.add("");
                }
                all.add(Line.from(
                        new Span(cursor + name + pad + " : ", nameStyle),
                        Span.raw(wrapped.get(0))));
                for (int k = 1; k < wrapped.size(); k++) {
                    all.add(Line.from(Span.raw(continuationIndent + wrapped.get(k))));
                }
            }
            else {
                all.add(Line.from(
                        new Span(cursor + name + pad + " : ", nameStyle),
                        Span.raw(truncate(value, valueBudget))));
            }
        }

        // Reserve 2 rows for borders + 2 rows for blank + hint.
        int viewport = Math.max(1, height - 4);
        int totalLines = all.size();
        int maxScroll = Math.max(0, totalLines - viewport);
        int scroll = Math.max(0, Math.min(state.modalScroll(), maxScroll));
        int end = Math.min(totalLines, scroll + viewport);

        List<Line> lines = new ArrayList<>(all.subList(scroll, end));
        lines.add(Line.empty());
        String hint;
        if (scroll + viewport < totalLines) {
            hint = " ↓ " + (totalLines - end) + " more lines · ↑↓ row · ←/→ column · Enter expand · Shift+↑↓ scroll · Esc close";
        }
        else if (scroll > 0) {
            hint = " ↑ " + scroll + " lines above · ↑↓ row · ←/→ column · Enter expand · Shift+↑↓ scroll · Esc close";
        }
        else {
            hint = " ↑↓ row · ←/→ column · Enter expand · Shift+↑↓ scroll · Esc close";
        }
        lines.add(Line.from(new Span(hint, Style.EMPTY.fg(Theme.DIM))));

        long absRow = state.firstRow() + state.modalRow();
        Block block = Block.builder()
                .title(String.format(" Row %,d ", absRow + 1))
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .borderColor(Theme.ACCENT)
                .build();
        Paragraph.builder()
                .block(block)
                .text(Text.from(lines))
                .left()
                .build()
                .render(area, buffer);
    }

    /// Splits a possibly multi-line value into display lines that each fit
    /// within `width` cells. Hard line breaks in the source are preserved;
    /// each segment is then chunked at `width` if it's longer.
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
            int i = 0;
            while (i < line.length()) {
                int end = Math.min(line.length(), i + width);
                out.add(line.substring(i, end));
                i = end;
            }
        }
        return out;
    }

    public static String keybarKeys() {
        return "[↑↓] row  [Enter] view record  [←→] columns  "
                + "[PgDn/PgUp or Shift+↓↑] page  [g/G] start/end  [t] logical types  [Esc] back";
    }

    private static boolean handleModal(KeyEvent event, ScreenState.DataPreview state,
                                       dev.hardwood.cli.dive.NavigationStack stack) {
        // Esc closes the row modal; Enter toggles inline expansion for the
        // currently highlighted column. ↑/↓ row-step (collapsing on row
        // change since expansion is per-column-in-current-row);
        // Shift+↑/↓ scroll the modal viewport vertically.
        if (event.isCancel()) {
            stack.replaceTop(withModalRow(state, -1));
            return true;
        }
        if (event.isConfirm()) {
            int newExpanded = state.expandedColumn() == state.modalColumn()
                    ? -1 : state.modalColumn();
            stack.replaceTop(withExpandedColumn(state, newExpanded));
            return true;
        }
        if (event.isUp() && !event.hasShift()) {
            if (state.modalRow() == 0) {
                return false;
            }
            int next = state.modalRow() - 1;
            stack.replaceTop(rebuild(state, state.firstRow(), state.pageSize(),
                    state.columnNames(), state.rows(), state.expandedRows(),
                    state.columnScroll(), next, next, state.logicalTypes(),
                    state.modalColumn(), -1, 0));
            return true;
        }
        if (event.isDown() && !event.hasShift()) {
            int last = state.rows().size() - 1;
            if (state.modalRow() >= last) {
                return false;
            }
            int next = state.modalRow() + 1;
            stack.replaceTop(rebuild(state, state.firstRow(), state.pageSize(),
                    state.columnNames(), state.rows(), state.expandedRows(),
                    state.columnScroll(), next, next, state.logicalTypes(),
                    state.modalColumn(), -1, 0));
            return true;
        }
        if (event.isUp() && event.hasShift()) {
            stack.replaceTop(withModalScroll(state, Math.max(0, state.modalScroll() - 10)));
            return true;
        }
        if (event.isDown() && event.hasShift()) {
            stack.replaceTop(withModalScroll(state, state.modalScroll() + 10));
            return true;
        }
        if (event.isLeft()) {
            if (state.modalColumn() == 0) {
                return false;
            }
            stack.replaceTop(rebuild(state, state.firstRow(), state.pageSize(),
                    state.columnNames(), state.rows(), state.expandedRows(),
                    state.columnScroll(), state.selectedRow(), state.modalRow(),
                    state.logicalTypes(), state.modalColumn() - 1, -1, 0));
            return true;
        }
        if (event.isRight()) {
            int last = state.columnNames().size() - 1;
            if (state.modalColumn() >= last) {
                return false;
            }
            stack.replaceTop(rebuild(state, state.firstRow(), state.pageSize(),
                    state.columnNames(), state.rows(), state.expandedRows(),
                    state.columnScroll(), state.selectedRow(), state.modalRow(),
                    state.logicalTypes(), state.modalColumn() + 1, -1, 0));
            return true;
        }
        return false;
    }

    private static ScreenState.DataPreview withSelectedRow(ScreenState.DataPreview s, int sel) {
        return new ScreenState.DataPreview(s.firstRow(), s.pageSize(), s.columnNames(), s.rows(),
                s.expandedRows(), s.columnScroll(), sel, s.modalRow(), s.logicalTypes(),
                s.modalColumn(), s.expandedColumn(), s.modalScroll());
    }

    private static ScreenState.DataPreview withModalRow(ScreenState.DataPreview s, int modalRow) {
        return new ScreenState.DataPreview(s.firstRow(), s.pageSize(), s.columnNames(), s.rows(),
                s.expandedRows(), s.columnScroll(), s.selectedRow(), modalRow, s.logicalTypes(),
                modalRow < 0 ? 0 : s.modalColumn(),
                modalRow < 0 ? -1 : s.expandedColumn(),
                modalRow < 0 ? 0 : s.modalScroll());
    }

    private static ScreenState.DataPreview withColumnScroll(ScreenState.DataPreview s, int scroll) {
        return new ScreenState.DataPreview(s.firstRow(), s.pageSize(), s.columnNames(), s.rows(),
                s.expandedRows(), scroll, s.selectedRow(), s.modalRow(), s.logicalTypes(),
                s.modalColumn(), s.expandedColumn(), s.modalScroll());
    }

    private static ScreenState.DataPreview withExpandedColumn(ScreenState.DataPreview s, int col) {
        return new ScreenState.DataPreview(s.firstRow(), s.pageSize(), s.columnNames(), s.rows(),
                s.expandedRows(), s.columnScroll(), s.selectedRow(), s.modalRow(), s.logicalTypes(),
                s.modalColumn(), col, 0);
    }

    private static ScreenState.DataPreview withModalScroll(ScreenState.DataPreview s, int scroll) {
        return new ScreenState.DataPreview(s.firstRow(), s.pageSize(), s.columnNames(), s.rows(),
                s.expandedRows(), s.columnScroll(), s.selectedRow(), s.modalRow(), s.logicalTypes(),
                s.modalColumn(), s.expandedColumn(), scroll);
    }

    private static ScreenState.DataPreview rebuild(ScreenState.DataPreview s,
                                                   long firstRow, int pageSize,
                                                   List<String> columnNames,
                                                   List<List<String>> rows,
                                                   List<List<String>> expandedRows,
                                                   int columnScroll, int selectedRow,
                                                   int modalRow, boolean logicalTypes,
                                                   int modalColumn, int expandedColumn,
                                                   int modalScroll) {
        return new ScreenState.DataPreview(firstRow, pageSize, columnNames, rows, expandedRows,
                columnScroll, selectedRow, modalRow, logicalTypes, modalColumn, expandedColumn,
                modalScroll);
    }

    private static ScreenState.DataPreview loadPage(ParquetModel model, long firstRow, int pageSize,
                                                    int columnScroll, boolean logicalTypes) {
        // `RowReader` indexes into the root message's top-level fields, not into
        // leaf columns. For a flat schema those counts coincide; for a schema
        // with lists / structs / maps / variants at the root they diverge, and
        // iterating to model.columnCount() (leaf count) overruns the reader.
        List<SchemaNode> topLevel = model.schema().getRootNode().children();
        List<String> columnNames = new ArrayList<>(topLevel.size());
        for (SchemaNode node : topLevel) {
            columnNames.add(node.name());
        }
        int fieldCount = columnNames.size();
        List<List<String>> rows = new ArrayList<>();
        List<List<String>> expandedRows = new ArrayList<>();
        try {
            model.readPreviewPage(firstRow, pageSize, reader -> {
                List<String> row = new ArrayList<>(fieldCount);
                List<String> expanded = new ArrayList<>(fieldCount);
                for (int c = 0; c < fieldCount; c++) {
                    SchemaNode field = topLevel.get(c);
                    row.add(RowValueFormatter.format(reader, c, field, logicalTypes));
                    expanded.add(RowValueFormatter.formatExpanded(reader, c, field, logicalTypes));
                }
                rows.add(row);
                expandedRows.add(expanded);
            });
        }
        catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
        return new ScreenState.DataPreview(firstRow, pageSize, columnNames, rows, expandedRows,
                columnScroll, 0, -1, logicalTypes, 0, -1, 0);
    }

    private static String truncate(String s, int max) {
        if (s.length() <= max) {
            return s;
        }
        return s.substring(0, max - 1) + "…";
    }
}
