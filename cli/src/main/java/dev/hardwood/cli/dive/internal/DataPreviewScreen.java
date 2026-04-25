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
            rows.add(Row.from(sliced.toArray(new String[0])));
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
        List<String> names = state.columnNames();
        int width = Math.min(100, screenArea.width() - 4);
        int contentLines = names.size();
        int height = Math.min(screenArea.height() - 2, contentLines + 4);
        int x = screenArea.left() + (screenArea.width() - width) / 2;
        int y = screenArea.top() + (screenArea.height() - height) / 2;
        Rect area = new Rect(x, y, width, height);
        Clear.INSTANCE.render(area, buffer);

        int maxKeyWidth = 0;
        for (String name : names) {
            maxKeyWidth = Math.max(maxKeyWidth, name.length());
        }

        List<Line> lines = new ArrayList<>();
        for (int i = 0; i < names.size(); i++) {
            String name = names.get(i);
            String value = i < values.size() ? values.get(i) : "";
            String pad = " ".repeat(maxKeyWidth - name.length());
            lines.add(Line.from(
                    new Span(" " + name + pad + " : ", Style.EMPTY.bold()),
                    Span.raw(value)));
        }
        lines.add(Line.empty());
        lines.add(Line.from(new Span(
                " ↑/↓ next/prev row   Esc/Enter close",
                Style.EMPTY.fg(Theme.DIM))));
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

    public static String keybarKeys() {
        return "[↑↓] row  [Enter] view record  [←→] columns  "
                + "[PgDn/PgUp or Shift+↓↑] page  [g/G] start/end  [t] logical types  [Esc] back";
    }

    private static boolean handleModal(KeyEvent event, ScreenState.DataPreview state,
                                       dev.hardwood.cli.dive.NavigationStack stack) {
        if (event.isCancel() || event.isConfirm()) {
            stack.replaceTop(withModalRow(state, -1));
            return true;
        }
        if (event.isUp()) {
            if (state.modalRow() == 0) {
                return false;
            }
            int next = state.modalRow() - 1;
            stack.replaceTop(new ScreenState.DataPreview(
                    state.firstRow(), state.pageSize(), state.columnNames(), state.rows(),
                    state.columnScroll(), next, next, state.logicalTypes()));
            return true;
        }
        if (event.isDown()) {
            int last = state.rows().size() - 1;
            if (state.modalRow() >= last) {
                return false;
            }
            int next = state.modalRow() + 1;
            stack.replaceTop(new ScreenState.DataPreview(
                    state.firstRow(), state.pageSize(), state.columnNames(), state.rows(),
                    state.columnScroll(), next, next, state.logicalTypes()));
            return true;
        }
        return false;
    }

    private static ScreenState.DataPreview withSelectedRow(ScreenState.DataPreview s, int sel) {
        return new ScreenState.DataPreview(s.firstRow(), s.pageSize(), s.columnNames(), s.rows(),
                s.columnScroll(), sel, s.modalRow(), s.logicalTypes());
    }

    private static ScreenState.DataPreview withModalRow(ScreenState.DataPreview s, int modalRow) {
        return new ScreenState.DataPreview(s.firstRow(), s.pageSize(), s.columnNames(), s.rows(),
                s.columnScroll(), s.selectedRow(), modalRow, s.logicalTypes());
    }

    private static ScreenState.DataPreview withColumnScroll(ScreenState.DataPreview s, int scroll) {
        return new ScreenState.DataPreview(s.firstRow(), s.pageSize(), s.columnNames(), s.rows(),
                scroll, s.selectedRow(), s.modalRow(), s.logicalTypes());
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
        try {
            model.readPreviewPage(firstRow, pageSize, reader -> {
                List<String> row = new ArrayList<>(fieldCount);
                for (int c = 0; c < fieldCount; c++) {
                    row.add(truncate(RowValueFormatter.format(reader, c, topLevel.get(c), logicalTypes),
                            VALUE_TRUNCATE));
                }
                rows.add(row);
            });
        }
        catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
        return new ScreenState.DataPreview(firstRow, pageSize, columnNames, rows, columnScroll, 0, -1,
                logicalTypes);
    }

    private static String truncate(String s, int max) {
        if (s.length() <= max) {
            return s;
        }
        return s.substring(0, max - 1) + "…";
    }
}
