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
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnSchema;
import dev.tamboui.buffer.Buffer;
import dev.tamboui.layout.Constraint;
import dev.tamboui.layout.Rect;
import dev.tamboui.style.Color;
import dev.tamboui.style.Style;
import dev.tamboui.tui.event.KeyCode;
import dev.tamboui.tui.event.KeyEvent;
import dev.tamboui.widgets.block.Block;
import dev.tamboui.widgets.block.BorderType;
import dev.tamboui.widgets.block.Borders;
import dev.tamboui.widgets.table.Row;
import dev.tamboui.widgets.table.Table;
import dev.tamboui.widgets.table.TableState;

/// Projected-row preview. `firstRow` / `pageSize` define which rows are currently
/// loaded; `←/→` scrolls the visible column window for wide schemas;
/// `PgDn`/`PgUp` flip pages by re-creating a [RowReader] starting from row 0 and
/// skipping ahead. Forward-only `RowReader` means each page-flip re-walks the
/// file from the start — fine for casual inspection, not intended for iteration.
public final class DataPreviewScreen {

    private static final int VISIBLE_COLUMNS = 5;
    private static final int VALUE_TRUNCATE = 32;

    private DataPreviewScreen() {
    }

    /// Loads the first page of rows for the given page size; used when the screen
    /// is first pushed onto the navigation stack.
    public static ScreenState.DataPreview initialState(ParquetModel model, int pageSize) {
        return loadPage(model, 0, pageSize, 0);
    }

    public static boolean handle(KeyEvent event, ParquetModel model, dev.hardwood.cli.dive.NavigationStack stack) {
        ScreenState.DataPreview state = (ScreenState.DataPreview) stack.top();
        long total = model.facts().totalRows();
        int columnCount = model.columnCount();
        if (event.code() == KeyCode.PAGE_DOWN) {
            long nextFirst = Math.min(total, state.firstRow() + state.pageSize());
            if (nextFirst >= total) {
                return false;
            }
            stack.replaceTop(loadPage(model, nextFirst, state.pageSize(), state.columnScroll()));
            return true;
        }
        if (event.code() == KeyCode.PAGE_UP) {
            long prevFirst = Math.max(0, state.firstRow() - state.pageSize());
            if (prevFirst == state.firstRow()) {
                return false;
            }
            stack.replaceTop(loadPage(model, prevFirst, state.pageSize(), state.columnScroll()));
            return true;
        }
        if (event.isLeft()) {
            if (state.columnScroll() == 0) {
                return false;
            }
            stack.replaceTop(new ScreenState.DataPreview(
                    state.firstRow(), state.pageSize(), state.columnNames(), state.rows(),
                    Math.max(0, state.columnScroll() - 1)));
            return true;
        }
        if (event.isRight()) {
            int maxScroll = Math.max(0, columnCount - VISIBLE_COLUMNS);
            if (state.columnScroll() >= maxScroll) {
                return false;
            }
            stack.replaceTop(new ScreenState.DataPreview(
                    state.firstRow(), state.pageSize(), state.columnNames(), state.rows(),
                    state.columnScroll() + 1));
            return true;
        }
        return false;
    }

    public static void render(Buffer buffer, Rect area, ParquetModel model, ScreenState.DataPreview state) {
        int columnCount = model.columnCount();
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
        String title = String.format(" Data preview (rows %,d–%,d of %,d · cols %d–%d of %d) ",
                state.firstRow() + 1, lastRow, total,
                state.columnScroll() + 1, windowEnd, columnCount);

        Block block = Block.builder()
                .title(title)
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .borderColor(Color.CYAN)
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
                .build();
        TableState tableState = new TableState();
        table.render(area, buffer, tableState);
    }

    public static String keybarKeys() {
        return "[←→] columns  [PgDn/PgUp] page  [Esc] back";
    }

    private static ScreenState.DataPreview loadPage(ParquetModel model, long firstRow, int pageSize, int columnScroll) {
        List<String> columnNames = new ArrayList<>(model.columnCount());
        for (int i = 0; i < model.columnCount(); i++) {
            ColumnSchema col = model.schema().getColumn(i);
            columnNames.add(col.fieldPath().toString());
        }
        List<List<String>> rows = new ArrayList<>();
        try (RowReader reader = model.reader().createRowReader()) {
            for (long skip = 0; skip < firstRow && reader.hasNext(); skip++) {
                reader.next();
            }
            int read = 0;
            while (read < pageSize && reader.hasNext()) {
                reader.next();
                List<String> row = new ArrayList<>(columnNames.size());
                for (int c = 0; c < columnNames.size(); c++) {
                    if (reader.isNull(c)) {
                        row.add("null");
                    }
                    else {
                        row.add(truncate(String.valueOf(reader.getValue(c)), VALUE_TRUNCATE));
                    }
                }
                rows.add(row);
                read++;
            }
        }
        return new ScreenState.DataPreview(firstRow, pageSize, columnNames, rows, columnScroll);
    }

    private static String truncate(String s, int max) {
        if (s.length() <= max) {
            return s;
        }
        return s.substring(0, max - 1) + "…";
    }
}
