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
import dev.tamboui.style.Color;
import dev.tamboui.style.Style;
import dev.tamboui.text.Line;
import dev.tamboui.text.Span;
import dev.tamboui.text.Text;
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
public final class ColumnIndexScreen {

    private ColumnIndexScreen() {
    }

    public static boolean handle(KeyEvent event, ParquetModel model, NavigationStack stack) {
        ScreenState.ColumnIndexView state = (ScreenState.ColumnIndexView) stack.top();
        ColumnIndex ci = model.columnIndex(state.rowGroupIndex(), state.columnIndex());
        int count = ci != null ? ci.getPageCount() : 0;
        if (event.isUp()) {
            stack.replaceTop(new ScreenState.ColumnIndexView(
                    state.rowGroupIndex(), state.columnIndex(),
                    Math.max(0, state.selection() - 1)));
            return true;
        }
        if (event.isDown()) {
            stack.replaceTop(new ScreenState.ColumnIndexView(
                    state.rowGroupIndex(), state.columnIndex(),
                    Math.min(count - 1, state.selection() + 1)));
            return true;
        }
        return false;
    }

    public static void render(Buffer buffer, Rect area, ParquetModel model, ScreenState.ColumnIndexView state) {
        ColumnIndex ci = model.columnIndex(state.rowGroupIndex(), state.columnIndex());
        if (ci == null) {
            renderEmpty(buffer, area, "No column index for this chunk.");
            return;
        }
        List<Rect> split = Layout.vertical()
                .constraints(new Constraint.Length(1), new Constraint.Fill(1))
                .split(area);

        Paragraph.builder()
                .text(Text.from(Line.from(
                        new Span(" Boundary order: ", Style.EMPTY.fg(Color.GRAY)),
                        new Span(ci.boundaryOrder().name(), Style.EMPTY.bold()))))
                .left()
                .build()
                .render(split.get(0), buffer);

        ColumnSchema col = model.schema().getColumn(state.columnIndex());
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < ci.getPageCount(); i++) {
            String nulls = ci.nullCounts() != null && i < ci.nullCounts().size()
                    ? String.format("%,d", ci.nullCounts().get(i))
                    : "—";
            rows.add(Row.from(
                    String.valueOf(i),
                    Boolean.TRUE.equals(ci.nullPages().get(i)) ? "yes" : "no",
                    nulls,
                    formatStat(ci.minValues().get(i), col),
                    formatStat(ci.maxValues().get(i), col)));
        }
        Row header = Row.from("#", "NullPg", "Nulls", "Min", "Max").style(Style.EMPTY.bold());
        Block block = Block.builder()
                .title(" Column index (" + ci.getPageCount() + " pages) ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .borderColor(Color.CYAN)
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
        if (ci.getPageCount() > 0) {
            tableState.select(state.selection());
        }
        table.render(split.get(1), buffer, tableState);
    }

    public static String keybarKeys() {
        return "[↑↓] move  [Esc] back";
    }

    private static String formatStat(byte[] bytes, ColumnSchema col) {
        if (bytes == null) {
            return "—";
        }
        return IndexValueFormatter.format(bytes, col);
    }

    private static void renderEmpty(Buffer buffer, Rect area, String message) {
        Block block = Block.builder()
                .title(" Column index ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .borderColor(Color.GRAY)
                .build();
        Paragraph.builder()
                .block(block)
                .text(Text.from(Line.from(new Span(" " + message, Style.EMPTY.fg(Color.GRAY)))))
                .left()
                .build()
                .render(area, buffer);
    }
}
