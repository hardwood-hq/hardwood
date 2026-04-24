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
import dev.hardwood.cli.internal.Sizes;
import dev.hardwood.metadata.OffsetIndex;
import dev.hardwood.metadata.PageLocation;
import dev.tamboui.buffer.Buffer;
import dev.tamboui.layout.Constraint;
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

/// Page-location listing for one column chunk: absolute file offset, compressed
/// size, first row index per page.
public final class OffsetIndexScreen {

    private OffsetIndexScreen() {
    }

    public static boolean handle(KeyEvent event, ParquetModel model, NavigationStack stack) {
        ScreenState.OffsetIndexView state = (ScreenState.OffsetIndexView) stack.top();
        OffsetIndex oi = model.offsetIndex(state.rowGroupIndex(), state.columnIndex());
        int count = oi != null ? oi.pageLocations().size() : 0;
        if (event.isUp()) {
            stack.replaceTop(new ScreenState.OffsetIndexView(
                    state.rowGroupIndex(), state.columnIndex(),
                    Math.max(0, state.selection() - 1)));
            return true;
        }
        if (event.isDown()) {
            stack.replaceTop(new ScreenState.OffsetIndexView(
                    state.rowGroupIndex(), state.columnIndex(),
                    Math.min(count - 1, state.selection() + 1)));
            return true;
        }
        if (Keys.isJumpTop(event) && count > 0) {
            stack.replaceTop(new ScreenState.OffsetIndexView(
                    state.rowGroupIndex(), state.columnIndex(), 0));
            return true;
        }
        if (Keys.isJumpBottom(event) && count > 0) {
            stack.replaceTop(new ScreenState.OffsetIndexView(
                    state.rowGroupIndex(), state.columnIndex(), count - 1));
            return true;
        }
        return false;
    }

    public static void render(Buffer buffer, Rect area, ParquetModel model, ScreenState.OffsetIndexView state) {
        OffsetIndex oi = model.offsetIndex(state.rowGroupIndex(), state.columnIndex());
        if (oi == null) {
            Block emptyBlock = Block.builder()
                    .title(" Offset index ")
                    .borders(Borders.ALL)
                    .borderType(BorderType.ROUNDED)
                    .borderColor(Color.GRAY)
                    .build();
            Paragraph.builder()
                    .block(emptyBlock)
                    .text(Text.from(Line.from(
                            new Span(" No offset index for this chunk.", Style.EMPTY.fg(Color.GRAY)))))
                    .left()
                    .build()
                    .render(area, buffer);
            return;
        }
        List<Row> rows = new ArrayList<>();
        List<PageLocation> locations = oi.pageLocations();
        for (int i = 0; i < locations.size(); i++) {
            PageLocation loc = locations.get(i);
            rows.add(Row.from(
                    String.valueOf(i),
                    String.format("%,d", loc.offset()),
                    Sizes.format(loc.compressedPageSize()),
                    String.format("%,d", loc.firstRowIndex())));
        }
        Row header = Row.from("#", "Offset", "Size", "First row").style(Style.EMPTY.bold());
        Block block = Block.builder()
                .title(" Offset index (" + Plurals.format(locations.size(), "page", "pages") + ") ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .borderColor(Color.CYAN)
                .build();
        Table table = Table.builder()
                .header(header)
                .rows(rows)
                .widths(new Constraint.Length(5),
                        new Constraint.Length(16),
                        new Constraint.Length(12),
                        new Constraint.Length(16))
                .columnSpacing(2)
                .block(block)
                .highlightSymbol("▶ ")
                .highlightStyle(Style.EMPTY.bold())
                .build();
        TableState tableState = new TableState();
        if (!locations.isEmpty()) {
            tableState.select(state.selection());
        }
        table.render(area, buffer, tableState);
    }

    public static String keybarKeys() {
        return "[↑↓] move  [Esc] back";
    }
}
