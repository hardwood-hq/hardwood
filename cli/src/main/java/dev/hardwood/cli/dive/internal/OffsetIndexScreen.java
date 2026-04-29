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
import dev.hardwood.cli.internal.Fmt;
import dev.hardwood.cli.internal.Sizes;
import dev.hardwood.metadata.OffsetIndex;
import dev.hardwood.metadata.PageLocation;
import dev.tamboui.buffer.Buffer;
import dev.tamboui.layout.Constraint;
import dev.tamboui.layout.Rect;
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
        if (Keys.isStepUp(event)) {
            stack.replaceTop(moved(state, Math.max(0, state.selection() - 1)));
            return true;
        }
        if (Keys.isStepDown(event)) {
            stack.replaceTop(moved(state, Math.min(count - 1, state.selection() + 1)));
            return true;
        }
        if (Keys.isPageDown(event) && count > 0) {
            stack.replaceTop(moved(state, Math.min(count - 1, state.selection() + Keys.viewportStride())));
            return true;
        }
        if (Keys.isPageUp(event) && count > 0) {
            stack.replaceTop(moved(state, Math.max(0, state.selection() - Keys.viewportStride())));
            return true;
        }
        if (Keys.isJumpTop(event) && count > 0) {
            stack.replaceTop(moved(state, 0));
            return true;
        }
        if (Keys.isJumpBottom(event) && count > 0) {
            stack.replaceTop(moved(state, count - 1));
            return true;
        }
        return false;
    }

    private static ScreenState.OffsetIndexView moved(ScreenState.OffsetIndexView state, int newSelection) {
        int newTop = RowWindow.adjustTop(state.scrollTop(), newSelection, Keys.viewportStride());
        return new ScreenState.OffsetIndexView(state.rowGroupIndex(), state.columnIndex(),
                newSelection, newTop);
    }

    public static void render(Buffer buffer, Rect area, ParquetModel model, ScreenState.OffsetIndexView state) {
        Keys.observeViewport(area.height() - 3);
        OffsetIndex oi = model.offsetIndex(state.rowGroupIndex(), state.columnIndex());
        if (oi == null) {
            Block emptyBlock = Block.builder()
                    .title(" Offset index ")
                    .borders(Borders.ALL)
                    .borderType(BorderType.ROUNDED)
                    .build();
            Paragraph.builder()
                    .block(emptyBlock)
                    .text(Text.from(Line.from(
                            new Span(" No offset index for this chunk.", Theme.dim()))))
                    .left()
                    .build()
                    .render(area, buffer);
            return;
        }
        List<PageLocation> locations = oi.pageLocations();
        // Build Row objects only for the visible window — see RowWindow.
        RowWindow window = RowWindow.from(state.scrollTop(), state.selection(),
                locations.size(), area.height() - 3);
        List<Row> rows = new ArrayList<>(window.size());
        for (int i = window.start(); i < window.end(); i++) {
            PageLocation loc = locations.get(i);
            rows.add(Row.from(
                    String.valueOf(i),
                    Fmt.fmt("%,d", loc.offset()),
                    Sizes.format(loc.compressedPageSize()),
                    Fmt.fmt("%,d", loc.firstRowIndex())));
        }
        Row header = Row.from("#", "Offset", "Size", "First row").style(Theme.accent().bold());
        Block block = Block.builder()
                .title(" Offset index "
                        + Plurals.rangeOf(state.selection(), locations.size(), Keys.viewportStride())
                        + " ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
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
                .highlightStyle(Theme.selection())
                .build();
        TableState tableState = new TableState();
        if (!locations.isEmpty()) {
            tableState.select(window.selectionInWindow());
        }
        table.render(area, buffer, tableState);
    }

    public static String keybarKeys(ScreenState.OffsetIndexView state, ParquetModel model) {
        OffsetIndex oi = model.offsetIndex(state.rowGroupIndex(), state.columnIndex());
        int count = oi != null ? oi.pageLocations().size() : 0;
        return new Keys.Hints()
                .add(count > 1, "[↑↓] move")
                .add(count > Keys.viewportStride(), "[PgDn/PgUp or Shift+↓↑] page")
                .add(count > 1, "[g/G] first/last")
                .add(true, "[Esc] back")
                .build();
    }
}
