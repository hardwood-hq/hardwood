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
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.tamboui.buffer.Buffer;
import dev.tamboui.layout.Constraint;
import dev.tamboui.layout.Rect;
import dev.tamboui.style.Style;
import dev.tamboui.tui.event.KeyEvent;
import dev.tamboui.widgets.block.Block;
import dev.tamboui.widgets.block.BorderType;
import dev.tamboui.widgets.block.Borders;
import dev.tamboui.widgets.table.Row;
import dev.tamboui.widgets.table.Table;
import dev.tamboui.widgets.table.TableState;

/// Per-chunk index-region layout for one row group. Surfaces the file
/// offset + size of each chunk's ColumnIndex and OffsetIndex — the
/// data the Parquet footer stores contiguously but which the rest of
/// the TUI only exposes as aggregate byte counts (Footer screen) or
/// coverage ratios (RowGroups list). Bloom-filter columns will land
/// here once #325 lands.
public final class RowGroupIndexesScreen {

    private RowGroupIndexesScreen() {
    }

    public static boolean handle(KeyEvent event, ParquetModel model, NavigationStack stack) {
        ScreenState.RowGroupIndexes state = (ScreenState.RowGroupIndexes) stack.top();
        int count = model.rowGroup(state.rowGroupIndex()).columns().size();
        if (event.isUp()) {
            stack.replaceTop(new ScreenState.RowGroupIndexes(
                    state.rowGroupIndex(), Math.max(0, state.selection() - 1)));
            return true;
        }
        if (event.isDown()) {
            stack.replaceTop(new ScreenState.RowGroupIndexes(
                    state.rowGroupIndex(), Math.min(count - 1, state.selection() + 1)));
            return true;
        }
        if (Keys.isJumpTop(event) && count > 0) {
            stack.replaceTop(new ScreenState.RowGroupIndexes(state.rowGroupIndex(), 0));
            return true;
        }
        if (Keys.isJumpBottom(event) && count > 0) {
            stack.replaceTop(new ScreenState.RowGroupIndexes(state.rowGroupIndex(), count - 1));
            return true;
        }
        if (event.isConfirm() && count > 0) {
            stack.push(new ScreenState.ColumnChunkDetail(
                    state.rowGroupIndex(), state.selection(),
                    ScreenState.ColumnChunkDetail.Pane.MENU, 0));
            return true;
        }
        return false;
    }

    public static void render(Buffer buffer, Rect area, ParquetModel model, ScreenState.RowGroupIndexes state) {
        RowGroup rg = model.rowGroup(state.rowGroupIndex());
        List<Row> rows = new ArrayList<>();
        for (ColumnChunk cc : rg.columns()) {
            ColumnMetaData cmd = cc.metaData();
            rows.add(Row.from(
                    Sizes.columnPath(cmd),
                    cc.columnIndexOffset() != null ? String.format("%,d", cc.columnIndexOffset()) : "—",
                    cc.columnIndexLength() != null ? Sizes.format(cc.columnIndexLength()) : "—",
                    cc.offsetIndexOffset() != null ? String.format("%,d", cc.offsetIndexOffset()) : "—",
                    cc.offsetIndexLength() != null ? Sizes.format(cc.offsetIndexLength()) : "—"));
        }
        Row header = Row.from("Column", "CI offset", "CI bytes", "OI offset", "OI bytes")
                .style(Style.EMPTY.bold());
        Block block = Block.builder()
                .title(" RG #" + state.rowGroupIndex() + " index regions ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .borderColor(Theme.ACCENT)
                .build();
        Table table = Table.builder()
                .header(header)
                .rows(rows)
                .widths(new Constraint.Fill(3),
                        new Constraint.Length(16),
                        new Constraint.Length(10),
                        new Constraint.Length(16),
                        new Constraint.Length(10))
                .columnSpacing(2)
                .block(block)
                .highlightSymbol("▶ ")
                .highlightStyle(Style.EMPTY.bold())
                .build();
        TableState tableState = new TableState();
        tableState.select(state.selection());
        table.render(area, buffer, tableState);
    }

    public static String keybarKeys() {
        return "[↑↓] move  [Enter] chunk detail  [Esc] back";
    }
}
