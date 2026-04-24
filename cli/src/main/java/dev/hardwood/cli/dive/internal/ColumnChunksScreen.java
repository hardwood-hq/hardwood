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

/// Columns within one row group, one row per chunk. Drills into
/// [ColumnChunkDetailScreen] on Enter.
public final class ColumnChunksScreen {

    private ColumnChunksScreen() {
    }

    public static boolean handle(KeyEvent event, ParquetModel model, NavigationStack stack) {
        ScreenState.ColumnChunks state = (ScreenState.ColumnChunks) stack.top();
        int count = model.rowGroup(state.rowGroupIndex()).columns().size();
        if (event.isUp()) {
            stack.replaceTop(new ScreenState.ColumnChunks(state.rowGroupIndex(), Math.max(0, state.selection() - 1)));
            return true;
        }
        if (event.isDown()) {
            stack.replaceTop(new ScreenState.ColumnChunks(state.rowGroupIndex(), Math.min(count - 1, state.selection() + 1)));
            return true;
        }
        if (Keys.isJumpTop(event) && count > 0) {
            stack.replaceTop(new ScreenState.ColumnChunks(state.rowGroupIndex(), 0));
            return true;
        }
        if (Keys.isJumpBottom(event) && count > 0) {
            stack.replaceTop(new ScreenState.ColumnChunks(state.rowGroupIndex(), count - 1));
            return true;
        }
        if (event.isConfirm() && count > 0) {
            stack.push(new ScreenState.ColumnChunkDetail(state.rowGroupIndex(), state.selection(),
                    ScreenState.ColumnChunkDetail.Pane.MENU, 0));
            return true;
        }
        return false;
    }

    public static void render(Buffer buffer, Rect area, ParquetModel model, ScreenState.ColumnChunks state) {
        RowGroup rg = model.rowGroup(state.rowGroupIndex());
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < rg.columns().size(); i++) {
            ColumnChunk cc = rg.columns().get(i);
            ColumnMetaData cmd = cc.metaData();
            double ratio = cmd.totalCompressedSize() == 0
                    ? 0.0
                    : (double) cmd.totalUncompressedSize() / cmd.totalCompressedSize();
            dev.hardwood.metadata.LogicalType logical = model.schema().getColumn(i).logicalType();
            rows.add(Row.from(
                    Sizes.columnPath(cmd),
                    cmd.type().name(),
                    logical != null ? logical.toString() : "—",
                    cmd.codec().name(),
                    Sizes.format(cmd.totalCompressedSize()),
                    String.format("%.1f×", ratio),
                    cmd.dictionaryPageOffset() != null ? "yes" : "no"));
        }
        Row header = Row.from("Column", "Type", "Logical", "Codec", "Compressed", "Ratio", "Dict")
                .style(Style.EMPTY.bold());
        Block block = Block.builder()
                .title(" RG #" + state.rowGroupIndex() + " column chunks ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .borderColor(Theme.ACCENT)
                .build();
        Table table = Table.builder()
                .header(header)
                .rows(rows)
                .widths(new Constraint.Fill(3),
                        new Constraint.Length(16),
                        new Constraint.Length(18),
                        new Constraint.Length(10),
                        new Constraint.Length(12),
                        new Constraint.Length(8),
                        new Constraint.Length(6))
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
        return "[↑↓] move  [Enter] inspect chunk  [Esc] back";
    }
}
