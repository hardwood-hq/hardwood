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

/// Tabular view of all row groups in the file. Selecting a row drills into the
/// [ColumnChunksScreen] scoped to that row group.
public final class RowGroupsScreen {

    private RowGroupsScreen() {
    }

    public static boolean handle(KeyEvent event, ParquetModel model, NavigationStack stack) {
        ScreenState.RowGroups state = (ScreenState.RowGroups) stack.top();
        int count = model.rowGroupCount();
        if (event.isUp()) {
            stack.replaceTop(new ScreenState.RowGroups(Math.max(0, state.selection() - 1)));
            return true;
        }
        if (event.isDown()) {
            stack.replaceTop(new ScreenState.RowGroups(Math.min(count - 1, state.selection() + 1)));
            return true;
        }
        if (Keys.isJumpTop(event) && count > 0) {
            stack.replaceTop(new ScreenState.RowGroups(0));
            return true;
        }
        if (Keys.isJumpBottom(event) && count > 0) {
            stack.replaceTop(new ScreenState.RowGroups(count - 1));
            return true;
        }
        if (event.isConfirm() && count > 0) {
            stack.push(new ScreenState.ColumnChunks(state.selection(), 0));
            return true;
        }
        return false;
    }

    public static void render(Buffer buffer, Rect area, ParquetModel model, ScreenState.RowGroups state) {
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < model.rowGroupCount(); i++) {
            RowGroup rg = model.rowGroup(i);
            long compressed = 0;
            long uncompressed = 0;
            int ciCount = 0;
            int oiCount = 0;
            int chunkCount = rg.columns().size();
            for (ColumnChunk cc : rg.columns()) {
                ColumnMetaData cmd = cc.metaData();
                compressed += cmd.totalCompressedSize();
                uncompressed += cmd.totalUncompressedSize();
                if (cc.columnIndexOffset() != null) {
                    ciCount++;
                }
                if (cc.offsetIndexOffset() != null) {
                    oiCount++;
                }
            }
            double ratio = compressed == 0 ? 0.0 : (double) uncompressed / compressed;
            rows.add(Row.from(
                    String.valueOf(i),
                    formatLong(rg.numRows()),
                    Sizes.format(uncompressed),
                    Sizes.format(compressed),
                    String.format("%.1f×", ratio),
                    ciCount + "/" + chunkCount,
                    oiCount + "/" + chunkCount));
        }
        Row header = Row.from("#", "Rows", "Uncompressed", "Compressed", "Ratio", "CI", "OI")
                .style(Style.EMPTY.bold());
        Block block = Block.builder()
                .title(" Row groups (" + model.rowGroupCount() + ") ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .borderColor(Theme.ACCENT)
                .build();
        Table table = Table.builder()
                .header(header)
                .rows(rows)
                .widths(new Constraint.Length(4),
                        new Constraint.Length(14),
                        new Constraint.Length(14),
                        new Constraint.Length(14),
                        new Constraint.Length(8),
                        new Constraint.Length(8),
                        new Constraint.Length(8))
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
        return "[↑↓] move  [Enter] drill  [Esc] back";
    }

    private static String formatLong(long v) {
        if (v < 1000) {
            return Long.toString(v);
        }
        return String.format("%,d", v);
    }
}
