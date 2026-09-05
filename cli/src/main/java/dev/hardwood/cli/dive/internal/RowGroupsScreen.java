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
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.tamboui.buffer.Buffer;
import dev.tamboui.layout.Constraint;
import dev.tamboui.layout.Rect;
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
        int next = CursorPane.select(event, state.selection(), count);
        if (next != CursorPane.UNHANDLED) {
            stack.replaceTop(moved(state, next));
            return true;
        }
        if (event.isConfirm() && count > 0) {
            stack.push(new ScreenState.RowGroupDetail(
                    state.selection(), ScreenState.RowGroupDetail.Pane.MENU, 0));
            return true;
        }
        return false;
    }

    private static ScreenState.RowGroups moved(ScreenState.RowGroups state, int newSelection) {
        int newTop = RowWindow.adjustTop(state.scrollTop(), newSelection, Keys.viewportStride());
        return new ScreenState.RowGroups(newSelection, newTop);
    }

    public static void render(Buffer buffer, Rect area, ParquetModel model, ScreenState.RowGroups state) {
        Keys.observeViewport(area.height() - 3);
        // Build Row objects only for the visible window — see RowWindow.
        RowWindow window = RowWindow.from(state.scrollTop(), state.selection(),
                model.rowGroupCount(), area.height() - 3);
        List<Row> rows = new ArrayList<>(window.size());
        for (int i = window.start(); i < window.end(); i++) {
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
            rows.add(Row.from(
                    String.valueOf(i),
                    formatLong(rg.numRows()),
                    Sizes.format(uncompressed),
                    Sizes.format(compressed),
                    Sizes.compression(compressed, uncompressed),
                    ciCount + "/" + chunkCount,
                    oiCount + "/" + chunkCount));
        }
        Row header = Row.from("#", "Rows", "Uncompressed", "Compressed", "Compression", "CI", "OI")
                .style(Theme.accent().bold());
        Block block = Block.builder()
                .title(" Row groups " + Plurals.rangeOf(window, model.rowGroupCount()) + " ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .build();
        Table table = Table.builder()
                .header(header)
                .rows(rows)
                .widths(new Constraint.Length(4),
                        new Constraint.Length(14),
                        new Constraint.Length(14),
                        new Constraint.Length(14),
                        new Constraint.Length(11),
                        new Constraint.Length(8),
                        new Constraint.Length(8))
                .columnSpacing(2)
                .block(block)
                .highlightSymbol("▶ ")
                .highlightStyle(Theme.selection())
                .build();
        TableState tableState = new TableState();
        tableState.select(window.selectionInWindow());
        table.render(area, buffer, tableState);
    }

    public static String keybarKeys(ScreenState.RowGroups state, ParquetModel model) {
        int count = model.rowGroupCount();
        return new Keys.Hints()
                .add(true, CursorPane.hints(count))
                .add(count > 0, "[Enter] open")
                .add(true, "[Esc] back")
                .build();
    }

    private static String formatLong(long v) {
        if (v < 1000) {
            return Long.toString(v);
        }
        return Fmt.fmt("%,d", v);
    }
}
