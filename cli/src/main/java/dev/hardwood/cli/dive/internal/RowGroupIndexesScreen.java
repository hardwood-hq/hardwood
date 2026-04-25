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
        if (Keys.isStepUp(event)) {
            stack.replaceTop(new ScreenState.RowGroupIndexes(
                    state.rowGroupIndex(), Math.max(0, state.selection() - 1)));
            return true;
        }
        if (Keys.isStepDown(event)) {
            stack.replaceTop(new ScreenState.RowGroupIndexes(
                    state.rowGroupIndex(), Math.min(count - 1, state.selection() + 1)));
            return true;
        }
        if (Keys.isPageDown(event) && count > 0) {
            stack.replaceTop(new ScreenState.RowGroupIndexes(state.rowGroupIndex(),
                    Math.min(count - 1, state.selection() + Keys.viewportStride())));
            return true;
        }
        if (Keys.isPageUp(event) && count > 0) {
            stack.replaceTop(new ScreenState.RowGroupIndexes(state.rowGroupIndex(),
                    Math.max(0, state.selection() - Keys.viewportStride())));
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
        // Enter drills into the most useful index for this chunk —
        // ColumnIndex if present (the predicate-pushdown table), else
        // OffsetIndex if present (the page-location table). The chunk's
        // full detail menu (Pages / Dictionary / etc.) is reachable via
        // Row groups → Column chunks instead — that path is the right
        // place when the user wants chunk metadata broadly. From the
        // Indexes view we stay in indexes.
        // `o` drills into OffsetIndex specifically when both are present.
        ColumnChunk selectedChunk = count > 0
                ? model.rowGroup(state.rowGroupIndex()).columns().get(state.selection())
                : null;
        if (event.isConfirm() && selectedChunk != null) {
            if (selectedChunk.columnIndexOffset() != null) {
                stack.push(new ScreenState.ColumnIndexView(
                        state.rowGroupIndex(), state.selection(), 0, "", false, true, false));
                return true;
            }
            if (selectedChunk.offsetIndexOffset() != null) {
                stack.push(new ScreenState.OffsetIndexView(
                        state.rowGroupIndex(), state.selection(), 0));
                return true;
            }
            return false;
        }
        if (event.code() == dev.tamboui.tui.event.KeyCode.CHAR && event.character() == 'o'
                && !event.hasCtrl() && !event.hasAlt()
                && selectedChunk != null && selectedChunk.offsetIndexOffset() != null) {
            stack.push(new ScreenState.OffsetIndexView(
                    state.rowGroupIndex(), state.selection(), 0));
            return true;
        }
        return false;
    }

    public static void render(Buffer buffer, Rect area, ParquetModel model, ScreenState.RowGroupIndexes state) {
        Keys.observeViewport(area.height() - 3);
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

    public static String keybarKeys(ScreenState.RowGroupIndexes state, ParquetModel model) {
        java.util.List<ColumnChunk> chunks = model.rowGroup(state.rowGroupIndex()).columns();
        int count = chunks.size();
        ColumnChunk selected = count > 0 && state.selection() < count
                ? chunks.get(state.selection()) : null;
        boolean hasCi = selected != null && selected.columnIndexOffset() != null;
        boolean hasOi = selected != null && selected.offsetIndexOffset() != null;
        return new Keys.Hints()
                .add(count > 1, "[↑↓] move")
                .add(count > Keys.viewportStride(), "[PgDn/PgUp or Shift+↓↑] page")
                .add(count > 1, "[g/G] first/last")
                .add(hasCi, "[Enter] column index")
                .add(hasCi && hasOi, "[o] offset index")
                .add(!hasCi && hasOi, "[Enter] offset index")
                .add(true, "[Esc] back")
                .build();
    }
}
