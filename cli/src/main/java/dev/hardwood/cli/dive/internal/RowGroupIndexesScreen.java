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

/// Per-chunk index-region layout for one row group. One row per
/// (chunk, index-kind) pair — Column index and Offset index get a row
/// each when present. Selecting a row drills into the matching index
/// view via Enter; no special key needed for the secondary kind.
public final class RowGroupIndexesScreen {

    /// One displayable row: a particular chunk's CI or OI entry.
    private record Entry(int columnIndex, ColumnChunk chunk, Kind kind) {
        enum Kind { COLUMN, OFFSET }

        long offset() {
            return kind == Kind.COLUMN ? chunk.columnIndexOffset() : chunk.offsetIndexOffset();
        }

        long size() {
            Integer len = kind == Kind.COLUMN ? chunk.columnIndexLength() : chunk.offsetIndexLength();
            return len != null ? len.longValue() : 0L;
        }

        String typeLabel() {
            return kind == Kind.COLUMN ? "Column index" : "Offset index";
        }
    }

    private RowGroupIndexesScreen() {
    }

    public static boolean handle(KeyEvent event, ParquetModel model, NavigationStack stack) {
        ScreenState.RowGroupIndexes state = (ScreenState.RowGroupIndexes) stack.top();
        List<Entry> entries = entries(model.rowGroup(state.rowGroupIndex()));
        int count = entries.size();
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
        if (event.isConfirm() && count > 0) {
            Entry e = entries.get(Math.min(state.selection(), count - 1));
            switch (e.kind()) {
                case COLUMN -> stack.push(new ScreenState.ColumnIndexView(
                        state.rowGroupIndex(), e.columnIndex(), 0, "", false, true, false));
                case OFFSET -> stack.push(new ScreenState.OffsetIndexView(
                        state.rowGroupIndex(), e.columnIndex(), 0));
            }
            return true;
        }
        return false;
    }

    public static void render(Buffer buffer, Rect area, ParquetModel model, ScreenState.RowGroupIndexes state) {
        Keys.observeViewport(area.height() - 3);
        RowGroup rg = model.rowGroup(state.rowGroupIndex());
        List<Entry> entries = entries(rg);
        List<Row> rows = new ArrayList<>();
        for (Entry e : entries) {
            rows.add(Row.from(
                    Sizes.columnPath(e.chunk().metaData()),
                    e.typeLabel(),
                    String.format("%,d", e.offset()),
                    Sizes.format(e.size())));
        }
        Row header = Row.from("Column", "Index type", "Offset", "Size")
                .style(Style.EMPTY.bold());
        Block block = Block.builder()
                .title(" RG #" + state.rowGroupIndex() + " index regions "
                        + Plurals.rangeOf(state.selection(), entries.size(),
                                Keys.viewportStride()) + " ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .borderColor(Theme.ACCENT)
                .build();
        Table table = Table.builder()
                .header(header)
                .rows(rows)
                .widths(new Constraint.Fill(3),
                        new Constraint.Length(14),
                        new Constraint.Length(16),
                        new Constraint.Length(10))
                .columnSpacing(2)
                .block(block)
                .highlightSymbol("▶ ")
                .highlightStyle(Style.EMPTY.bold())
                .build();
        TableState tableState = new TableState();
        if (!entries.isEmpty()) {
            tableState.select(Math.min(state.selection(), entries.size() - 1));
        }
        table.render(area, buffer, tableState);
    }

    public static String keybarKeys(ScreenState.RowGroupIndexes state, ParquetModel model) {
        int count = entries(model.rowGroup(state.rowGroupIndex())).size();
        return new Keys.Hints()
                .add(count > 1, "[↑↓] move")
                .add(count > Keys.viewportStride(), "[PgDn/PgUp or Shift+↓↑] page")
                .add(count > 1, "[g/G] first/last")
                .add(count > 0, "[Enter] open")
                .add(true, "[Esc] back")
                .build();
    }

    private static List<Entry> entries(RowGroup rg) {
        List<Entry> out = new ArrayList<>();
        List<ColumnChunk> chunks = rg.columns();
        for (int i = 0; i < chunks.size(); i++) {
            ColumnChunk cc = chunks.get(i);
            if (cc.columnIndexOffset() != null) {
                out.add(new Entry(i, cc, Entry.Kind.COLUMN));
            }
            if (cc.offsetIndexOffset() != null) {
                out.add(new Entry(i, cc, Entry.Kind.OFFSET));
            }
        }
        return out;
    }
}
