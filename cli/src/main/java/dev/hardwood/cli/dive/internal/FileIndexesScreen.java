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

/// File-wide list of every chunk that carries either a Column index or an
/// Offset index. Reachable from `Footer & indexes → Page indexes → {Column,
/// Offset} indexes` via Enter. Selecting a row drills into the existing
/// per-chunk `ColumnIndex` / `OffsetIndex` screen.
public final class FileIndexesScreen {

    private FileIndexesScreen() {
    }

    private record Entry(int rowGroupIndex, int columnIndex, ColumnChunk chunk) {
    }

    public static boolean handle(KeyEvent event, ParquetModel model, NavigationStack stack) {
        ScreenState.FileIndexes state = (ScreenState.FileIndexes) stack.top();
        List<Entry> entries = entries(model, state.kind());
        int count = entries.size();
        if (Keys.isStepUp(event) && count > 0) {
            stack.replaceTop(new ScreenState.FileIndexes(state.kind(),
                    Math.max(0, state.selection() - 1)));
            return true;
        }
        if (Keys.isStepDown(event) && count > 0) {
            stack.replaceTop(new ScreenState.FileIndexes(state.kind(),
                    Math.min(count - 1, state.selection() + 1)));
            return true;
        }
        if (Keys.isPageDown(event) && count > 0) {
            stack.replaceTop(new ScreenState.FileIndexes(state.kind(),
                    Math.min(count - 1, state.selection() + Keys.viewportStride())));
            return true;
        }
        if (Keys.isPageUp(event) && count > 0) {
            stack.replaceTop(new ScreenState.FileIndexes(state.kind(),
                    Math.max(0, state.selection() - Keys.viewportStride())));
            return true;
        }
        if (Keys.isJumpTop(event) && count > 0) {
            stack.replaceTop(new ScreenState.FileIndexes(state.kind(), 0));
            return true;
        }
        if (Keys.isJumpBottom(event) && count > 0) {
            stack.replaceTop(new ScreenState.FileIndexes(state.kind(), count - 1));
            return true;
        }
        if (event.isConfirm() && count > 0) {
            Entry e = entries.get(Math.min(state.selection(), count - 1));
            switch (state.kind()) {
                case COLUMN -> stack.push(new ScreenState.ColumnIndexView(
                        e.rowGroupIndex(), e.columnIndex(), 0, "", false, true, false));
                case OFFSET -> stack.push(new ScreenState.OffsetIndexView(
                        e.rowGroupIndex(), e.columnIndex(), 0));
                case DICTIONARY -> stack.push(new ScreenState.DictionaryView(
                        e.rowGroupIndex(), e.columnIndex(), 0, false, "", false, false, true));
            }
            return true;
        }
        return false;
    }

    public static void render(Buffer buffer, Rect area, ParquetModel model, ScreenState.FileIndexes state) {
        Keys.observeViewport(area.height() - 3);
        List<Entry> entries = entries(model, state.kind());
        List<Row> rows = new ArrayList<>();
        for (Entry e : entries) {
            ColumnChunk cc = e.chunk();
            ColumnMetaData cmd = cc.metaData();
            String columnPath = Sizes.columnPath(cmd);
            switch (state.kind()) {
                case COLUMN -> rows.add(Row.from(
                        String.valueOf(e.rowGroupIndex()),
                        columnPath,
                        String.format("%,d", cc.columnIndexOffset()),
                        Sizes.format(cc.columnIndexLength())));
                case OFFSET -> rows.add(Row.from(
                        String.valueOf(e.rowGroupIndex()),
                        columnPath,
                        String.format("%,d", cc.offsetIndexOffset()),
                        Sizes.format(cc.offsetIndexLength())));
                case DICTIONARY -> {
                    long dictOffset = cmd.dictionaryPageOffset();
                    long dataOffset = cmd.dataPageOffset();
                    long dictSpan = Math.max(0, dataOffset - dictOffset);
                    rows.add(Row.from(
                            String.valueOf(e.rowGroupIndex()),
                            columnPath,
                            String.format("%,d", dictOffset),
                            String.format("%,d", dataOffset),
                            Sizes.format(dictSpan)));
                }
            }
        }
        Row header;
        List<Constraint> widths;
        switch (state.kind()) {
            case COLUMN -> {
                header = Row.from("RG", "Column", "CI offset", "CI bytes")
                        .style(Style.EMPTY.bold());
                widths = List.of(
                        new Constraint.Length(4),
                        new Constraint.Fill(3),
                        new Constraint.Length(16),
                        new Constraint.Length(10));
            }
            case OFFSET -> {
                header = Row.from("RG", "Column", "OI offset", "OI bytes")
                        .style(Style.EMPTY.bold());
                widths = List.of(
                        new Constraint.Length(4),
                        new Constraint.Fill(3),
                        new Constraint.Length(16),
                        new Constraint.Length(10));
            }
            case DICTIONARY -> {
                header = Row.from("RG", "Column", "Dict offset", "Data offset", "Dict span")
                        .style(Style.EMPTY.bold());
                widths = List.of(
                        new Constraint.Length(4),
                        new Constraint.Fill(3),
                        new Constraint.Length(14),
                        new Constraint.Length(14),
                        new Constraint.Length(10));
            }
            default -> throw new IllegalStateException("kind: " + state.kind());
        }
        String range = Plurals.rangeOf(state.selection(), entries.size(), Keys.viewportStride());
        String title = switch (state.kind()) {
            case COLUMN -> " All column indexes " + range + " ";
            case OFFSET -> " All offset indexes " + range + " ";
            case DICTIONARY -> " All dictionaries " + range + " ";
        };
        Block block = Block.builder()
                .title(title)
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .borderColor(Theme.ACCENT)
                .build();
        Table table = Table.builder()
                .header(header)
                .rows(rows)
                .widths(widths.toArray(new Constraint[0]))
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

    public static String keybarKeys(ScreenState.FileIndexes state, ParquetModel model) {
        int count = entries(model, state.kind()).size();
        return new Keys.Hints()
                .add(count > 1, "[↑↓] move")
                .add(count > Keys.viewportStride(), "[PgDn/PgUp or Shift+↓↑] page")
                .add(count > 1, "[g/G] first/last")
                .add(count > 0, "[Enter] open")
                .add(true, "[Esc] back")
                .build();
    }

    private static List<Entry> entries(ParquetModel model, ScreenState.FileIndexes.Kind kind) {
        List<Entry> out = new ArrayList<>();
        List<RowGroup> rgs = model.metadata().rowGroups();
        for (int rg = 0; rg < rgs.size(); rg++) {
            List<ColumnChunk> chunks = rgs.get(rg).columns();
            for (int c = 0; c < chunks.size(); c++) {
                ColumnChunk cc = chunks.get(c);
                boolean has = switch (kind) {
                    case COLUMN -> cc.columnIndexOffset() != null && cc.columnIndexLength() != null;
                    case OFFSET -> cc.offsetIndexOffset() != null && cc.offsetIndexLength() != null;
                    case DICTIONARY -> cc.metaData().dictionaryPageOffset() != null;
                };
                if (has) {
                    out.add(new Entry(rg, c, cc));
                }
            }
        }
        return out;
    }
}
