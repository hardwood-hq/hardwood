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
import java.util.Locale;

import dev.hardwood.cli.dive.NavigationStack;
import dev.hardwood.cli.dive.ParquetModel;
import dev.hardwood.cli.dive.ScreenState;
import dev.hardwood.cli.internal.Fmt;
import dev.hardwood.cli.internal.Sizes;
import dev.hardwood.cli.internal.Strings;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.tamboui.buffer.Buffer;
import dev.tamboui.layout.Constraint;
import dev.tamboui.layout.Layout;
import dev.tamboui.layout.Rect;
import dev.tamboui.text.Line;
import dev.tamboui.text.Span;
import dev.tamboui.text.Text;
import dev.tamboui.tui.event.KeyCode;
import dev.tamboui.tui.event.KeyEvent;
import dev.tamboui.widgets.block.Block;
import dev.tamboui.widgets.block.BorderType;
import dev.tamboui.widgets.block.Borders;
import dev.tamboui.widgets.paragraph.Paragraph;
import dev.tamboui.widgets.table.Row;
import dev.tamboui.widgets.table.Table;
import dev.tamboui.widgets.table.TableState;

/// Columns within one row group, one row per chunk. Drills into
/// [ColumnChunkDetailScreen] on Enter.
///
/// `/` enters inline search: the filter matches against each chunk's column
/// path (case-insensitive substring).
public final class ColumnChunksScreen {

    private ColumnChunksScreen() {
    }

    /// Used by [DiveApp] to decide whether the screen should receive printable
    /// chars instead of the global keymap.
    public static boolean isInInputMode(ScreenState.ColumnChunks state) {
        return state.searching();
    }

    public static boolean handle(KeyEvent event, ParquetModel model, NavigationStack stack) {
        ScreenState.ColumnChunks state = (ScreenState.ColumnChunks) stack.top();
        if (state.searching()) {
            return handleSearching(event, state, stack);
        }
        List<Integer> filtered = filteredColumns(model, state);
        if (event.code() == KeyCode.CHAR && event.character() == '/') {
            stack.replaceTop(with(state, 0, state.filter(), true));
            return true;
        }
        int next = CursorPane.select(event, state.selection(), filtered.size());
        if (next != CursorPane.UNHANDLED) {
            stack.replaceTop(with(state, next, state.filter(), false));
            return true;
        }
        if (event.isConfirm() && !filtered.isEmpty()) {
            stack.push(ColumnChunkDetailScreen.initialState(
                    model, state.rowGroupIndex(), filtered.get(Math.min(state.selection(), filtered.size() - 1)), true));
            return true;
        }
        return false;
    }

    private static boolean handleSearching(KeyEvent event, ScreenState.ColumnChunks state,
                                           NavigationStack stack) {
        if (event.isCancel()) {
            stack.replaceTop(with(state, 0, "", false));
            return true;
        }
        if (event.isConfirm()) {
            stack.replaceTop(with(state, 0, state.filter(), false));
            return true;
        }
        if (event.isDeleteBackward()) {
            String f = state.filter();
            String next = f.isEmpty() ? f : f.substring(0, f.length() - 1);
            stack.replaceTop(with(state, 0, next, true));
            return true;
        }
        if (event.code() == KeyCode.CHAR) {
            char c = event.character();
            if (c >= ' ' && c != 127) {
                stack.replaceTop(with(state, 0, state.filter() + c, true));
                return true;
            }
        }
        return false;
    }

    /// Column indexes whose path contains the filter, in schema order; every
    /// index when the filter is empty. Selection indexes into this list, not
    /// into the row group's columns — the two agree only while unfiltered.
    private static List<Integer> filteredColumns(ParquetModel model, ScreenState.ColumnChunks state) {
        RowGroup rg = model.rowGroup(state.rowGroupIndex());
        List<Integer> out = new ArrayList<>();
        String needle = state.filter().toLowerCase(Locale.ROOT);
        for (int i = 0; i < rg.columns().size(); i++) {
            if (needle.isEmpty()
                    || Sizes.columnPath(rg.columns().get(i).metaData())
                            .toLowerCase(Locale.ROOT).contains(needle)) {
                out.add(i);
            }
        }
        return out;
    }

    private static ScreenState.ColumnChunks with(ScreenState.ColumnChunks state,
                                                  int selection, String filter, boolean searching) {
        return new ScreenState.ColumnChunks(state.rowGroupIndex(), selection,
                RowWindow.adjustTop(state.scrollTop(), selection, Keys.viewportStride()),
                filter, searching);
    }

    public static void render(Buffer buffer, Rect area, ParquetModel model, ScreenState.ColumnChunks state) {
        // Search bar (1) + block borders (2) + header (1) = 4 chrome rows.
        Keys.observeViewport(area.height() - 4);
        RowGroup rg = model.rowGroup(state.rowGroupIndex());
        List<Integer> filtered = filteredColumns(model, state);

        List<Rect> split = Layout.vertical()
                .constraints(new Constraint.Length(1), new Constraint.Fill(1))
                .split(area);

        renderSearchBar(buffer, split.get(0), state, rg.columns().size(), filtered.size());

        // Build Row objects only for the visible window — see RowWindow.
        RowWindow window = RowWindow.from(state.scrollTop(), state.selection(),
                filtered.size(), split.get(1).height() - 3);
        List<Row> rows = new ArrayList<>(window.size());
        for (int i = window.start(); i < window.end(); i++) {
            ColumnChunk cc = rg.columns().get(filtered.get(i));
            ColumnMetaData cmd = cc.metaData();
            dev.hardwood.metadata.LogicalType logical = model.schema().getColumn(filtered.get(i)).logicalType();
            rows.add(Row.from(
                    String.valueOf(filtered.get(i)),
                    Sizes.columnPath(cmd),
                    cmd.type().name(),
                    logical != null ? logical.toString() : Strings.ABSENT_VALUE,
                    cmd.codec().name(),
                    Sizes.format(cmd.totalCompressedSize()),
                    Sizes.compression(cmd.totalCompressedSize(), cmd.totalUncompressedSize()),
                    cmd.dictionaryPageOffset() != null ? "yes" : "no"));
        }
        Row header = Row.from("#", "Column", "Type", "Logical", "Codec", "Compressed", "Compression", "Dict")
                .style(Theme.accent().bold());
        Block block = Block.builder()
                .title(" RG #" + state.rowGroupIndex() + " column chunks "
                        + Plurals.rangeOf(window, filtered.size())
                        + (state.filter().isEmpty()
                                ? ""
                                : " · " + Plurals.format(rg.columns().size(), "column", "columns") + " total")
                        + " ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .build();
        Table table = Table.builder()
                .header(header)
                .rows(rows)
                .widths(new Constraint.Length(4),
                        new Constraint.Fill(3),
                        new Constraint.Length(16),
                        new Constraint.Length(18),
                        new Constraint.Length(10),
                        new Constraint.Length(12),
                        new Constraint.Length(11),
                        new Constraint.Length(6))
                .columnSpacing(2)
                .block(block)
                .highlightSymbol("▶ ")
                .highlightStyle(Theme.selection())
                .build();
        TableState tableState = new TableState();
        if (!filtered.isEmpty()) {
            tableState.select(window.selectionInWindow());
        }
        table.render(split.get(1), buffer, tableState);
    }

    private static void renderSearchBar(Buffer buffer, Rect area, ScreenState.ColumnChunks state,
                                        int totalColumns, int matchCount) {
        if (!state.searching() && state.filter().isEmpty()) {
            Paragraph.builder()
                    .text(Text.from(Line.from(new Span(
                            " " + Plurals.format(totalColumns, "column", "columns")
                                    + ". Press / to filter by column name.",
                            Theme.dim()))))
                    .left()
                    .build()
                    .render(area, buffer);
            return;
        }
        String cursor = state.searching() ? "█" : "";
        Line line = Line.from(
                new Span(" / ", Theme.primary()),
                new Span(state.filter() + cursor, Theme.primary()),
                new Span("  (" + Fmt.fmt("%,d", matchCount) + " / "
                        + Plurals.format(totalColumns, "column", "columns") + ")", Theme.dim()));
        Paragraph.builder().text(Text.from(line)).left().build().render(area, buffer);
    }

    public static String keybarKeys(ScreenState.ColumnChunks state, ParquetModel model) {
        List<Integer> filtered = filteredColumns(model, state);
        int count = filtered.size();
        return new Keys.Hints()
                .add(true, CursorPane.hints(count))
                .add(count > 0, "[Enter] open")
                .add(true, "[/] search")
                .add(true, "[Esc] back")
                .build();
    }
}
