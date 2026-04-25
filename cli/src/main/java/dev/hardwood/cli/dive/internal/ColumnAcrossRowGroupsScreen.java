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
import dev.hardwood.cli.internal.IndexValueFormatter;
import dev.hardwood.cli.internal.Sizes;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.schema.ColumnSchema;
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

/// Cross-row-group view of one leaf column — one table row per row group.
/// Selecting a row drills into [ColumnChunkDetail] for that `(rg, col)`.
public final class ColumnAcrossRowGroupsScreen {

    private ColumnAcrossRowGroupsScreen() {
    }

    public static boolean handle(KeyEvent event, ParquetModel model, NavigationStack stack) {
        ScreenState.ColumnAcrossRowGroups state = (ScreenState.ColumnAcrossRowGroups) stack.top();
        int count = model.rowGroupCount();
        boolean logical = state.logicalTypes();
        if (Keys.isStepUp(event)) {
            stack.replaceTop(new ScreenState.ColumnAcrossRowGroups(
                    state.columnIndex(), Math.max(0, state.selection() - 1), logical));
            return true;
        }
        if (Keys.isStepDown(event)) {
            stack.replaceTop(new ScreenState.ColumnAcrossRowGroups(
                    state.columnIndex(), Math.min(count - 1, state.selection() + 1), logical));
            return true;
        }
        if (Keys.isPageDown(event) && count > 0) {
            stack.replaceTop(new ScreenState.ColumnAcrossRowGroups(
                    state.columnIndex(),
                    Math.min(count - 1, state.selection() + Keys.viewportStride()), logical));
            return true;
        }
        if (Keys.isPageUp(event) && count > 0) {
            stack.replaceTop(new ScreenState.ColumnAcrossRowGroups(
                    state.columnIndex(),
                    Math.max(0, state.selection() - Keys.viewportStride()), logical));
            return true;
        }
        if (Keys.isJumpTop(event) && count > 0) {
            stack.replaceTop(new ScreenState.ColumnAcrossRowGroups(state.columnIndex(), 0, logical));
            return true;
        }
        if (Keys.isJumpBottom(event) && count > 0) {
            stack.replaceTop(new ScreenState.ColumnAcrossRowGroups(state.columnIndex(), count - 1, logical));
            return true;
        }
        if (event.isConfirm() && count > 0) {
            stack.push(new ScreenState.ColumnChunkDetail(
                    state.selection(), state.columnIndex(),
                    ScreenState.ColumnChunkDetail.Pane.MENU, 0, state.logicalTypes()));
            return true;
        }
        if (event.code() == dev.tamboui.tui.event.KeyCode.CHAR && event.character() == 't'
                && !event.hasCtrl() && !event.hasAlt()) {
            stack.replaceTop(new ScreenState.ColumnAcrossRowGroups(
                    state.columnIndex(), state.selection(), !logical));
            return true;
        }
        return false;
    }

    public static void render(Buffer buffer, Rect area, ParquetModel model, ScreenState.ColumnAcrossRowGroups state) {
        Keys.observeViewport(area.height() - 3);
        ColumnSchema col = model.schema().getColumn(state.columnIndex());
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < model.rowGroupCount(); i++) {
            RowGroup rg = model.rowGroup(i);
            ColumnChunk cc = rg.columns().get(state.columnIndex());
            ColumnMetaData cmd = cc.metaData();
            Statistics stats = cmd.statistics();
            String min = stats != null && stats.minValue() != null
                    ? IndexValueFormatter.format(stats.minValue(), col, state.logicalTypes())
                    : "—";
            String max = stats != null && stats.maxValue() != null
                    ? IndexValueFormatter.format(stats.maxValue(), col, state.logicalTypes())
                    : "—";
            String nulls = stats != null && stats.nullCount() != null
                    ? String.format("%,d", stats.nullCount())
                    : "—";
            double ratio = cmd.totalCompressedSize() == 0
                    ? 0.0
                    : (double) cmd.totalUncompressedSize() / cmd.totalCompressedSize();
            // Page count from OffsetIndex if present; without OI we'd need
            // to walk page headers, which the chunk-detail screen does
            // already — render "—" here.
            dev.hardwood.metadata.OffsetIndex oi = cc.offsetIndexOffset() != null
                    ? model.offsetIndex(i, state.columnIndex()) : null;
            String pages = oi != null ? String.format("%,d", oi.pageLocations().size()) : "—";
            rows.add(Row.from(
                    String.valueOf(i),
                    String.format("%,d", rg.numRows()),
                    pages,
                    Sizes.format(cmd.totalCompressedSize()),
                    String.format("%.1f×", ratio),
                    cmd.dictionaryPageOffset() != null ? "yes" : "no",
                    cc.columnIndexOffset() != null ? "yes" : "no",
                    nulls,
                    min,
                    max));
        }
        Row header = Row.from("RG", "Rows", "Pages", "Comp", "Ratio", "Dict", "CI", "Nulls", "Min", "Max")
                .style(Style.EMPTY.bold());
        String typeMode = state.logicalTypes() ? "" : " · physical";
        Block block = Block.builder()
                .title(" " + truncateLeft(col.fieldPath().toString(), 40)
                        + " · RG "
                        + Plurals.rangeOf(state.selection(), model.rowGroupCount(),
                                Keys.viewportStride())
                        + typeMode + " ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .borderColor(Theme.ACCENT)
                .build();
        Table table = Table.builder()
                .header(header)
                .rows(rows)
                .widths(new Constraint.Length(4),
                        new Constraint.Length(12),
                        new Constraint.Length(8),
                        new Constraint.Length(12),
                        new Constraint.Length(6),
                        new Constraint.Length(5),
                        new Constraint.Length(5),
                        new Constraint.Length(10),
                        new Constraint.Fill(1),
                        new Constraint.Fill(1))
                .columnSpacing(1)
                .block(block)
                .highlightSymbol("▶ ")
                .highlightStyle(Style.EMPTY.bold())
                .build();
        TableState tableState = new TableState();
        tableState.select(state.selection());
        table.render(area, buffer, tableState);
    }

    public static String keybarKeys(ScreenState.ColumnAcrossRowGroups state, ParquetModel model) {
        int count = model.rowGroupCount();
        ColumnSchema col = model.schema().getColumn(state.columnIndex());
        boolean hasLogical = col.logicalType() != null;
        return new Keys.Hints()
                .add(count > 1, "[↑↓] move")
                .add(count > Keys.viewportStride(), "[PgDn/PgUp or Shift+↓↑] page")
                .add(count > 1, "[g/G] first/last")
                .add(count > 0, "[Enter] chunk detail")
                .add(hasLogical, "[t] logical types")
                .add(true, "[Esc] back")
                .build();
    }

    private static String truncateLeft(String s, int maxWidth) {
        if (s.length() <= maxWidth) {
            return s;
        }
        return "…" + s.substring(s.length() - maxWidth + 1);
    }
}
