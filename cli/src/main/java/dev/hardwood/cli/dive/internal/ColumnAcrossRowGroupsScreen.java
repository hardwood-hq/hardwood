/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.dive.internal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import dev.hardwood.cli.dive.NavigationStack;
import dev.hardwood.cli.dive.ParquetModel;
import dev.hardwood.cli.dive.ScreenState;
import dev.hardwood.cli.internal.Fmt;
import dev.hardwood.cli.internal.LevelSummary;
import dev.hardwood.cli.internal.Sizes;
import dev.hardwood.cli.internal.Strings;
import dev.hardwood.cli.internal.ValueFormatter;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.OffsetIndex;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.schema.ColumnSchema;
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

/// Cross-row-group view of one leaf column — one table row per row group.
/// Selecting a row drills into [ScreenState.ColumnChunkDetail] for that `(rg, col)`.
public final class ColumnAcrossRowGroupsScreen {

    /// The fixed-width columns, in display order. Header label and cell width
    /// travel together so they cannot drift, and the widths feed [#statBudget]
    /// rather than being restated there as literals.
    private enum Col {
        RG("RG", 4),
        ROWS("Rows", 10),
        PAGES("Pages", 7),
        COMP("Comp", 11),
        COMPRESSION("Compression", 11),
        UNENCODED("Unencoded", 11),
        DICT("Dict", 5),
        CI("CI", 5),
        NULLS("Nulls", 9);

        private final String label;
        private final int width;

        Col(String label, int width) {
            this.label = label;
            this.width = width;
        }
    }

    /// Cached so rendering a frame does not clone the constant pool array.
    private static final Col[] COLUMNS = Col.values();
    /// Min and Max, appended when the chunk carries statistics. They fill the
    /// width the fixed columns leave, so they are not part of [Col].
    private static final String[] STAT_LABELS = {"Min", "Max"};
    private static final int TABLE_BORDER_WIDTH = 2;
    private static final int HIGHLIGHT_SYMBOL_WIDTH = 2;

    private ColumnAcrossRowGroupsScreen() {
    }

    public static boolean handle(KeyEvent event, ParquetModel model, NavigationStack stack) {
        ScreenState.ColumnAcrossRowGroups state = (ScreenState.ColumnAcrossRowGroups) stack.top();
        int count = model.rowGroupCount();
        boolean logical = state.logicalTypes();
        int next = CursorPane.select(event, state.selection(), count);
        if (next != CursorPane.UNHANDLED) {
            stack.replaceTop(moved(state, next, logical));
            return true;
        }
        if (event.isConfirm() && count > 0) {
            stack.push(ColumnChunkDetailScreen.initialState(
                    model, state.selection(), state.columnIndex(), state.logicalTypes()));
            return true;
        }
        if (event.code() == dev.tamboui.tui.event.KeyCode.CHAR && event.character() == 't'
                && !event.hasCtrl() && !event.hasAlt()) {
            stack.replaceTop(new ScreenState.ColumnAcrossRowGroups(
                    state.columnIndex(), state.selection(), !logical, state.scrollTop()));
            return true;
        }
        return false;
    }

    private static ScreenState.ColumnAcrossRowGroups moved(ScreenState.ColumnAcrossRowGroups state,
                                                            int newSelection, boolean logical) {
        int newTop = RowWindow.adjustTop(state.scrollTop(), newSelection, Keys.viewportStride());
        return new ScreenState.ColumnAcrossRowGroups(state.columnIndex(), newSelection, logical, newTop);
    }

    public static void render(Buffer buffer, Rect area, ParquetModel model, ScreenState.ColumnAcrossRowGroups state) {
        Keys.observeViewport(area.height() - 3);
        ColumnSchema col = model.schema().getColumn(state.columnIndex());
        // Build Row objects only for the visible window — see RowWindow.
        RowWindow window = RowWindow.from(state.scrollTop(), state.selection(),
                model.rowGroupCount(), area.height() - 3);
        List<Row> rows = new ArrayList<>(window.size());
        int statBudget = statBudget(area.width());
        for (int i = window.start(); i < window.end(); i++) {
            RowGroup rg = model.rowGroup(i);
            ColumnChunk cc = rg.columns().get(state.columnIndex());
            ColumnMetaData cmd = cc.metaData();
            Statistics stats = cmd.statistics();
            String min = stats != null && stats.minValue() != null
                    ? formatStat(stats.minValue(), col, state.logicalTypes(), statBudget)
                    : Strings.ABSENT_VALUE;
            String max = stats != null && stats.maxValue() != null
                    ? formatStat(stats.maxValue(), col, state.logicalTypes(), statBudget)
                    : Strings.ABSENT_VALUE;
            // This screen is the interactive twin of `inspect columns --column`:
            // one row per row group for one column. The unencoded size belongs
            // here for the same reason it belongs there — it is what says
            // whether a chunk is large because of its values or its encoding.
            LevelSummary summary = LevelSummary.of(model.schema(), col, cmd);
            long nullCount = summary.nullCount(stats);
            String nulls = nullCount >= 0 ? Fmt.fmt("%,d", nullCount) : Strings.ABSENT_VALUE;
            // Page count from OffsetIndex if present; without OI we'd need
            // to walk page headers, which the chunk-detail screen does
            // already — render "—" here.
            OffsetIndex oi = cc.offsetIndexOffset() != null
                    ? model.offsetIndex(i, state.columnIndex()) : null;
            String pages = oi != null ? Fmt.fmt("%,d", oi.pageLocations().size()) : Strings.ABSENT_VALUE;
            rows.add(Row.from(
                    String.valueOf(i),
                    Fmt.fmt("%,d", rg.numRows()),
                    pages,
                    Sizes.format(cmd.totalCompressedSize()),
                    Sizes.compression(cmd.totalCompressedSize(), cmd.totalUncompressedSize()),
                    summary.hasUnencoded() ? Sizes.format(summary.unencodedBytes()) : Strings.ABSENT_VALUE,
                    cmd.dictionaryPageOffset() != null ? "yes" : "no",
                    cc.columnIndexOffset() != null ? "yes" : "no",
                    nulls,
                    min,
                    max));
        }
        Row header = header();
        String typeMode = state.logicalTypes() ? "" : " · physical";
        Block block = Block.builder()
                .title(" " + truncateLeft(col.fieldPath().toString(), 40)
                        + " · RG "
                        + Plurals.rangeOf(window, model.rowGroupCount())
                        + typeMode + " ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .build();
        Table table = Table.builder()
                .header(header)
                .rows(rows)
                .widths(widths())
                .columnSpacing(1)
                .block(block)
                .highlightSymbol("▶ ")
                .highlightStyle(Theme.selection())
                .build();
        TableState tableState = new TableState();
        tableState.select(window.selectionInWindow());
        table.render(area, buffer, tableState);
    }

    public static String keybarKeys(ScreenState.ColumnAcrossRowGroups state, ParquetModel model) {
        int count = model.rowGroupCount();
        ColumnSchema col = model.schema().getColumn(state.columnIndex());
        boolean hasLogical = col.logicalType() != null;
        return new Keys.Hints()
                .add(true, CursorPane.hints(count))
                .add(count > 0, "[Enter] open")
                .add(hasLogical, "[t] logical types")
                .add(true, "[Esc] back")
                .build();
    }

    /// Used by the table cells, where the two Min/Max columns share whatever's
    /// left after the fixed-width columns. Compute that width before rendering
    /// and mark the cut with `…`.
    private static int statBudget(int viewportWidth) {
        int fixedColumns = Arrays.stream(COLUMNS).mapToInt(c -> c.width).sum();
        int gaps = COLUMNS.length + STAT_LABELS.length - 1;
        return Math.max(1, (viewportWidth - fixedColumns - gaps
                - TABLE_BORDER_WIDTH - HIGHLIGHT_SYMBOL_WIDTH) / STAT_LABELS.length);
    }

    private static Row header() {
        String[] labels = new String[COLUMNS.length + STAT_LABELS.length];
        // The header is two label sources laid end to end: the fixed columns
        // first, then Min / Max. The index alone tells which one to read from.
        Arrays.setAll(labels, i -> i < COLUMNS.length
                ? COLUMNS[i].label
                : STAT_LABELS[i - COLUMNS.length]);
        return Row.from(labels).style(Theme.accent().bold());
    }

    private static Constraint[] widths() {
        Constraint[] widths = new Constraint[COLUMNS.length + STAT_LABELS.length];
        // Same two sources as [#header]: fixed widths first, then Min / Max,
        // which take equal shares of what is left.
        Arrays.setAll(widths, i -> i < COLUMNS.length
                ? new Constraint.Length(COLUMNS[i].width)
                : new Constraint.Fill(1));
        return widths;
    }

    private static String formatStat(byte[] bytes, ColumnSchema col, boolean logical, int budget) {
        if (bytes == null) {
            return Strings.ABSENT_VALUE;
        }
        String full = ValueFormatter.formatBytes(bytes, col, logical, budget);
        return Strings.truncateRight(full, budget);
    }

    private static String truncateLeft(String s, int maxWidth) {
        return Strings.truncateLeft(s, maxWidth);
    }
}
