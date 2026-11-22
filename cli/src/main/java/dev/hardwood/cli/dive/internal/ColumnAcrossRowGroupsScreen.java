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
import java.util.Locale;

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
import dev.tamboui.layout.Layout;
import dev.tamboui.layout.Rect;
import dev.tamboui.text.Line;
import dev.tamboui.text.Span;
import dev.tamboui.text.Text;
import dev.tamboui.tui.event.KeyEvent;
import dev.tamboui.widgets.block.Block;
import dev.tamboui.widgets.block.BorderType;
import dev.tamboui.widgets.block.Borders;
import dev.tamboui.widgets.paragraph.Paragraph;
import dev.tamboui.widgets.table.Row;
import dev.tamboui.widgets.table.Table;
import dev.tamboui.widgets.table.TableState;

/// Cross-row-group view of one leaf column — one table row per row group.
/// Selecting a row drills into [ScreenState.ColumnChunkDetail] for that `(rg, col)`.
///
/// `/` enters inline search: the filter matches against each row group's
/// formatted min or max value (case-insensitive substring).
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
        DICT("Dict", 7),
        CI("CI", 7),
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

    /// Used by [DiveApp] to decide whether the screen should receive printable
    /// chars instead of the global keymap.
    public static boolean isInInputMode(ScreenState.ColumnAcrossRowGroups state) {
        return state.searching();
    }

    public static boolean handle(KeyEvent event, ParquetModel model, NavigationStack stack) {
        ScreenState.ColumnAcrossRowGroups state = (ScreenState.ColumnAcrossRowGroups) stack.top();
        if (state.searching()) {
            return handleSearching(event, state, stack);
        }
        List<Integer> filtered = filteredRowGroups(model, state);
        if (event.code() == dev.tamboui.tui.event.KeyCode.CHAR && event.character() == '/') {
            stack.replaceTop(with(state, 0, state.filter(), true));
            return true;
        }
        int count = filtered.size();
        boolean logical = state.logicalTypes();
        int next = CursorPane.select(event, state.selection(), count);
        if (next != CursorPane.UNHANDLED) {
            stack.replaceTop(moved(state, next, logical));
            return true;
        }
        if (event.isConfirm() && count > 0) {
            stack.push(ColumnChunkDetailScreen.initialState(
                    model, filtered.get(Math.min(state.selection(), count - 1)), state.columnIndex(), state.logicalTypes()));
            return true;
        }
        if (event.code() == dev.tamboui.tui.event.KeyCode.CHAR && event.character() == 't'
                && !event.hasCtrl() && !event.hasAlt()) {
            stack.replaceTop(new ScreenState.ColumnAcrossRowGroups(
                    state.columnIndex(), state.selection(), !logical, state.scrollTop(),
                    state.filter(), state.searching()));
            return true;
        }
        return false;
    }

    private static boolean handleSearching(KeyEvent event, ScreenState.ColumnAcrossRowGroups state,
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
        if (event.code() == dev.tamboui.tui.event.KeyCode.CHAR) {
            char c = event.character();
            if (c >= ' ' && c != 127) {
                stack.replaceTop(with(state, 0, state.filter() + c, true));
                return true;
            }
        }
        return false;
    }

    /// Row-group indexes whose formatted min or max contains the filter, in
    /// row-group order; every row group when the filter is empty. Selection
    /// indexes into this list, not into the row groups — the two agree only
    /// while unfiltered.
    private static List<Integer> filteredRowGroups(ParquetModel model,
                                                   ScreenState.ColumnAcrossRowGroups state) {
        List<Integer> out = new ArrayList<>();
        String needle = state.filter().toLowerCase(Locale.ROOT);
        ColumnSchema col = model.schema().getColumn(state.columnIndex());
        for (int i = 0; i < model.rowGroupCount(); i++) {
            if (needle.isEmpty()) {
                out.add(i);
                continue;
            }
            // Matched against the full rendering, not the cell: a search for
            // part of a long bound still finds its row group, even though
            // the cell shows only a marked prefix of it.
            ColumnMetaData cmd = model.rowGroup(i).columns().get(state.columnIndex()).metaData();
            Statistics stats = cmd.statistics();
            String min = formatStatFull(stats != null ? stats.minValue() : null, col,
                    state.logicalTypes()).toLowerCase(Locale.ROOT);
            String max = formatStatFull(stats != null ? stats.maxValue() : null, col,
                    state.logicalTypes()).toLowerCase(Locale.ROOT);
            if (min.contains(needle) || max.contains(needle)) {
                out.add(i);
            }
        }
        return out;
    }

    private static ScreenState.ColumnAcrossRowGroups with(ScreenState.ColumnAcrossRowGroups state,
                                                           int selection, String filter,
                                                           boolean searching) {
        return new ScreenState.ColumnAcrossRowGroups(state.columnIndex(), selection,
                state.logicalTypes(),
                RowWindow.adjustTop(state.scrollTop(), selection, Keys.viewportStride()),
                filter, searching);
    }

    private static ScreenState.ColumnAcrossRowGroups moved(ScreenState.ColumnAcrossRowGroups state,
                                                            int newSelection, boolean logical) {
        int newTop = RowWindow.adjustTop(state.scrollTop(), newSelection, Keys.viewportStride());
        return new ScreenState.ColumnAcrossRowGroups(state.columnIndex(), newSelection, logical, newTop,
                state.filter(), state.searching());
    }

    public static void render(Buffer buffer, Rect screenArea, ParquetModel model,
                              ScreenState.ColumnAcrossRowGroups state) {
        // Search bar (1) + block borders (2) + header (1) = 4 chrome rows;
        // the offset-index flag, when present, takes one more above the
        // search bar, so the warning stays the first line of the screen.
        List<Integer> filtered = filteredRowGroups(model, state);
        // The page counts come from the offset indexes as the file stores them;
        // the first visible row group whose index a read refuses is flagged.
        String violation = firstViolation(model, state, filtered,
                RowWindow.from(state.scrollTop(), state.selection(), filtered.size(), screenArea.height() - 4));
        Rect body = OffsetIndexFlag.above(buffer, screenArea, violation);
        Keys.observeViewport(body.height() - 4);
        List<Rect> split = Layout.vertical()
                .constraints(new Constraint.Length(1), new Constraint.Fill(1))
                .split(body);
        renderSearchBar(buffer, split.get(0), state, model.rowGroupCount(), filtered.size());
        Rect area = split.get(1);
        ColumnSchema col = model.schema().getColumn(state.columnIndex());
        // Build Row objects only for the visible window — see RowWindow.
        RowWindow window = RowWindow.from(state.scrollTop(), state.selection(),
                filtered.size(), area.height() - 3);
        List<Row> rows = new ArrayList<>(window.size());
        int statBudget = statBudget(area.width());
        for (int i = window.start(); i < window.end(); i++) {
            int rgIndex = filtered.get(i);
            RowGroup rg = model.rowGroup(rgIndex);
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
                    ? model.offsetIndex(rgIndex, state.columnIndex()) : null;
            String pages = oi != null ? Fmt.fmt("%,d", oi.pageLocations().size()) : Strings.ABSENT_VALUE;
            rows.add(Row.from(
                    String.valueOf(rgIndex),
                    Fmt.fmt("%,d", rg.numRows()),
                    pages,
                    Sizes.format(cmd.totalCompressedSize()),
                    Sizes.compression(cmd.totalCompressedSize(), cmd.totalUncompressedSize()),
                    summary.hasUnencoded() ? Sizes.format(summary.unencodedBytes()) : Strings.ABSENT_VALUE,
                    ColumnChunkDetailScreen.presence(cmd.dictionaryPageOffset() != null),
                    ColumnChunkDetailScreen.presence(cc.columnIndexOffset() != null),
                    nulls,
                    min,
                    max));
        }
        Row header = header();
        String typeMode = state.logicalTypes() ? "" : " · physical";
        Block block = Block.builder()
                .title(" " + truncateLeft(col.fieldPath().toString(), 40)
                        + " · RG "
                        + Plurals.rangeOf(window, filtered.size())
                        + (state.filter().isEmpty()
                                ? ""
                                : " · " + Plurals.format(model.rowGroupCount(), "row group", "row groups")
                                        + " total")
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
        if (!filtered.isEmpty()) {
            tableState.select(window.selectionInWindow());
        }
        table.render(area, buffer, tableState);
    }

    private static void renderSearchBar(Buffer buffer, Rect area, ScreenState.ColumnAcrossRowGroups state,
                                        int totalRowGroups, int matchCount) {
        if (!state.searching() && state.filter().isEmpty()) {
            Paragraph.builder()
                    .text(Text.from(Line.from(new Span(
                            " " + Plurals.format(totalRowGroups, "row group", "row groups")
                                    + ". Press / to filter by min/max.",
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
                        + Plurals.format(totalRowGroups, "row group", "row groups") + ")", Theme.dim()));
        Paragraph.builder().text(Text.from(line)).left().build().render(area, buffer);
    }

    /// The message a read fails with on the offset index of the first row group
    /// in `window` whose index its chunk contradicts, naming the row group, or
    /// `null` when there is none. Only the visible row groups are checked, as
    /// only theirs are read. `window` indexes into `filtered`, not into the
    /// row groups.
    private static String firstViolation(ParquetModel model, ScreenState.ColumnAcrossRowGroups state,
                                         List<Integer> filtered, RowWindow window) {
        for (int i = window.start(); i < window.end(); i++) {
            int rgIndex = filtered.get(i);
            if (model.rowGroup(rgIndex).columns().get(state.columnIndex()).offsetIndexOffset() == null) {
                continue;
            }
            String violation = model.offsetIndexViolation(rgIndex, state.columnIndex());
            if (violation != null) {
                return "RG #" + rgIndex + "  " + violation;
            }
        }
        return null;
    }

    public static String keybarKeys(ScreenState.ColumnAcrossRowGroups state, ParquetModel model) {
        List<Integer> filtered = filteredRowGroups(model, state);
        int count = filtered.size();
        ColumnSchema col = model.schema().getColumn(state.columnIndex());
        boolean hasLogical = col.logicalType() != null;
        return new Keys.Hints()
                .add(true, CursorPane.hints(count))
                .add(count > 0, "[Enter] open")
                .add(true, "[/] search")
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

    private static String formatStatFull(byte[] bytes, ColumnSchema col, boolean logical) {
        return bytes == null ? Strings.ABSENT_VALUE : ValueFormatter.formatBytes(bytes, col, logical);
    }

    private static String truncateLeft(String s, int maxWidth) {
        return Strings.truncateLeft(s, maxWidth);
    }
}
