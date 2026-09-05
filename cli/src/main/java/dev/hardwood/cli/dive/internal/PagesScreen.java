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
import dev.hardwood.cli.internal.Sizes;
import dev.hardwood.cli.internal.Strings;
import dev.hardwood.cli.internal.ValueFormatter;
import dev.hardwood.internal.metadata.DataPageHeader;
import dev.hardwood.internal.metadata.DataPageHeaderV2;
import dev.hardwood.internal.metadata.DictionaryPageHeader;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.metadata.ColumnIndex;
import dev.hardwood.metadata.OffsetIndex;
import dev.hardwood.metadata.PageLocation;
import dev.hardwood.metadata.PageType;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.schema.ColumnSchema;
import dev.tamboui.buffer.Buffer;
import dev.tamboui.layout.Constraint;
import dev.tamboui.layout.Rect;
import dev.tamboui.style.Style;
import dev.tamboui.text.Line;
import dev.tamboui.text.Span;
import dev.tamboui.tui.event.KeyEvent;
import dev.tamboui.widgets.block.Block;
import dev.tamboui.widgets.block.BorderType;
import dev.tamboui.widgets.block.Borders;
import dev.tamboui.widgets.table.Row;
import dev.tamboui.widgets.table.Table;
import dev.tamboui.widgets.table.TableState;

/// Lists data + dictionary pages for one column chunk. Enter opens a modal with
/// the full thrift page header; Esc in the modal closes it, Esc on the list
/// pops back to Column chunk detail.
public final class PagesScreen {

    /// The fixed-width columns, in display order. Header label and cell width
    /// travel together so they cannot drift, and the widths feed [#statBudget]
    /// rather than being restated there as a literal.
    private enum Col {
        NUM("#", 4),
        TYPE("Type", 16),
        FIRST_ROW("First row", 12),
        VALUES("Values", 10),
        /// Fits `DELTA_LENGTH_BYTE_ARRAY`, the longest encoding name.
        ENCODING("Encoding", 23),
        COMP("Comp", 10),
        UNCOMP("Uncomp", 10),
        NULLS("Nulls", 10);

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
    private static final int STAT_COLUMN_COUNT = STAT_LABELS.length;
    private static final int TABLE_BORDER_WIDTH = 2;
    private static final int HIGHLIGHT_SYMBOL_WIDTH = 2;

    private PagesScreen() {
    }

    public static boolean handle(KeyEvent event, ParquetModel model, NavigationStack stack) {
        ScreenState.Pages state = (ScreenState.Pages) stack.top();
        boolean logical = state.logicalTypes();
        List<PageHeader> headers = model.pageHeaders(state.rowGroupIndex(), state.columnIndex());
        // `t` toggles logical-type rendering of inline-stats Min / Max
        // values, which only exist on data pages. When the cursor is on
        // a dictionary page (or no pages at all) the toggle has no
        // visible effect, so ignore it — keeps the keybar's own gate
        // honest. Handled before the modal-open short-circuit so the
        // toggle isn't swallowed when the page-header modal is open
        // on a data page.
        boolean onDataPage = !headers.isEmpty()
                && state.selection() < headers.size()
                && headers.get(state.selection()).type() != PageType.DICTIONARY_PAGE;
        if (event.code() == dev.tamboui.tui.event.KeyCode.CHAR && event.character() == 't'
                && !event.hasCtrl() && !event.hasAlt() && onDataPage) {
            stack.replaceTop(new ScreenState.Pages(
                    state.rowGroupIndex(), state.columnIndex(),
                    state.selection(), state.modalOpen(), !logical, state.scrollTop()));
            return true;
        }
        if (state.modalOpen()) {
            if (event.isCancel() || event.isConfirm()) {
                stack.replaceTop(new ScreenState.Pages(
                        state.rowGroupIndex(), state.columnIndex(), state.selection(), false, logical,
                        state.scrollTop()));
                return true;
            }
            int next = ScrollPane.scroll(event, state.modalScroll(),
                    modalLineCount(model, state), Keys.viewportStride());
            if (next == ScrollPane.UNHANDLED) {
                return false;
            }
            if (next != state.modalScroll()) {
                stack.replaceTop(new ScreenState.Pages(
                        state.rowGroupIndex(), state.columnIndex(), state.selection(), true, logical,
                        state.scrollTop(), next));
            }
            return true;
        }
        int selected = CursorPane.select(event, state.selection(), headers.size());
        if (selected != CursorPane.UNHANDLED) {
            stack.replaceTop(moved(state, selected, logical));
            return true;
        }
        if (event.isConfirm() && !headers.isEmpty()) {
            stack.replaceTop(new ScreenState.Pages(
                    state.rowGroupIndex(), state.columnIndex(), state.selection(), true, logical,
                    state.scrollTop()));
            return true;
        }
        return false;
    }

    private static ScreenState.Pages moved(ScreenState.Pages state, int newSelection, boolean logical) {
        int newTop = RowWindow.adjustTop(state.scrollTop(), newSelection, Keys.viewportStride());
        return new ScreenState.Pages(state.rowGroupIndex(), state.columnIndex(), newSelection,
                false, logical, newTop);
    }

    public static void render(Buffer buffer, Rect area, ParquetModel model, ScreenState.Pages state) {
        Keys.observeViewport(area.height() - 3);
        List<PageHeader> headers = model.pageHeaders(state.rowGroupIndex(), state.columnIndex());
        ColumnIndex columnIndex = model.columnIndex(state.rowGroupIndex(), state.columnIndex());
        OffsetIndex offsetIndex = model.offsetIndex(state.rowGroupIndex(), state.columnIndex());
        ColumnSchema col = model.schema().getColumn(state.columnIndex());

        // Hide Min / Max columns entirely when no page-level stats are available
        // anywhere (no ColumnIndex AND no inline statistics on any page). Every
        // row would be "—" otherwise, pure visual noise.
        boolean hasAnyStats = columnIndex != null || headers.stream()
                .anyMatch(h -> h.type() != PageType.DICTIONARY_PAGE && inlineStats(h) != null);
        // Build Row objects only for the visible window — see RowWindow.
        RowWindow window = RowWindow.from(state.scrollTop(), state.selection(),
                headers.size(), area.height() - 3);
        // Per-data-page stats are addressed by `dataPageIdx`, which advances
        // only on non-dictionary pages. Recover its value at window.start()
        // by counting non-dict pages in the skipped prefix.
        int dataPageIdx = 0;
        for (int i = 0; i < window.start(); i++) {
            if (headers.get(i).type() != PageType.DICTIONARY_PAGE) {
                dataPageIdx++;
            }
        }
        List<Row> rows = new ArrayList<>(window.size());
        int statBudget = statBudget(area.width());
        for (int i = window.start(); i < window.end(); i++) {
            PageHeader h = headers.get(i);
            String firstRow = Strings.ABSENT_VALUE;
            String min = Strings.ABSENT_VALUE;
            String max = Strings.ABSENT_VALUE;
            String nulls = Strings.ABSENT_VALUE;
            int values;
            String uncompressed = Sizes.format(h.uncompressedPageSize());
            if (h.type() == PageType.DICTIONARY_PAGE) {
                DictionaryPageHeader dph = h.dictionaryPageHeader();
                values = dph != null ? dph.numValues() : 0;
            }
            else {
                values = dataValues(h);
                if (offsetIndex != null && dataPageIdx < offsetIndex.pageLocations().size()) {
                    PageLocation loc = offsetIndex.pageLocations().get(dataPageIdx);
                    firstRow = Fmt.fmt("%,d", loc.firstRowIndex());
                }
                if (columnIndex != null && dataPageIdx < columnIndex.getPageCount()) {
                    min = formatStat(columnIndex.minValues().get(dataPageIdx), col, state.logicalTypes(), statBudget);
                    max = formatStat(columnIndex.maxValues().get(dataPageIdx), col, state.logicalTypes(), statBudget);
                    if (columnIndex.nullCounts() != null
                            && dataPageIdx < columnIndex.nullCounts().length) {
                        nulls = Fmt.fmt("%,d", columnIndex.nullCounts()[dataPageIdx]);
                    }
                }
                else {
                    Statistics inline = inlineStats(h);
                    if (inline != null) {
                        min = formatStat(inline.minValue(), col, state.logicalTypes(), statBudget);
                        max = formatStat(inline.maxValue(), col, state.logicalTypes(), statBudget);
                        if (inline.nullCount() != null) {
                            nulls = Fmt.fmt("%,d", inline.nullCount());
                        }
                    }
                }
                dataPageIdx++;
            }
            if (hasAnyStats) {
                rows.add(Row.from(
                        String.valueOf(i),
                        h.type().name(),
                        firstRow,
                        Fmt.fmt("%,d", values),
                        dataEncoding(h),
                        Sizes.format(h.compressedPageSize()),
                        uncompressed,
                        nulls,
                        min,
                        max));
            }
            else {
                rows.add(Row.from(
                        String.valueOf(i),
                        h.type().name(),
                        firstRow,
                        Fmt.fmt("%,d", values),
                        dataEncoding(h),
                        Sizes.format(h.compressedPageSize()),
                        uncompressed,
                        nulls));
            }
        }
        Row header = header(hasAnyStats);
        String titleSuffix = hasAnyStats ? "" : " (no column index)";
        String typeMode = state.logicalTypes() ? "" : " · physical";
        Block block = Block.builder()
                .title(" Pages "
                        + Plurals.rangeOf(window, headers.size())
                        + titleSuffix + typeMode + " ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .build();
        Table table = Table.builder()
                .header(header)
                .rows(rows)
                .widths(widths(hasAnyStats))
                .columnSpacing(1)
                .block(block)
                .highlightSymbol("▶ ")
                .highlightStyle(Theme.selection())
                .build();
        TableState tableState = new TableState();
        if (!headers.isEmpty()) {
            tableState.select(window.selectionInWindow());
        }
        table.render(area, buffer, tableState);

        if (state.modalOpen() && !headers.isEmpty()) {
            buffer.setStyle(area, Theme.dim());
            renderHeaderModal(buffer, area, headers.get(state.selection()), state.selection(), col,
                    state.logicalTypes(), state.modalScroll());
        }
    }

    public static String keybarKeys(ScreenState.Pages state, ParquetModel model) {
        if (state.modalOpen()) {
            return "";
        }
        List<PageHeader> headers = model.pageHeaders(state.rowGroupIndex(), state.columnIndex());
        int count = headers.size();
        ColumnSchema col = model.schema().getColumn(state.columnIndex());
        // `t` toggles logical-type rendering of inline-stats Min / Max,
        // but those values only exist on data pages — not on dictionary
        // pages. Hide the affordance when the cursor is on a
        // DICTIONARY_PAGE row.
        boolean onDataPage = count > 0
                && state.selection() < count
                && headers.get(state.selection()).type() != PageType.DICTIONARY_PAGE;
        boolean hasLogical = col.logicalType() != null && onDataPage;
        return new Keys.Hints()
                .add(true, CursorPane.hints(count))
                .add(count > 0, "[Enter] view page header")
                .add(hasLogical, "[t] logical types")
                .add(true, "[Esc] back")
                .build();
    }

    private static int dataValues(PageHeader h) {
        if (h.dataPageHeader() != null) {
            return h.dataPageHeader().numValues();
        }
        if (h.dataPageHeaderV2() != null) {
            return h.dataPageHeaderV2().numValues();
        }
        return 0;
    }

    private static String dataEncoding(PageHeader h) {
        if (h.dataPageHeader() != null) {
            return h.dataPageHeader().encoding().name();
        }
        if (h.dataPageHeaderV2() != null) {
            return h.dataPageHeaderV2().encoding().name();
        }
        if (h.dictionaryPageHeader() != null) {
            return h.dictionaryPageHeader().encoding().name();
        }
        return Strings.ABSENT_VALUE;
    }

    private static Row header(boolean hasAnyStats) {
        int count = hasAnyStats ? COLUMNS.length + STAT_COLUMN_COUNT : COLUMNS.length;
        String[] labels = new String[count];
        // The header is two label sources laid end to end: the fixed columns
        // first, then Min / Max. `count` decides whether the second source is
        // there at all, so the index alone tells which one to read from.
        Arrays.setAll(labels, i -> i < COLUMNS.length
                ? COLUMNS[i].label
                : STAT_LABELS[i - COLUMNS.length]);
        return Row.from(labels).style(Theme.accent().bold());
    }

    /// Min and Max share the width left after the fixed columns. The renderer
    /// computes the matching display budget in [#statBudget].
    private static Constraint[] widths(boolean hasAnyStats) {
        int count = hasAnyStats ? COLUMNS.length + STAT_COLUMN_COUNT : COLUMNS.length;
        Constraint[] widths = new Constraint[count];
        // Same two sources as [#header], in the same order.
        Arrays.setAll(widths, i -> i < COLUMNS.length
                ? new Constraint.Length(COLUMNS[i].width)
                : new Constraint.Fill(1));
        return widths;
    }

    /// Used by the table cells, where two Min/Max columns share whatever's
    /// left after the fixed-width columns. Compute that width before rendering
    /// and mark the cut with `…`. The table gives the columns everything except
    /// its border, the selection marker, and one gap between each pair.
    private static int statBudget(int viewportWidth) {
        int fixedColumns = Arrays.stream(COLUMNS).mapToInt(column -> column.width).sum();
        int gaps = COLUMNS.length + STAT_COLUMN_COUNT - 1;
        return Math.max(1, (viewportWidth - fixedColumns - gaps
                - TABLE_BORDER_WIDTH - HIGHLIGHT_SYMBOL_WIDTH) / STAT_COLUMN_COUNT);
    }

    private static String formatStat(byte[] bytes, ColumnSchema col, boolean logical, int budget) {
        return bytes == null ?
                Strings.ABSENT_VALUE :
                Strings.truncateRight(
                        ValueFormatter.formatBytes(bytes, col, logical, budget),
                        budget);
    }

    private static String formatStatFull(byte[] bytes, ColumnSchema col, boolean logical) {
        // Modal has space — no budget constraint, so the whole value is rendered.
        return bytes == null ?
                Strings.ABSENT_VALUE :
                ValueFormatter.formatBytes(bytes, col, logical);
    }

    private static void renderHeaderModal(Buffer buffer, Rect screenArea, PageHeader header,
                                          int index, ColumnSchema col, boolean logical, int scroll) {
        // Grow the modal to fill the available area so long inline-stats
        // values aren't clipped at a fixed 60-cell width.
        Rect area = ScrollPane.modalArea(screenArea, Math.max(40, screenArea.width()), screenArea.height());
        // Dictionary pages have no inline stats — `t` is a no-op even
        // when the column carries a logical type, so suppress the hint.
        boolean onDataPage = header.type() != PageType.DICTIONARY_PAGE;
        boolean hasLogical = col.logicalType() != null && onDataPage;
        ScrollPane.renderModal(buffer, area, "Page #" + index + " header",
                headerModalLines(header, col, logical), scroll,
                "Esc / Enter close" + (hasLogical ? " · t logical types" : ""));
    }

    /// Line count of the open header modal, so the key handler and the
    /// renderer agree on how far it can scroll.
    private static int modalLineCount(ParquetModel model, ScreenState.Pages state) {
        List<PageHeader> headers = model.pageHeaders(state.rowGroupIndex(), state.columnIndex());
        if (headers.isEmpty()) {
            return 0;
        }
        ColumnSchema col = model.schema().getColumn(state.columnIndex());
        int index = Math.min(state.selection(), headers.size() - 1);
        return headerModalLines(headers.get(index), col, state.logicalTypes()).size();
    }

    /// The header modal's content. Shared with the key handler, which needs
    /// the line count to know how far the modal can scroll.
    private static List<Line> headerModalLines(PageHeader header, ColumnSchema col, boolean logical) {
        List<Line> lines = new ArrayList<>();
        lines.add(kv("Type", header.type().name()));
        lines.add(kv("Compressed size", Sizes.dualFormat(header.compressedPageSize())));
        lines.add(kv("Uncompressed size", Sizes.dualFormat(header.uncompressedPageSize())));
        lines.add(kv("CRC", header.crc() != null ? "0x" + Integer.toHexString(header.crc()) : Strings.ABSENT_VALUE));
        lines.add(Line.empty());
        DataPageHeader dph = header.dataPageHeader();
        DataPageHeaderV2 dphv2 = header.dataPageHeaderV2();
        DictionaryPageHeader dictHeader = header.dictionaryPageHeader();
        if (dph != null) {
            lines.add(kv("Num values", Fmt.fmt("%,d", dph.numValues())));
            lines.add(kv("Encoding", dph.encoding().name()));
            lines.add(kv("Def-level encoding", dph.definitionLevelEncoding().name()));
            lines.add(kv("Rep-level encoding", dph.repetitionLevelEncoding().name()));
        }
        if (dphv2 != null) {
            lines.add(kv("Num values", Fmt.fmt("%,d", dphv2.numValues())));
            lines.add(kv("Num nulls", Fmt.fmt("%,d", dphv2.numNulls())));
            lines.add(kv("Num rows", Fmt.fmt("%,d", dphv2.numRows())));
            lines.add(kv("Encoding", dphv2.encoding().name()));
            lines.add(kv("Def-level bytes", String.valueOf(dphv2.definitionLevelsByteLength())));
            lines.add(kv("Rep-level bytes", String.valueOf(dphv2.repetitionLevelsByteLength())));
            lines.add(kv("Is compressed", String.valueOf(dphv2.isCompressed())));
        }
        if (dictHeader != null) {
            lines.add(kv("Num values", Fmt.fmt("%,d", dictHeader.numValues())));
            lines.add(kv("Encoding", dictHeader.encoding().name()));
        }
        Statistics inline = inlineStats(header);
        if (inline != null) {
            lines.add(Line.empty());
            lines.add(Line.from(new Span(" Inline statistics ", Theme.accent().bold())));
            lines.add(kv("  Min", formatStatFull(inline.minValue(), col, logical)));
            lines.add(kv("  Max", formatStatFull(inline.maxValue(), col, logical)));
            if (inline.nullCount() != null) {
                lines.add(kv("  Nulls", Fmt.fmt("%,d", inline.nullCount())));
            }
        }
        return lines;
    }

    /// Returns the per-page inline statistics (if any), preferring v2 over v1
    /// since both can technically be present on exotic files.
    private static Statistics inlineStats(PageHeader h) {
        if (h.dataPageHeaderV2() != null && h.dataPageHeaderV2().statistics() != null) {
            return h.dataPageHeaderV2().statistics();
        }
        if (h.dataPageHeader() != null && h.dataPageHeader().statistics() != null) {
            return h.dataPageHeader().statistics();
        }
        return null;
    }

    private static Line kv(String key, String value) {
        return Line.from(
                Span.raw(" "),
                new Span(padRight(key, 20), Theme.primary()),
                new Span(value, Style.EMPTY));
    }

    private static String padRight(String s, int width) {
        return Strings.padRight(s, width);
    }
}
