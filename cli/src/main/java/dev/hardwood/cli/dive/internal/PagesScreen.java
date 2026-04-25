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
import dev.hardwood.internal.metadata.DataPageHeader;
import dev.hardwood.internal.metadata.DataPageHeaderV2;
import dev.hardwood.internal.metadata.DictionaryPageHeader;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.metadata.ColumnIndex;
import dev.hardwood.metadata.OffsetIndex;
import dev.hardwood.metadata.PageLocation;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.schema.ColumnSchema;
import dev.tamboui.buffer.Buffer;
import dev.tamboui.layout.Constraint;
import dev.tamboui.layout.Rect;
import dev.tamboui.style.Style;
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

/// Lists data + dictionary pages for one column chunk. Enter opens a modal with
/// the full thrift page header; Esc in the modal closes it, Esc on the list
/// pops back to Column chunk detail.
public final class PagesScreen {

    private PagesScreen() {
    }

    public static boolean handle(KeyEvent event, ParquetModel model, NavigationStack stack) {
        ScreenState.Pages state = (ScreenState.Pages) stack.top();
        boolean logical = state.logicalTypes();
        // `t` toggles the logical-type rendering at any time — including
        // while the page-header modal is open, since the modal renders
        // inline statistics that need to refresh too. Handled before the
        // modal-open short-circuit so the toggle isn't swallowed.
        if (event.code() == dev.tamboui.tui.event.KeyCode.CHAR && event.character() == 't'
                && !event.hasCtrl() && !event.hasAlt()) {
            stack.replaceTop(new ScreenState.Pages(
                    state.rowGroupIndex(), state.columnIndex(),
                    state.selection(), state.modalOpen(), !logical));
            return true;
        }
        if (state.modalOpen()) {
            if (event.isCancel() || event.isConfirm()) {
                stack.replaceTop(new ScreenState.Pages(
                        state.rowGroupIndex(), state.columnIndex(), state.selection(), false, logical));
                return true;
            }
            return false;
        }
        List<PageHeader> headers = model.pageHeaders(state.rowGroupIndex(), state.columnIndex());
        if (Keys.isStepUp(event)) {
            stack.replaceTop(new ScreenState.Pages(
                    state.rowGroupIndex(), state.columnIndex(),
                    Math.max(0, state.selection() - 1), false, logical));
            return true;
        }
        if (Keys.isStepDown(event)) {
            stack.replaceTop(new ScreenState.Pages(
                    state.rowGroupIndex(), state.columnIndex(),
                    Math.min(headers.size() - 1, state.selection() + 1), false, logical));
            return true;
        }
        if (Keys.isPageDown(event) && !headers.isEmpty()) {
            stack.replaceTop(new ScreenState.Pages(
                    state.rowGroupIndex(), state.columnIndex(),
                    Math.min(headers.size() - 1, state.selection() + Keys.viewportStride()),
                    false, logical));
            return true;
        }
        if (Keys.isPageUp(event) && !headers.isEmpty()) {
            stack.replaceTop(new ScreenState.Pages(
                    state.rowGroupIndex(), state.columnIndex(),
                    Math.max(0, state.selection() - Keys.viewportStride()),
                    false, logical));
            return true;
        }
        if (Keys.isJumpTop(event) && !headers.isEmpty()) {
            stack.replaceTop(new ScreenState.Pages(
                    state.rowGroupIndex(), state.columnIndex(), 0, false, logical));
            return true;
        }
        if (Keys.isJumpBottom(event) && !headers.isEmpty()) {
            stack.replaceTop(new ScreenState.Pages(
                    state.rowGroupIndex(), state.columnIndex(), headers.size() - 1, false, logical));
            return true;
        }
        if (event.isConfirm() && !headers.isEmpty()) {
            stack.replaceTop(new ScreenState.Pages(
                    state.rowGroupIndex(), state.columnIndex(), state.selection(), true, logical));
            return true;
        }
        return false;
    }

    public static void render(Buffer buffer, Rect area, ParquetModel model, ScreenState.Pages state) {
        Keys.observeViewport(area.height() - 3);
        List<PageHeader> headers = model.pageHeaders(state.rowGroupIndex(), state.columnIndex());
        ColumnIndex columnIndex = model.columnIndex(state.rowGroupIndex(), state.columnIndex());
        OffsetIndex offsetIndex = model.offsetIndex(state.rowGroupIndex(), state.columnIndex());
        ColumnSchema col = model.schema().getColumn(state.columnIndex());

        List<Row> rows = new ArrayList<>();
        // Hide Min / Max columns entirely when no page-level stats are available
        // anywhere (no ColumnIndex AND no inline statistics on any page). Every
        // row would be "—" otherwise, pure visual noise.
        boolean hasAnyStats = columnIndex != null || headers.stream()
                .anyMatch(h -> h.type() != PageHeader.PageType.DICTIONARY_PAGE && inlineStats(h) != null);
        int dataPageIdx = 0;
        for (int i = 0; i < headers.size(); i++) {
            PageHeader h = headers.get(i);
            String firstRow = "—";
            String min = "—";
            String max = "—";
            String nulls = "—";
            int values;
            String uncompressed = Sizes.format(h.uncompressedPageSize());
            if (h.type() == PageHeader.PageType.DICTIONARY_PAGE) {
                DictionaryPageHeader dph = h.dictionaryPageHeader();
                values = dph != null ? dph.numValues() : 0;
            }
            else {
                values = dataValues(h);
                if (offsetIndex != null && dataPageIdx < offsetIndex.pageLocations().size()) {
                    PageLocation loc = offsetIndex.pageLocations().get(dataPageIdx);
                    firstRow = String.format("%,d", loc.firstRowIndex());
                }
                if (columnIndex != null && dataPageIdx < columnIndex.getPageCount()) {
                    min = formatStat(columnIndex.minValues().get(dataPageIdx), col, state.logicalTypes());
                    max = formatStat(columnIndex.maxValues().get(dataPageIdx), col, state.logicalTypes());
                    if (columnIndex.nullCounts() != null
                            && dataPageIdx < columnIndex.nullCounts().size()) {
                        nulls = String.format("%,d", columnIndex.nullCounts().get(dataPageIdx));
                    }
                }
                else {
                    Statistics inline = inlineStats(h);
                    if (inline != null) {
                        min = formatStat(inline.minValue(), col, state.logicalTypes());
                        max = formatStat(inline.maxValue(), col, state.logicalTypes());
                        if (inline.nullCount() != null) {
                            nulls = String.format("%,d", inline.nullCount());
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
                        String.format("%,d", values),
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
                        String.format("%,d", values),
                        dataEncoding(h),
                        Sizes.format(h.compressedPageSize()),
                        uncompressed,
                        nulls));
            }
        }
        Row header = hasAnyStats
                ? Row.from("#", "Type", "First row", "Values", "Encoding", "Comp", "Uncomp", "Nulls", "Min", "Max")
                        .style(Style.EMPTY.bold())
                : Row.from("#", "Type", "First row", "Values", "Encoding", "Comp", "Uncomp", "Nulls")
                        .style(Style.EMPTY.bold());
        String titleSuffix = hasAnyStats ? "" : " (no column index)";
        String typeMode = state.logicalTypes() ? "" : " · physical";
        Block block = Block.builder()
                .title(" Pages (" + Plurals.format(headers.size(), "page", "pages") + ")"
                        + titleSuffix + typeMode + " ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .borderColor(Theme.ACCENT)
                .build();
        List<Constraint> widths = new ArrayList<>();
        widths.add(new Constraint.Length(4));   // #
        widths.add(new Constraint.Length(16));  // Type
        widths.add(new Constraint.Length(12));  // First row
        widths.add(new Constraint.Length(10));  // Values
        widths.add(new Constraint.Length(23));  // Encoding (fits DELTA_LENGTH_BYTE_ARRAY = 23)
        widths.add(new Constraint.Length(10));  // Comp
        widths.add(new Constraint.Length(10));  // Uncomp
        widths.add(new Constraint.Length(10));  // Nulls
        if (hasAnyStats) {
            widths.add(new Constraint.Fill(1)); // Min
            widths.add(new Constraint.Fill(1)); // Max
        }
        Table table = Table.builder()
                .header(header)
                .rows(rows)
                .widths(widths)
                .columnSpacing(1)
                .block(block)
                .highlightSymbol("▶ ")
                .highlightStyle(Style.EMPTY.bold())
                .build();
        TableState tableState = new TableState();
        if (!headers.isEmpty()) {
            tableState.select(state.selection());
        }
        table.render(area, buffer, tableState);

        if (state.modalOpen() && !headers.isEmpty()) {
            renderHeaderModal(buffer, area, headers.get(state.selection()), state.selection(), col,
                    state.logicalTypes());
        }
    }

    public static String keybarKeys() {
        return "[↑↓] move  [PgDn/PgUp or Shift+↓↑] page  [Enter] page header  [t] logical types  [Esc] back";
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
        return "—";
    }

    /// Used by the table cells, where two Min/Max columns share whatever's
    /// left after the fixed-width columns. tamboui's Table clips silently,
    /// so cap the formatted string ourselves and append `…` to make
    /// truncation visible. The page-header modal calls the un-capped
    /// `IndexValueFormatter.format` directly.
    private static final int TABLE_STAT_MAX = 16;

    private static String formatStat(byte[] bytes, ColumnSchema col, boolean logical) {
        if (bytes == null) {
            return "—";
        }
        String full = IndexValueFormatter.format(bytes, col, logical);
        if (full.length() <= TABLE_STAT_MAX) {
            return full;
        }
        return full.substring(0, TABLE_STAT_MAX - 1) + "…";
    }

    private static String formatStatFull(byte[] bytes, ColumnSchema col, boolean logical) {
        if (bytes == null) {
            return "—";
        }
        // Modal has space — bypass the per-string 20-char cap.
        return IndexValueFormatter.format(bytes, col, logical, false);
    }

    private static void renderHeaderModal(Buffer buffer, Rect screenArea, PageHeader header,
                                          int index, ColumnSchema col, boolean logical) {
        // Grow the modal to fill the available area so long inline-stats
        // values aren't clipped at a fixed 60-cell width (the previous cap
        // hid the bulk of any UTF-8 string min/max past ~50 chars).
        int width = Math.max(40, screenArea.width() - 4);
        int height = Math.max(8, screenArea.height() - 2);
        int x = screenArea.left() + (screenArea.width() - width) / 2;
        int y = screenArea.top() + (screenArea.height() - height) / 2;
        Rect area = new Rect(x, y, width, height);
        // Wipe the area so the underlying table doesn't bleed through cells
        // that the Paragraph doesn't paint (Paragraph only writes where text is).
        dev.tamboui.widgets.Clear.INSTANCE.render(area, buffer);

        List<Line> lines = new ArrayList<>();
        lines.add(kv("Type", header.type().name()));
        lines.add(kv("Compressed size", Sizes.dualFormat(header.compressedPageSize())));
        lines.add(kv("Uncompressed size", Sizes.dualFormat(header.uncompressedPageSize())));
        lines.add(kv("CRC", header.crc() != null ? "0x" + Integer.toHexString(header.crc()) : "—"));
        lines.add(Line.empty());
        DataPageHeader dph = header.dataPageHeader();
        DataPageHeaderV2 dphv2 = header.dataPageHeaderV2();
        DictionaryPageHeader dictHeader = header.dictionaryPageHeader();
        if (dph != null) {
            lines.add(kv("Num values", String.format("%,d", dph.numValues())));
            lines.add(kv("Encoding", dph.encoding().name()));
            lines.add(kv("Def-level encoding", dph.definitionLevelEncoding().name()));
            lines.add(kv("Rep-level encoding", dph.repetitionLevelEncoding().name()));
        }
        if (dphv2 != null) {
            lines.add(kv("Num values", String.format("%,d", dphv2.numValues())));
            lines.add(kv("Num nulls", String.format("%,d", dphv2.numNulls())));
            lines.add(kv("Num rows", String.format("%,d", dphv2.numRows())));
            lines.add(kv("Encoding", dphv2.encoding().name()));
            lines.add(kv("Def-level bytes", String.valueOf(dphv2.definitionLevelsByteLength())));
            lines.add(kv("Rep-level bytes", String.valueOf(dphv2.repetitionLevelsByteLength())));
            lines.add(kv("Is compressed", String.valueOf(dphv2.isCompressed())));
        }
        if (dictHeader != null) {
            lines.add(kv("Num values", String.format("%,d", dictHeader.numValues())));
            lines.add(kv("Encoding", dictHeader.encoding().name()));
        }
        Statistics inline = inlineStats(header);
        if (inline != null) {
            lines.add(Line.empty());
            lines.add(Line.from(new Span(" Inline statistics ", Style.EMPTY.bold())));
            lines.add(kv("  Min", formatStatFull(inline.minValue(), col, logical)));
            lines.add(kv("  Max", formatStatFull(inline.maxValue(), col, logical)));
            if (inline.nullCount() != null) {
                lines.add(kv("  Nulls", String.format("%,d", inline.nullCount())));
            }
        }
        lines.add(Line.empty());
        lines.add(Line.from(new Span(" Press Esc or Enter to close", Style.EMPTY.fg(Theme.DIM))));

        Block block = Block.builder()
                .title(" Page #" + index + " header ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .borderColor(Theme.ACCENT)
                .build();
        Paragraph.builder().block(block).text(Text.from(lines)).left().build().render(area, buffer);
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
                new Span(padRight(key, 20), Style.EMPTY),
                new Span(value, Style.EMPTY.bold()));
    }

    private static String padRight(String s, int width) {
        if (s.length() >= width) {
            return s;
        }
        return s + " ".repeat(width - s.length());
    }
}
