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
import dev.hardwood.schema.ColumnSchema;
import dev.tamboui.buffer.Buffer;
import dev.tamboui.layout.Constraint;
import dev.tamboui.layout.Rect;
import dev.tamboui.style.Color;
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
        if (state.modalOpen()) {
            if (event.isCancel() || event.isConfirm()) {
                stack.replaceTop(new ScreenState.Pages(
                        state.rowGroupIndex(), state.columnIndex(), state.selection(), false));
                return true;
            }
            return false;
        }
        List<PageHeader> headers = model.pageHeaders(state.rowGroupIndex(), state.columnIndex());
        if (event.isUp()) {
            stack.replaceTop(new ScreenState.Pages(
                    state.rowGroupIndex(), state.columnIndex(),
                    Math.max(0, state.selection() - 1), false));
            return true;
        }
        if (event.isDown()) {
            stack.replaceTop(new ScreenState.Pages(
                    state.rowGroupIndex(), state.columnIndex(),
                    Math.min(headers.size() - 1, state.selection() + 1), false));
            return true;
        }
        if (event.isConfirm() && !headers.isEmpty()) {
            stack.replaceTop(new ScreenState.Pages(
                    state.rowGroupIndex(), state.columnIndex(), state.selection(), true));
            return true;
        }
        return false;
    }

    public static void render(Buffer buffer, Rect area, ParquetModel model, ScreenState.Pages state) {
        List<PageHeader> headers = model.pageHeaders(state.rowGroupIndex(), state.columnIndex());
        ColumnIndex columnIndex = model.columnIndex(state.rowGroupIndex(), state.columnIndex());
        OffsetIndex offsetIndex = model.offsetIndex(state.rowGroupIndex(), state.columnIndex());
        ColumnSchema col = model.schema().getColumn(state.columnIndex());

        List<Row> rows = new ArrayList<>();
        int dataPageIdx = 0;
        for (int i = 0; i < headers.size(); i++) {
            PageHeader h = headers.get(i);
            String firstRow = "—";
            String min = "—";
            String max = "—";
            int values;
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
                    min = formatStat(columnIndex.minValues().get(dataPageIdx), col);
                    max = formatStat(columnIndex.maxValues().get(dataPageIdx), col);
                }
                dataPageIdx++;
            }
            rows.add(Row.from(
                    String.valueOf(i),
                    h.type().name(),
                    firstRow,
                    String.format("%,d", values),
                    dataEncoding(h),
                    Sizes.format(h.compressedPageSize()),
                    min,
                    max));
        }
        Row header = Row.from("#", "Type", "First row", "Values", "Encoding", "Comp", "Min", "Max")
                .style(Style.EMPTY.bold());
        Block block = Block.builder()
                .title(" Pages (" + headers.size() + ") ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .borderColor(Color.CYAN)
                .build();
        Table table = Table.builder()
                .header(header)
                .rows(rows)
                .widths(new Constraint.Length(4),
                        new Constraint.Length(16),
                        new Constraint.Length(12),
                        new Constraint.Length(10),
                        new Constraint.Length(12),
                        new Constraint.Length(10),
                        new Constraint.Fill(1),
                        new Constraint.Fill(1))
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
            renderHeaderModal(buffer, area, headers.get(state.selection()), state.selection());
        }
    }

    public static String keybarKeys() {
        return "[↑↓] move  [Enter] page header  [Esc] back";
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

    private static String formatStat(byte[] bytes, ColumnSchema col) {
        if (bytes == null) {
            return "—";
        }
        return IndexValueFormatter.format(bytes, col);
    }

    private static void renderHeaderModal(Buffer buffer, Rect screenArea, PageHeader header, int index) {
        int width = Math.min(60, screenArea.width() - 4);
        int height = Math.min(20, screenArea.height() - 2);
        int x = screenArea.left() + (screenArea.width() - width) / 2;
        int y = screenArea.top() + (screenArea.height() - height) / 2;
        Rect area = new Rect(x, y, width, height);

        List<Line> lines = new ArrayList<>();
        lines.add(kv("Type", header.type().name()));
        lines.add(kv("Compressed size", String.format("%,d", header.compressedPageSize())));
        lines.add(kv("Uncompressed size", String.format("%,d", header.uncompressedPageSize())));
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
        lines.add(Line.empty());
        lines.add(Line.from(new Span(" Press Esc or Enter to close", Style.EMPTY.fg(Color.GRAY))));

        Block block = Block.builder()
                .title(" Page #" + index + " header ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .borderColor(Color.CYAN)
                .build();
        Paragraph.builder().block(block).text(Text.from(lines)).left().build().render(area, buffer);
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
