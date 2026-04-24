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
import java.util.stream.Collectors;

import dev.hardwood.cli.dive.NavigationStack;
import dev.hardwood.cli.dive.ParquetModel;
import dev.hardwood.cli.dive.ScreenState;
import dev.hardwood.cli.internal.IndexValueFormatter;
import dev.hardwood.cli.internal.Sizes;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.schema.ColumnSchema;
import dev.tamboui.buffer.Buffer;
import dev.tamboui.layout.Constraint;
import dev.tamboui.layout.Layout;
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

/// Two-pane detail for one `(rowGroup, column)` chunk: facts on the left, drill
/// menu on the right leading into Pages, Column index, Offset index, and
/// Dictionary (dictionary deferred to phase 3).
public final class ColumnChunkDetailScreen {

    public enum MenuItem {
        PAGES("Pages"),
        COLUMN_INDEX("Column index"),
        OFFSET_INDEX("Offset index"),
        DICTIONARY("Dictionary");

        final String label;

        MenuItem(String label) {
            this.label = label;
        }
    }

    private ColumnChunkDetailScreen() {
    }

    public static boolean handle(KeyEvent event, ParquetModel model, NavigationStack stack) {
        ScreenState.ColumnChunkDetail state = (ScreenState.ColumnChunkDetail) stack.top();
        if (event.isFocusNext() || event.isFocusPrevious()) {
            ScreenState.ColumnChunkDetail.Pane next = state.focus() == ScreenState.ColumnChunkDetail.Pane.FACTS
                    ? ScreenState.ColumnChunkDetail.Pane.MENU
                    : ScreenState.ColumnChunkDetail.Pane.FACTS;
            stack.replaceTop(new ScreenState.ColumnChunkDetail(
                    state.rowGroupIndex(), state.columnIndex(), next, state.menuSelection()));
            return true;
        }
        if (state.focus() != ScreenState.ColumnChunkDetail.Pane.MENU) {
            return false;
        }
        MenuItem[] items = MenuItem.values();
        if (event.isUp()) {
            stack.replaceTop(state(state, Math.max(0, state.menuSelection() - 1)));
            return true;
        }
        if (event.isDown()) {
            stack.replaceTop(state(state, Math.min(items.length - 1, state.menuSelection() + 1)));
            return true;
        }
        if (event.isConfirm()) {
            MenuItem item = items[state.menuSelection()];
            if (!itemEnabled(item, model, state)) {
                return false;
            }
            switch (item) {
                case PAGES -> stack.push(new ScreenState.Pages(state.rowGroupIndex(), state.columnIndex(), 0, false));
                case COLUMN_INDEX -> stack.push(new ScreenState.ColumnIndexView(
                        state.rowGroupIndex(), state.columnIndex(), 0));
                case OFFSET_INDEX -> stack.push(new ScreenState.OffsetIndexView(
                        state.rowGroupIndex(), state.columnIndex(), 0));
                case DICTIONARY -> stack.push(new ScreenState.DictionaryView(
                        state.rowGroupIndex(), state.columnIndex(), 0, false, "", false));
            }
            return true;
        }
        return false;
    }

    public static void render(Buffer buffer, Rect area, ParquetModel model, ScreenState.ColumnChunkDetail state) {
        List<Rect> cols = Layout.horizontal()
                .constraints(new Constraint.Percentage(60), new Constraint.Percentage(40))
                .split(area);
        renderFactsPane(buffer, cols.get(0), model, state);
        renderMenuPane(buffer, cols.get(1), model, state);
    }

    public static String keybarKeys() {
        return "[Tab] pane  [↑↓] move  [Enter] drill  [Esc] back";
    }

    private static ScreenState.ColumnChunkDetail state(ScreenState.ColumnChunkDetail state, int selection) {
        return new ScreenState.ColumnChunkDetail(
                state.rowGroupIndex(), state.columnIndex(), state.focus(), selection);
    }

    private static boolean itemEnabled(MenuItem item, ParquetModel model, ScreenState.ColumnChunkDetail state) {
        ColumnChunk chunk = model.chunk(state.rowGroupIndex(), state.columnIndex());
        return switch (item) {
            case PAGES -> true;
            case COLUMN_INDEX -> chunk.columnIndexOffset() != null && chunk.columnIndexLength() != null;
            case OFFSET_INDEX -> chunk.offsetIndexOffset() != null && chunk.offsetIndexLength() != null;
            case DICTIONARY -> chunk.metaData().dictionaryPageOffset() != null;
        };
    }

    private static void renderFactsPane(Buffer buffer, Rect area, ParquetModel model, ScreenState.ColumnChunkDetail state) {
        ColumnChunk chunk = model.chunk(state.rowGroupIndex(), state.columnIndex());
        ColumnMetaData cmd = chunk.metaData();
        ColumnSchema col = model.schema().getColumn(state.columnIndex());
        Statistics stats = cmd.statistics();
        boolean focused = state.focus() == ScreenState.ColumnChunkDetail.Pane.FACTS;

        List<Line> lines = new ArrayList<>();
        lines.add(fact("Path", Sizes.columnPath(cmd)));
        lines.add(fact("Column idx", String.valueOf(col.columnIndex())));
        lines.add(fact("Physical", cmd.type().name()));
        lines.add(fact("Logical", col.logicalType() != null ? col.logicalType().toString() : "—"));
        lines.add(fact("Codec", cmd.codec().name()));
        lines.add(fact("Encodings", cmd.encodings().stream()
                .map(Enum::name)
                .collect(Collectors.joining(", "))));
        lines.add(Line.empty());
        lines.add(fact("Data offset", String.valueOf(cmd.dataPageOffset())));
        lines.add(fact("Dict offset", cmd.dictionaryPageOffset() != null
                ? cmd.dictionaryPageOffset().toString()
                : "—"));
        lines.add(fact("Column index offset", chunk.columnIndexOffset() != null
                ? chunk.columnIndexOffset().toString()
                : "—"));
        lines.add(fact("Offset index offset", chunk.offsetIndexOffset() != null
                ? chunk.offsetIndexOffset().toString()
                : "—"));
        lines.add(Line.empty());
        lines.add(fact("Values", String.format("%,d", cmd.numValues())));
        lines.add(fact("Nulls", stats != null && stats.nullCount() != null
                ? String.format("%,d", stats.nullCount())
                : "—"));
        lines.add(fact("Uncompressed", Sizes.format(cmd.totalUncompressedSize())));
        lines.add(fact("Compressed", Sizes.format(cmd.totalCompressedSize())));
        lines.add(fact("Min", stats != null ? formatStatValue(stats.minValue(), col) : "—"));
        lines.add(fact("Max", stats != null ? formatStatValue(stats.maxValue(), col) : "—"));

        Block block = paneBlock(" " + Sizes.columnPath(cmd) + " (RG #" + state.rowGroupIndex() + ") ", focused);
        Paragraph.builder().block(block).text(Text.from(lines)).left().build().render(area, buffer);
    }

    private static void renderMenuPane(Buffer buffer, Rect area, ParquetModel model, ScreenState.ColumnChunkDetail state) {
        boolean focused = state.focus() == ScreenState.ColumnChunkDetail.Pane.MENU;
        Block block = paneBlock(" Drill into ", focused);
        List<Line> lines = new ArrayList<>();
        MenuItem[] items = MenuItem.values();
        for (int i = 0; i < items.length; i++) {
            MenuItem item = items[i];
            boolean enabled = itemEnabled(item, model, state);
            boolean selected = focused && i == state.menuSelection();
            String cursor = selected ? "▶ " : "  ";
            String hint = menuHint(item, model, state);
            Style labelStyle = !enabled
                    ? Style.EMPTY.fg(Color.GRAY)
                    : selected ? Style.EMPTY.bold() : Style.EMPTY;
            Style hintStyle = Style.EMPTY.fg(Color.GRAY);
            lines.add(Line.from(
                    new Span(cursor, labelStyle),
                    new Span(padRight(item.label, 16), labelStyle),
                    new Span(hint, hintStyle)));
        }
        Paragraph.builder().block(block).text(Text.from(lines)).left().build().render(area, buffer);
    }

    private static String menuHint(MenuItem item, ParquetModel model, ScreenState.ColumnChunkDetail state) {
        ColumnChunk chunk = model.chunk(state.rowGroupIndex(), state.columnIndex());
        return switch (item) {
            case PAGES -> {
                var oi = model.offsetIndex(state.rowGroupIndex(), state.columnIndex());
                yield oi != null ? oi.pageLocations().size() + " pages" : "…";
            }
            case COLUMN_INDEX -> chunk.columnIndexOffset() != null ? "present" : "n/a";
            case OFFSET_INDEX -> chunk.offsetIndexOffset() != null ? "present" : "n/a";
            case DICTIONARY -> chunk.metaData().dictionaryPageOffset() != null ? "present" : "n/a";
        };
    }

    private static Block paneBlock(String title, boolean focused) {
        return Block.builder()
                .title(title)
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .borderColor(focused ? Color.CYAN : Color.GRAY)
                .build();
    }

    private static Line fact(String key, String value) {
        return Line.from(
                new Span(" " + padRight(key, 22), Style.EMPTY),
                new Span(value, Style.EMPTY.bold()));
    }

    private static String formatStatValue(byte[] bytes, ColumnSchema col) {
        if (bytes == null) {
            return "—";
        }
        return IndexValueFormatter.format(bytes, col);
    }

    private static String padRight(String s, int width) {
        if (s.length() >= width) {
            return s;
        }
        return s + " ".repeat(width - s.length());
    }
}
