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
import dev.hardwood.cli.internal.Fmt;
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
                    state.rowGroupIndex(), state.columnIndex(), next, state.menuSelection(),
                    state.logicalTypes()));
            return true;
        }
        // `t` toggles logical-type rendering on the facts pane, regardless of
        // which pane has focus. Wire before the MENU-only check.
        if (event.code() == dev.tamboui.tui.event.KeyCode.CHAR && event.character() == 't'
                && !event.hasCtrl() && !event.hasAlt()) {
            stack.replaceTop(new ScreenState.ColumnChunkDetail(
                    state.rowGroupIndex(), state.columnIndex(), state.focus(),
                    state.menuSelection(), !state.logicalTypes()));
            return true;
        }
        if (state.focus() != ScreenState.ColumnChunkDetail.Pane.MENU) {
            return false;
        }
        MenuItem[] items = MenuItem.values();
        // Snap an out-of-place selection to the first enabled item — covers
        // the initial entry where menuSelection is 0 (PAGES) and (in
        // principle) any later state where the chunk shape has changed.
        if (!itemEnabled(items[state.menuSelection()], model, state)) {
            int first = firstEnabledIndex(items, model, state);
            if (first >= 0 && first != state.menuSelection()) {
                state = state(state, first);
                stack.replaceTop(state);
            }
        }
        if (event.isUp()) {
            int prev = previousEnabledIndex(items, model, state, state.menuSelection());
            if (prev < 0) {
                return false;
            }
            stack.replaceTop(state(state, prev));
            return true;
        }
        if (event.isDown()) {
            int next = nextEnabledIndex(items, model, state, state.menuSelection());
            if (next < 0) {
                return false;
            }
            stack.replaceTop(state(state, next));
            return true;
        }
        if (event.isConfirm()) {
            MenuItem item = items[state.menuSelection()];
            if (!itemEnabled(item, model, state)) {
                return false;
            }
            switch (item) {
                case PAGES -> stack.push(new ScreenState.Pages(
                        state.rowGroupIndex(), state.columnIndex(), 0, false, true));
                case COLUMN_INDEX -> stack.push(new ScreenState.ColumnIndexView(
                        state.rowGroupIndex(), state.columnIndex(), 0, "", false, true, false));
                case OFFSET_INDEX -> stack.push(new ScreenState.OffsetIndexView(
                        state.rowGroupIndex(), state.columnIndex(), 0));
                case DICTIONARY -> stack.push(new ScreenState.DictionaryView(
                        state.rowGroupIndex(), state.columnIndex(), 0, false, "", false, false, true));
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

    public static String keybarKeys(ScreenState.ColumnChunkDetail state, ParquetModel model) {
        boolean onMenu = state.focus() == ScreenState.ColumnChunkDetail.Pane.MENU;
        MenuItem[] items = MenuItem.values();
        int enabledCount = 0;
        boolean currentEnabled = false;
        for (int i = 0; i < items.length; i++) {
            if (itemEnabled(items[i], model, state)) {
                enabledCount++;
                if (i == state.menuSelection()) {
                    currentEnabled = true;
                }
            }
        }
        ColumnSchema col = model.schema().getColumn(state.columnIndex());
        boolean hasLogical = col.logicalType() != null;
        return new Keys.Hints()
                .add(true, "[Tab] pane")
                .add(onMenu && enabledCount > 1, "[↑↓] move")
                .add(onMenu && currentEnabled, "[Enter] open")
                .add(hasLogical, "[t] logical types")
                .add(true, "[Esc] back")
                .build();
    }

    private static ScreenState.ColumnChunkDetail state(ScreenState.ColumnChunkDetail state, int selection) {
        return new ScreenState.ColumnChunkDetail(
                state.rowGroupIndex(), state.columnIndex(), state.focus(), selection,
                state.logicalTypes());
    }

    private static int firstEnabledIndex(MenuItem[] items, ParquetModel model,
                                          ScreenState.ColumnChunkDetail state) {
        for (int i = 0; i < items.length; i++) {
            if (itemEnabled(items[i], model, state)) {
                return i;
            }
        }
        return -1;
    }

    private static int nextEnabledIndex(MenuItem[] items, ParquetModel model,
                                         ScreenState.ColumnChunkDetail state, int from) {
        for (int i = from + 1; i < items.length; i++) {
            if (itemEnabled(items[i], model, state)) {
                return i;
            }
        }
        return -1;
    }

    private static int previousEnabledIndex(MenuItem[] items, ParquetModel model,
                                             ScreenState.ColumnChunkDetail state, int from) {
        for (int i = from - 1; i >= 0; i--) {
            if (itemEnabled(items[i], model, state)) {
                return i;
            }
        }
        return -1;
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
        lines.addAll(pathLines(Sizes.columnPath(cmd)));
        lines.add(fact("Column idx", String.valueOf(col.columnIndex())));
        lines.add(fact("Physical", cmd.type().name()));
        lines.add(fact("Logical", col.logicalType() != null ? col.logicalType().toString() : "—"));
        lines.add(fact("Codec", cmd.codec().name()));
        lines.add(fact("Encodings", cmd.encodings().stream()
                .map(Enum::name)
                .collect(Collectors.joining(", "))));
        lines.add(Line.empty());
        lines.add(fact("Data offset", Fmt.fmt("%,d", cmd.dataPageOffset())));
        lines.add(fact("Dict offset", cmd.dictionaryPageOffset() != null
                ? Fmt.fmt("%,d", cmd.dictionaryPageOffset())
                : "—"));
        lines.add(fact("Column index offset", chunk.columnIndexOffset() != null
                ? Fmt.fmt("%,d", chunk.columnIndexOffset())
                : "—"));
        lines.add(fact("Offset index offset", chunk.offsetIndexOffset() != null
                ? Fmt.fmt("%,d", chunk.offsetIndexOffset())
                : "—"));
        lines.add(Line.empty());
        lines.add(fact("Values", Fmt.fmt("%,d", cmd.numValues())));
        lines.add(fact("Nulls", stats != null && stats.nullCount() != null
                ? Fmt.fmt("%,d", stats.nullCount())
                : "—"));
        lines.add(fact("Uncompressed", Sizes.format(cmd.totalUncompressedSize())));
        lines.add(fact("Compressed", Sizes.format(cmd.totalCompressedSize())));
        lines.add(fact("Min", stats != null ? formatStatValue(stats.minValue(), col, state.logicalTypes()) : "—"));
        lines.add(fact("Max", stats != null ? formatStatValue(stats.maxValue(), col, state.logicalTypes()) : "—"));

        Block block = paneBlock(" " + truncateLeft(Sizes.columnPath(cmd), 40)
                + " (RG #" + state.rowGroupIndex() + ") ", focused);
        Paragraph.builder().block(block).text(Text.from(lines)).left().build().render(area, buffer);
    }

    private static void renderMenuPane(Buffer buffer, Rect area, ParquetModel model, ScreenState.ColumnChunkDetail state) {
        boolean focused = state.focus() == ScreenState.ColumnChunkDetail.Pane.MENU;
        Block block = paneBlock(" Drill into ", focused);
        List<Line> lines = new ArrayList<>();
        MenuItem[] items = MenuItem.values();
        // First-render snap: if state.menuSelection() points at a disabled
        // item, paint the cursor on the first enabled item so the user
        // sees a usable affordance immediately. handle() persists the
        // snap into state on the next event.
        int effectiveSelection = state.menuSelection();
        if (!itemEnabled(items[effectiveSelection], model, state)) {
            int first = firstEnabledIndex(items, model, state);
            if (first >= 0) {
                effectiveSelection = first;
            }
        }
        for (int i = 0; i < items.length; i++) {
            MenuItem item = items[i];
            boolean enabled = itemEnabled(item, model, state);
            boolean selected = focused && i == effectiveSelection && enabled;
            String cursor = selected ? "▶ " : "  ";
            String hint = menuHint(item, model, state);
            Style labelStyle = selected
                    ? Theme.selection()
                    : Theme.primary();
            Style hintStyle = Style.EMPTY;
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
                dev.hardwood.metadata.OffsetIndex oi =
                        model.offsetIndex(state.rowGroupIndex(), state.columnIndex());
                yield oi != null ? Plurals.format(oi.pageLocations().size(), "page", "pages") : "—";
            }
            case COLUMN_INDEX -> chunk.columnIndexOffset() != null ? "present" : "n/a";
            case OFFSET_INDEX -> chunk.offsetIndexOffset() != null ? "present" : "n/a";
            case DICTIONARY -> chunk.metaData().dictionaryPageOffset() != null ? "present" : "n/a";
        };
    }

    private static Block paneBlock(String title, boolean focused) {
        Block.Builder b = Block.builder()
                .title(title)
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED);
        if (!focused) {
            b.borderStyle(Theme.dim());
        }
        return b.build();
    }

    private static Line fact(String key, String value) {
        return Line.from(
                new Span(" " + padRight(key, 22), Theme.primary()),
                new Span(value, Style.EMPTY));
    }

    /// Special-case the Path row: when the path is short, a single "Path  value"
    /// line is fine; for a deeply-nested path the value would overflow the pane,
    /// so split it over two lines — key on its own line, path value indented below.
    private static List<Line> pathLines(String path) {
        // 22 is the key-padding width used by `fact`, plus 1 leading space.
        int inlineBudget = 50 - 23;
        if (path.length() <= inlineBudget) {
            return List.of(fact("Path", path));
        }
        return List.of(
                Line.from(new Span(" " + padRight("Path", 22), Theme.primary())),
                Line.from(new Span("   " + path, Style.EMPTY)));
    }

    private static String formatStatValue(byte[] bytes, ColumnSchema col, boolean useLogicalType) {
        if (bytes == null) {
            return "—";
        }
        // Facts pane has plenty of horizontal room — render the full value
        // without IndexValueFormatter's per-string 20-char cap.
        return IndexValueFormatter.format(bytes, col, useLogicalType, false);
    }

    private static String padRight(String s, int width) {
        return Strings.padRight(s, width);
    }

    private static String truncateLeft(String s, int maxWidth) {
        return Strings.truncateLeft(s, maxWidth);
    }
}
