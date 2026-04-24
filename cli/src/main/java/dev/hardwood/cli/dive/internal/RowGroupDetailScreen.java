/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.dive.internal;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import dev.hardwood.cli.dive.NavigationStack;
import dev.hardwood.cli.dive.ParquetModel;
import dev.hardwood.cli.dive.ScreenState;
import dev.hardwood.cli.internal.Sizes;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.RowGroup;
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

/// Row-group-level overview. Two panes: facts (row count, bytes,
/// compression, encoding / codec mix, index aggregates) and a drill
/// menu pushing into Column chunks or Indexes-for-this-RG.
public final class RowGroupDetailScreen {

    public enum MenuItem {
        COLUMN_CHUNKS("Column chunks"),
        INDEXES("Indexes for this RG");

        final String label;

        MenuItem(String label) {
            this.label = label;
        }
    }

    private RowGroupDetailScreen() {
    }

    public static boolean handle(KeyEvent event, ParquetModel model, NavigationStack stack) {
        ScreenState.RowGroupDetail state = (ScreenState.RowGroupDetail) stack.top();
        if (event.isFocusNext() || event.isFocusPrevious()) {
            ScreenState.RowGroupDetail.Pane next = state.focus() == ScreenState.RowGroupDetail.Pane.FACTS
                    ? ScreenState.RowGroupDetail.Pane.MENU
                    : ScreenState.RowGroupDetail.Pane.FACTS;
            stack.replaceTop(new ScreenState.RowGroupDetail(
                    state.rowGroupIndex(), next, state.menuSelection()));
            return true;
        }
        if (state.focus() != ScreenState.RowGroupDetail.Pane.MENU) {
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
            switch (item) {
                case COLUMN_CHUNKS -> stack.push(new ScreenState.ColumnChunks(state.rowGroupIndex(), 0));
                case INDEXES -> stack.push(new ScreenState.RowGroupIndexes(state.rowGroupIndex(), 0));
            }
            return true;
        }
        return false;
    }

    public static void render(Buffer buffer, Rect area, ParquetModel model, ScreenState.RowGroupDetail state) {
        List<Rect> cols = Layout.horizontal()
                .constraints(new Constraint.Percentage(60), new Constraint.Percentage(40))
                .split(area);
        renderFactsPane(buffer, cols.get(0), model, state);
        renderMenuPane(buffer, cols.get(1), model, state);
    }

    public static String keybarKeys() {
        return "[Tab] pane  [↑↓] move  [Enter] drill  [Esc] back";
    }

    private static ScreenState.RowGroupDetail state(ScreenState.RowGroupDetail s, int selection) {
        return new ScreenState.RowGroupDetail(s.rowGroupIndex(), s.focus(), selection);
    }

    private static void renderFactsPane(Buffer buffer, Rect area, ParquetModel model,
                                        ScreenState.RowGroupDetail state) {
        boolean focused = state.focus() == ScreenState.RowGroupDetail.Pane.FACTS;
        RowGroup rg = model.rowGroup(state.rowGroupIndex());
        long compressed = 0;
        long uncompressed = 0;
        long ciBytes = 0;
        long oiBytes = 0;
        int ciCount = 0;
        int oiCount = 0;
        Map<String, Integer> encodingCounts = new LinkedHashMap<>();
        Map<String, Integer> codecCounts = new LinkedHashMap<>();
        int chunkCount = rg.columns().size();
        for (ColumnChunk cc : rg.columns()) {
            ColumnMetaData cmd = cc.metaData();
            compressed += cmd.totalCompressedSize();
            uncompressed += cmd.totalUncompressedSize();
            for (Encoding enc : cmd.encodings()) {
                encodingCounts.merge(enc.name(), 1, Integer::sum);
            }
            codecCounts.merge(cmd.codec().name(), 1, Integer::sum);
            if (cc.columnIndexLength() != null) {
                ciBytes += cc.columnIndexLength();
                ciCount++;
            }
            if (cc.offsetIndexLength() != null) {
                oiBytes += cc.offsetIndexLength();
                oiCount++;
            }
        }
        double ratio = compressed == 0 ? 0.0 : (double) uncompressed / compressed;

        List<Line> lines = new ArrayList<>();
        lines.add(fact("Row group index", String.valueOf(state.rowGroupIndex())));
        lines.add(fact("Rows", String.format("%,d", rg.numRows())));
        lines.add(fact("Column chunks", String.valueOf(chunkCount)));
        lines.add(fact("Total byte size", dualSize(rg.totalByteSize())));
        lines.add(Line.empty());
        lines.add(Line.from(new Span(" Compression ", Style.EMPTY.bold())));
        lines.add(fact("  Compressed", dualSize(compressed)));
        lines.add(fact("  Uncompressed", dualSize(uncompressed)));
        lines.add(fact("  Ratio", String.format("%.2f×", ratio)));
        lines.add(Line.empty());
        lines.add(Line.from(new Span(" Encoding mix ", Style.EMPTY.bold())));
        lines.add(fact("  Encodings", mix(encodingCounts)));
        lines.add(fact("  Codecs", mix(codecCounts)));
        lines.add(Line.empty());
        lines.add(Line.from(new Span(" Page indexes ", Style.EMPTY.bold())));
        lines.add(fact("  Column indexes", dualSize(ciBytes)
                + "  (" + ciCount + "/" + chunkCount + " chunks)"));
        lines.add(fact("  Offset indexes", dualSize(oiBytes)
                + "  (" + oiCount + "/" + chunkCount + " chunks)"));

        Block block = paneBlock(" RG #" + state.rowGroupIndex() + " ", focused);
        Paragraph.builder().block(block).text(Text.from(lines)).left().build().render(area, buffer);
    }

    private static void renderMenuPane(Buffer buffer, Rect area, ParquetModel model,
                                       ScreenState.RowGroupDetail state) {
        boolean focused = state.focus() == ScreenState.RowGroupDetail.Pane.MENU;
        Block block = paneBlock(" Drill into ", focused);
        List<Line> lines = new ArrayList<>();
        MenuItem[] items = MenuItem.values();
        for (int i = 0; i < items.length; i++) {
            MenuItem item = items[i];
            boolean selected = focused && i == state.menuSelection();
            String cursor = selected ? "▶ " : "  ";
            Style labelStyle = selected ? Style.EMPTY.bold() : Style.EMPTY;
            lines.add(Line.from(
                    new Span(cursor, labelStyle),
                    new Span(item.label, labelStyle)));
        }
        Paragraph.builder().block(block).text(Text.from(lines)).left().build().render(area, buffer);
    }

    private static String mix(Map<String, Integer> counts) {
        if (counts.isEmpty()) {
            return "—";
        }
        return counts.entrySet().stream()
                .map(e -> e.getKey() + " (" + e.getValue() + ")")
                .collect(Collectors.joining(", "));
    }

    private static String dualSize(long bytes) {
        return Sizes.format(bytes) + "  (" + String.format("%,d", bytes) + " B)";
    }

    private static Block paneBlock(String title, boolean focused) {
        return Block.builder()
                .title(title)
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .borderColor(focused ? Theme.ACCENT : Theme.DIM)
                .build();
    }

    private static Line fact(String key, String value) {
        return Line.from(
                new Span(" " + padRight(key, 22), Style.EMPTY),
                new Span(value, Style.EMPTY.bold()));
    }

    private static String padRight(String s, int width) {
        if (s.length() >= width) {
            return s;
        }
        return s + " ".repeat(width - s.length());
    }
}
