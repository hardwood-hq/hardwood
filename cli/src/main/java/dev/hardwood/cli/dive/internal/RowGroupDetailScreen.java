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
import dev.hardwood.cli.internal.Fmt;
import dev.hardwood.cli.internal.Sizes;
import dev.hardwood.cli.internal.Strings;
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
                    state.rowGroupIndex(), next, state.menuSelection(), state.scrollTop(),
                    state.factsTop()));
            return true;
        }
        if (state.focus() != ScreenState.RowGroupDetail.Pane.MENU) {
            return scrollFacts(event, model, stack, state);
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

    public static String keybarKeys(ScreenState.RowGroupDetail state, ParquetModel model) {
        boolean onMenu = state.focus() == ScreenState.RowGroupDetail.Pane.MENU;
        return new Keys.Hints()
                .add(true, "[Tab] pane")
                .add(onMenu && MenuItem.values().length > 1, "[↑↓] move")
                .add(!onMenu, factsLines(model, state).hints(Keys.viewportStride()))
                .add(onMenu, "[Enter] open")
                .add(true, "[Esc] back")
                .build();
    }

    /// Moves the facts cursor, which is what the navigation keys mean while
    /// the pane has focus. Nothing here is actionable, so the cursor carries
    /// no marker; it is the reader's place in the pane.
    private static boolean scrollFacts(KeyEvent event, ParquetModel model, NavigationStack stack,
                                       ScreenState.RowGroupDetail state) {
        int next = factsLines(model, state).select(event, state.scrollTop(), Keys.viewportStride());
        if (next == CursorPane.UNHANDLED) {
            return false;
        }
        if (next != state.scrollTop()) {
            stack.replaceTop(new ScreenState.RowGroupDetail(
                    state.rowGroupIndex(), state.focus(), state.menuSelection(), next,
                    factsLines(model, state).windowTopAfterMove(state.factsTop(),
                            state.scrollTop(), next, Keys.viewportStride())));
        }
        return true;
    }

    private static ScreenState.RowGroupDetail state(ScreenState.RowGroupDetail s, int selection) {
        return new ScreenState.RowGroupDetail(s.rowGroupIndex(), s.focus(), selection, s.scrollTop(),
                s.factsTop());
    }

    private static void renderFactsPane(Buffer buffer, Rect area, ParquetModel model,
                                        ScreenState.RowGroupDetail state) {
        boolean focused = state.focus() == ScreenState.RowGroupDetail.Pane.FACTS;
        Document facts = factsLines(model, state);
        // The facts pane is the only thing the arrows move on this screen, so
        // its height is the stride the key handler should use.
        int viewport = factsViewport(area);
        Keys.observeViewport(viewport);
        int cursorLine = Math.max(0, facts.lineOfRow(state.scrollTop()));
        RowWindow window = RowWindow.from(
                facts.windowTop(state.factsTop(), state.scrollTop(), viewport),
                cursorLine, facts.lineCount(), viewport);
        Block block = paneBlock(" RG #" + state.rowGroupIndex() + " ", focused);
        Paragraph.builder()
                .block(block)
                .text(Text.from(withCursor(
                        facts.lines().subList(window.start(), window.end()),
                        cursorLine - window.start(), focused)))
                .left()
                .build()
                .render(area, buffer);
    }

    /// The facts pane's full content, independent of how much of it fits.
    /// Shared with the keybar, which needs the line count to decide whether
    /// the pane scrolls.
    private static Document factsLines(ParquetModel model, ScreenState.RowGroupDetail state) {
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
        Document.Builder lines = Document.builder();
        lines.row(fact("Row group index", String.valueOf(state.rowGroupIndex())));
        lines.row(fact("Rows", Fmt.fmt("%,d", rg.numRows())));
        lines.row(fact("Column chunks", String.valueOf(chunkCount)));
        lines.row(fact("Total byte size", Sizes.dualFormat(rg.totalByteSize())));
        lines.blank();
        // "Storage" rather than "Compression": the group holds a `Compression`
        // row of its own now, and it is the same vocabulary the column chunk
        // detail screen groups the same three figures under.
        lines.decoration(Line.from(new Span(" Storage ", Theme.accent().bold())));
        lines.row(fact("  Compressed", Sizes.dualFormat(compressed)));
        lines.row(fact("  Uncompressed", Sizes.dualFormat(uncompressed)));
        lines.row(fact("  Compression", Sizes.compression(compressed, uncompressed)));
        lines.blank();
        lines.decoration(Line.from(new Span(" Encoding mix ", Theme.accent().bold())));
        lines.row(fact("  Encodings", mix(encodingCounts)));
        lines.row(fact("  Codecs", mix(codecCounts)));
        lines.blank();
        lines.decoration(Line.from(new Span(" Page indexes ", Theme.accent().bold())));
        lines.row(fact("  Column indexes", Sizes.dualFormat(ciBytes)
                + "  (" + ciCount + "/" + chunkCount + " chunks)"));
        lines.row(fact("  Offset indexes", Sizes.dualFormat(oiBytes)
                + "  (" + oiCount + "/" + chunkCount + " chunks)"));

        return lines.build();
    }


    /// Repaints the cursor line in the selection colour. Nothing in a facts
    /// pane is actionable, so the cursor carries no marker — the colour alone
    /// is the reader's place in the pane.
    private static List<Line> withCursor(List<Line> window, int offset, boolean focused) {
        if (!focused || offset < 0 || offset >= window.size()) {
            return window;
        }
        List<Line> painted = new ArrayList<>(window);
        StringBuilder text = new StringBuilder();
        for (Span span : window.get(offset).spans()) {
            text.append(span.content());
        }
        painted.set(offset, Line.from(new Span(text.toString(), Theme.selection())));
        return painted;
    }

    /// Block borders take a row at the top and bottom of the pane.
    private static int factsViewport(Rect area) {
        return Math.max(1, area.height() - 2);
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
            String cursor = CursorPane.marker(true, selected, false);
            Style labelStyle = selected
                    ? Theme.selection()
                    : Theme.primary();
            lines.add(Line.from(
                    new Span(cursor, labelStyle),
                    new Span(item.label, labelStyle)));
        }
        Paragraph.builder().block(block).text(Text.from(lines)).left().build().render(area, buffer);
    }

    private static String mix(Map<String, Integer> counts) {
        if (counts.isEmpty()) {
            return Strings.ABSENT_VALUE;
        }
        return counts.entrySet().stream()
                .map(e -> e.getKey() + " (" + e.getValue() + ")")
                .collect(Collectors.joining(", "));
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

    private static String padRight(String s, int width) {
        return Strings.padRight(s, width);
    }
}
