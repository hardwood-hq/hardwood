/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.dive.internal;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import dev.hardwood.cli.dive.NavigationStack;
import dev.hardwood.cli.dive.ParquetModel;
import dev.hardwood.cli.dive.ScreenState;
import dev.hardwood.internal.reader.Dictionary;
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

/// Dictionary entries for one column chunk. Enter opens a modal with the full
/// un-truncated value (useful for long BYTE_ARRAY payloads).
public final class DictionaryScreen {

    private static final int VALUE_PREVIEW_MAX = 60;

    private DictionaryScreen() {
    }

    public static boolean handle(KeyEvent event, ParquetModel model, NavigationStack stack) {
        ScreenState.DictionaryView state = (ScreenState.DictionaryView) stack.top();
        if (state.modalOpen()) {
            if (event.isCancel() || event.isConfirm()) {
                stack.replaceTop(new ScreenState.DictionaryView(
                        state.rowGroupIndex(), state.columnIndex(), state.selection(), false));
                return true;
            }
            return false;
        }
        Dictionary dict = model.dictionary(state.rowGroupIndex(), state.columnIndex());
        int count = dict != null ? dict.size() : 0;
        if (event.isUp()) {
            stack.replaceTop(new ScreenState.DictionaryView(
                    state.rowGroupIndex(), state.columnIndex(),
                    Math.max(0, state.selection() - 1), false));
            return true;
        }
        if (event.isDown()) {
            stack.replaceTop(new ScreenState.DictionaryView(
                    state.rowGroupIndex(), state.columnIndex(),
                    Math.min(count - 1, state.selection() + 1), false));
            return true;
        }
        if (event.isConfirm() && count > 0) {
            stack.replaceTop(new ScreenState.DictionaryView(
                    state.rowGroupIndex(), state.columnIndex(), state.selection(), true));
            return true;
        }
        return false;
    }

    public static void render(Buffer buffer, Rect area, ParquetModel model, ScreenState.DictionaryView state) {
        Dictionary dict = model.dictionary(state.rowGroupIndex(), state.columnIndex());
        if (dict == null) {
            renderEmpty(buffer, area);
            return;
        }

        List<Row> rows = new ArrayList<>();
        int size = dict.size();
        for (int i = 0; i < size; i++) {
            rows.add(Row.from(
                    "[" + i + "]",
                    formatValue(dict, i, VALUE_PREVIEW_MAX)));
        }
        Row header = Row.from("#", "Value").style(Style.EMPTY.bold());
        Block block = Block.builder()
                .title(" Dictionary (" + size + " entries) ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .borderColor(Color.CYAN)
                .build();
        Table table = Table.builder()
                .header(header)
                .rows(rows)
                .widths(new Constraint.Length(8), new Constraint.Fill(1))
                .columnSpacing(2)
                .block(block)
                .highlightSymbol("▶ ")
                .highlightStyle(Style.EMPTY.bold())
                .build();
        TableState tableState = new TableState();
        if (size > 0) {
            tableState.select(state.selection());
        }
        table.render(area, buffer, tableState);

        if (state.modalOpen() && size > 0) {
            renderValueModal(buffer, area, dict, state.selection());
        }
    }

    public static String keybarKeys() {
        return "[↑↓] move  [Enter] full value  [Esc] back";
    }

    private static String formatValue(Dictionary dict, int index, int max) {
        String full = fullValue(dict, index);
        if (full.length() <= max) {
            return full;
        }
        return full.substring(0, max - 1) + "…";
    }

    private static String fullValue(Dictionary dict, int index) {
        return switch (dict) {
            case Dictionary.IntDictionary d -> Integer.toString(d.values()[index]);
            case Dictionary.LongDictionary d -> Long.toString(d.values()[index]);
            case Dictionary.FloatDictionary d -> Float.toString(d.values()[index]);
            case Dictionary.DoubleDictionary d -> Double.toString(d.values()[index]);
            case Dictionary.ByteArrayDictionary d -> formatBytes(d.values()[index]);
        };
    }

    private static String formatBytes(byte[] bytes) {
        if (bytes == null) {
            return "null";
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static void renderEmpty(Buffer buffer, Rect area) {
        Block block = Block.builder()
                .title(" Dictionary ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .borderColor(Color.GRAY)
                .build();
        Paragraph.builder()
                .block(block)
                .text(Text.from(Line.from(new Span(
                        " This chunk is not dictionary-encoded.",
                        Style.EMPTY.fg(Color.GRAY)))))
                .left()
                .build()
                .render(area, buffer);
    }

    private static void renderValueModal(Buffer buffer, Rect screenArea, Dictionary dict, int index) {
        int width = Math.min(80, screenArea.width() - 4);
        int height = Math.min(16, screenArea.height() - 2);
        int x = screenArea.left() + (screenArea.width() - width) / 2;
        int y = screenArea.top() + (screenArea.height() - height) / 2;
        Rect area = new Rect(x, y, width, height);

        String full = fullValue(dict, index);
        List<Line> lines = new ArrayList<>();
        lines.add(Line.empty());
        lines.add(Line.from(Span.raw(" " + full)));
        lines.add(Line.empty());
        lines.add(Line.from(new Span(" Press Esc or Enter to close", Style.EMPTY.fg(Color.GRAY))));

        Block block = Block.builder()
                .title(" Entry #" + index + " ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .borderColor(Color.CYAN)
                .build();
        Paragraph.builder().block(block).text(Text.from(lines)).left().build().render(area, buffer);
    }
}
