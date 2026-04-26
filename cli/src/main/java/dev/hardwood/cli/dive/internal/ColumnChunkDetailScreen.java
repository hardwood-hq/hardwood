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

/// Full metadata dump for one `(rowGroup, column)` chunk. In phase 1 this is a
/// single facts pane; phase 2 adds a drill-into menu alongside for pages, column
/// index, offset index, and dictionary.
public final class ColumnChunkDetailScreen {

    private ColumnChunkDetailScreen() {
    }

    public static boolean handle(KeyEvent event, ParquetModel model, NavigationStack stack) {
        return false;
    }

    public static void render(Buffer buffer, Rect area, ParquetModel model, ScreenState.ColumnChunkDetail state) {
        ColumnChunk chunk = model.chunk(state.rowGroupIndex(), state.columnIndex());
        ColumnMetaData cmd = chunk.metaData();
        ColumnSchema col = model.schema().getColumn(state.columnIndex());

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
        Statistics stats = cmd.statistics();
        lines.add(fact("Nulls", stats != null && stats.nullCount() != null
                ? String.format("%,d", stats.nullCount())
                : "—"));
        lines.add(fact("Uncompressed", Sizes.format(cmd.totalUncompressedSize())));
        lines.add(fact("Compressed", Sizes.format(cmd.totalCompressedSize())));
        lines.add(fact("Min", formatStatValue(stats != null ? stats.minValue() : null, col)));
        lines.add(fact("Max", formatStatValue(stats != null ? stats.maxValue() : null, col)));

        Block block = Block.builder()
                .title(" " + Sizes.columnPath(cmd) + " (RG #" + state.rowGroupIndex() + ") ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .borderColor(Color.CYAN)
                .build();
        Paragraph.builder().block(block).text(Text.from(lines)).left().build().render(area, buffer);
    }

    public static String keybarKeys() {
        return "[Esc] back";
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
