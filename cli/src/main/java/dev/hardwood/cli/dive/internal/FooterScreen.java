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
import dev.hardwood.cli.internal.Sizes;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.RowGroup;
import dev.tamboui.buffer.Buffer;
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

/// Raw file layout: size, footer location, aggregate bytes occupied by column
/// indexes / offset indexes / bloom filters. No drill targets in phase 2.
public final class FooterScreen {

    private static final int FOOTER_TRAILER_BYTES = 8; // 4-byte footer length + 4-byte "PAR1" magic

    private FooterScreen() {
    }

    public static boolean handle(KeyEvent event, ParquetModel model, NavigationStack stack) {
        return false;
    }

    public static void render(Buffer buffer, Rect area, ParquetModel model) {
        long fileSize = model.fileSizeBytes();
        long columnIndexBytes = 0;
        long offsetIndexBytes = 0;
        for (RowGroup rg : model.metadata().rowGroups()) {
            for (ColumnChunk cc : rg.columns()) {
                if (cc.columnIndexLength() != null) {
                    columnIndexBytes += cc.columnIndexLength();
                }
                if (cc.offsetIndexLength() != null) {
                    offsetIndexBytes += cc.offsetIndexLength();
                }
            }
        }
        long footerApproxOffset = fileSize - FOOTER_TRAILER_BYTES;

        List<Line> lines = new ArrayList<>();
        lines.add(fact("File size", dualSize(fileSize)));
        lines.add(fact("Footer trailer offset", String.format("%,d", footerApproxOffset)));
        lines.add(fact("Trailer bytes", String.valueOf(FOOTER_TRAILER_BYTES)));
        lines.add(Line.empty());
        lines.add(Line.from(new Span(" Page indexes ", Style.EMPTY.bold())));
        lines.add(fact("  Column indexes", dualSize(columnIndexBytes)
                + " across " + chunkCount(model) + " chunks"));
        lines.add(fact("  Offset indexes", dualSize(offsetIndexBytes)
                + " across " + chunkCount(model) + " chunks"));
        lines.add(Line.empty());
        lines.add(Line.from(new Span(" Aggregate ", Style.EMPTY.bold())));
        lines.add(fact("  Compressed data", dualSize(model.facts().compressedBytes())));
        lines.add(fact("  Uncompressed data", dualSize(model.facts().uncompressedBytes())));
        lines.add(fact("  Compression ratio", String.format("%.2f×", model.facts().compressionRatio())));

        Block block = Block.builder()
                .title(" Footer & indexes ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .borderColor(Theme.ACCENT)
                .build();
        Paragraph.builder().block(block).text(Text.from(lines)).left().build().render(area, buffer);
    }

    public static String keybarKeys() {
        return "[Esc] back";
    }

    private static String dualSize(long bytes) {
        return Sizes.format(bytes) + "  (" + String.format("%,d", bytes) + " B)";
    }

    private static int chunkCount(ParquetModel model) {
        int total = 0;
        for (RowGroup rg : model.metadata().rowGroups()) {
            total += rg.columns().size();
        }
        return total;
    }

    private static Line fact(String key, String value) {
        return Line.from(
                new Span(" " + padRight(key, 26), Style.EMPTY),
                new Span(value, Style.EMPTY.bold()));
    }

    private static String padRight(String s, int width) {
        if (s.length() >= width) {
            return s;
        }
        return s + " ".repeat(width - s.length());
    }
}
