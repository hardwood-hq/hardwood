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
import java.util.TreeMap;

import dev.hardwood.cli.dive.NavigationStack;
import dev.hardwood.cli.dive.ParquetModel;
import dev.hardwood.cli.dive.ScreenState;
import dev.hardwood.cli.internal.Sizes;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.Encoding;
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

/// Raw file layout: size, footer location, encodings / codecs histograms,
/// page-index and dictionary coverage, aggregate sizes.
public final class FooterScreen {

    private static final int FOOTER_TRAILER_BYTES = 8; // 4-byte footer length + 4-byte "PAR1" magic

    private FooterScreen() {
    }

    public static boolean handle(KeyEvent event, ParquetModel model, NavigationStack stack) {
        ScreenState.Footer state = (ScreenState.Footer) stack.top();
        FooterBody body = bodyAndAnchors(model);
        int total = body.lines().size();
        int viewport = Keys.viewportStride();
        if (Keys.isStepUp(event)) {
            if (state.cursor() == 0) {
                return false;
            }
            stack.replaceTop(new ScreenState.Footer(state.cursor() - 1));
            return true;
        }
        if (Keys.isStepDown(event)) {
            if (state.cursor() >= total - 1) {
                return false;
            }
            stack.replaceTop(new ScreenState.Footer(state.cursor() + 1));
            return true;
        }
        if (Keys.isPageDown(event)) {
            stack.replaceTop(new ScreenState.Footer(Math.min(total - 1, state.cursor() + viewport)));
            return true;
        }
        if (Keys.isPageUp(event)) {
            stack.replaceTop(new ScreenState.Footer(Math.max(0, state.cursor() - viewport)));
            return true;
        }
        if (Keys.isJumpTop(event)) {
            stack.replaceTop(new ScreenState.Footer(0));
            return true;
        }
        if (Keys.isJumpBottom(event)) {
            stack.replaceTop(new ScreenState.Footer(total - 1));
            return true;
        }
        if (event.isConfirm()) {
            if (state.cursor() == body.columnIndexLine() && body.columnIndexCount() > 0) {
                stack.push(new ScreenState.FileIndexes(
                        ScreenState.FileIndexes.Kind.COLUMN, 0));
                return true;
            }
            if (state.cursor() == body.offsetIndexLine() && body.offsetIndexCount() > 0) {
                stack.push(new ScreenState.FileIndexes(
                        ScreenState.FileIndexes.Kind.OFFSET, 0));
                return true;
            }
        }
        return false;
    }

    public static void render(Buffer buffer, Rect area, ParquetModel model, ScreenState.Footer state) {
        // Block borders + 2 rows for trailing scroll hint = 4 chrome rows.
        Keys.observeViewport(area.height() - 4);
        FooterBody body = bodyAndAnchors(model);
        List<Line> all = body.lines();
        int viewport = Math.max(1, area.height() - 4);
        int total = all.size();
        int cursor = Math.max(0, Math.min(state.cursor(), total - 1));
        // Centre the cursor in the viewport when possible.
        int maxScroll = Math.max(0, total - viewport);
        int scroll = Math.max(0, Math.min(maxScroll, cursor - viewport / 2));
        int end = Math.min(total, scroll + viewport);

        // Emit a copy of the body slice; restyle the cursor's line in-place
        // with the accent colour so it's visible on entry. Drill-target
        // lines (Column indexes / Offset indexes) get a ▶ marker when
        // hovered to advertise the Enter affordance.
        List<Line> lines = new ArrayList<>(all.subList(scroll, end));
        int cursorOffset = cursor - scroll;
        if (cursorOffset >= 0 && cursorOffset < lines.size()) {
            boolean drillable = (cursor == body.columnIndexLine() && body.columnIndexCount() > 0)
                    || (cursor == body.offsetIndexLine() && body.offsetIndexCount() > 0);
            String marker = drillable ? "▶" : " ";
            String text = renderLine(all.get(cursor));
            // Replace the leading single space with the marker, keep the rest.
            String shown = text.startsWith(" ") ? marker + text.substring(1) : marker + text;
            lines.set(cursorOffset, Line.from(
                    new Span(shown, Style.EMPTY.bold().fg(Theme.ACCENT))));
        }

        lines.add(Line.empty());
        String hint;
        if (scroll + viewport < total) {
            hint = " ↓ " + (total - end) + " more lines · ↑↓ navigate · Enter drill · PgDn/PgUp page · Esc back";
        }
        else if (scroll > 0) {
            hint = " ↑ " + scroll + " lines above · ↑↓ navigate · Enter drill · PgDn/PgUp page · Esc back";
        }
        else {
            hint = " ↑↓ navigate · Enter drill on Column / Offset indexes · Esc back";
        }
        lines.add(Line.from(new Span(hint, Style.EMPTY.fg(Theme.DIM))));

        Block block = Block.builder()
                .title(" Footer & indexes ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .borderColor(Theme.ACCENT)
                .build();
        Paragraph.builder().block(block).text(Text.from(lines)).left().build().render(area, buffer);
    }

    private static String renderLine(Line line) {
        StringBuilder sb = new StringBuilder();
        for (Span span : line.spans()) {
            sb.append(span.content());
        }
        return sb.toString();
    }

    /// Body content plus indices of the drill-target lines so Enter can
    /// be context-aware without recomputing the layout.
    private record FooterBody(
            List<Line> lines,
            int columnIndexLine,
            int columnIndexCount,
            int offsetIndexLine,
            int offsetIndexCount) {
    }

    private static FooterBody bodyAndAnchors(ParquetModel model) {
        List<Line> lines = bodyLines(model);
        FooterStats stats = computeStats(model);
        int columnIndexLine = -1;
        int offsetIndexLine = -1;
        for (int i = 0; i < lines.size(); i++) {
            String text = renderLine(lines.get(i)).trim();
            if (text.startsWith("Column indexes")) {
                columnIndexLine = i;
            }
            else if (text.startsWith("Offset indexes")) {
                offsetIndexLine = i;
            }
        }
        return new FooterBody(lines, columnIndexLine, stats.columnIndexCount(),
                offsetIndexLine, stats.offsetIndexCount());
    }

    private static List<Line> bodyLines(ParquetModel model) {
        FooterStats stats = computeStats(model);
        long fileSize = model.fileSizeBytes();
        long footerTrailerOffset = fileSize - FOOTER_TRAILER_BYTES;

        List<Line> lines = new ArrayList<>();

        lines.add(Line.from(new Span(" File layout ", Style.EMPTY.bold())));
        lines.add(fact("  File size", dualSize(fileSize)));
        lines.add(fact("  Format version", String.valueOf(model.metadata().version())));
        lines.add(fact("  Created by",
                model.facts().createdBy() != null ? model.facts().createdBy() : "unknown"));
        lines.add(fact("  Footer trailer offset", String.format("%,d", footerTrailerOffset)));
        lines.add(fact("  Trailer bytes", String.valueOf(FOOTER_TRAILER_BYTES)));
        if (stats.minDataOffset() < Long.MAX_VALUE) {
            lines.add(fact("  Data region",
                    String.format("%,d .. %,d  (%s)",
                            stats.minDataOffset(), stats.maxDataEnd(),
                            Sizes.format(stats.maxDataEnd() - stats.minDataOffset()))));
            lines.add(fact("  Footer + indexes",
                    dualSize(footerAndIndexBytes(model))));
        }

        lines.add(Line.empty());
        lines.add(Line.from(new Span(" Encodings ", Style.EMPTY.bold())));
        for (Map.Entry<Encoding, Integer> e : stats.encodingHistogram().entrySet()) {
            lines.add(fact("  " + e.getKey().name(),
                    Plurals.format(e.getValue(), "chunk", "chunks")));
        }

        lines.add(Line.empty());
        lines.add(Line.from(new Span(" Codecs ", Style.EMPTY.bold())));
        for (Map.Entry<CompressionCodec, Integer> e : stats.codecHistogram().entrySet()) {
            int pct = stats.totalChunks() == 0 ? 0
                    : (int) Math.round(100.0 * e.getValue() / stats.totalChunks());
            lines.add(fact("  " + e.getKey().name(),
                    Plurals.format(e.getValue(), "chunk", "chunks") + "  (" + pct + "%)"));
        }

        lines.add(Line.empty());
        lines.add(Line.from(new Span(" Page indexes ", Style.EMPTY.bold())));
        lines.add(fact("  Column indexes",
                dualSize(stats.columnIndexBytes()) + "  ("
                        + coverage(stats.columnIndexCount(), stats.totalChunks()) + ")"));
        lines.add(fact("  Offset indexes",
                dualSize(stats.offsetIndexBytes()) + "  ("
                        + coverage(stats.offsetIndexCount(), stats.totalChunks()) + ")"));

        lines.add(Line.empty());
        lines.add(Line.from(new Span(" Dictionary ", Style.EMPTY.bold())));
        lines.add(fact("  With dictionary",
                coverage(stats.dictionaryCount(), stats.totalChunks())));

        lines.add(Line.empty());
        lines.add(Line.from(new Span(" Aggregate ", Style.EMPTY.bold())));
        lines.add(fact("  Compressed data", dualSize(model.facts().compressedBytes())));
        lines.add(fact("  Uncompressed data", dualSize(model.facts().uncompressedBytes())));
        lines.add(fact("  Compression ratio",
                String.format("%.2f×", model.facts().compressionRatio())));
        return lines;
    }

    public static String keybarKeys() {
        return "[↑↓] scroll  [PgDn/PgUp] page  [g/G] top/bottom  [Esc] back";
    }

    /// Total bytes occupied by the footer thrift + page indexes + trailer —
    /// everything past the data region. Used both here and by the Overview
    /// drill-into hint so the menu shows the size of "footer & indexes",
    /// not the whole file.
    public static long footerAndIndexBytes(ParquetModel model) {
        long fileSize = model.fileSizeBytes();
        long maxDataEnd = 0;
        for (RowGroup rg : model.metadata().rowGroups()) {
            for (ColumnChunk cc : rg.columns()) {
                long chunkEnd = chunkEnd(cc);
                if (chunkEnd > maxDataEnd) {
                    maxDataEnd = chunkEnd;
                }
            }
        }
        if (maxDataEnd == 0) {
            return fileSize;
        }
        return Math.max(0, fileSize - maxDataEnd);
    }

    private static long chunkEnd(ColumnChunk cc) {
        ColumnMetaData cmd = cc.metaData();
        Long dict = cmd.dictionaryPageOffset();
        long start = dict != null ? Math.min(dict, cmd.dataPageOffset()) : cmd.dataPageOffset();
        return start + cmd.totalCompressedSize();
    }

    private record FooterStats(
            long minDataOffset, long maxDataEnd,
            int totalChunks,
            int columnIndexCount, long columnIndexBytes,
            int offsetIndexCount, long offsetIndexBytes,
            int dictionaryCount,
            Map<Encoding, Integer> encodingHistogram,
            Map<CompressionCodec, Integer> codecHistogram) {
    }

    private static FooterStats computeStats(ParquetModel model) {
        long minDataOffset = Long.MAX_VALUE;
        long maxDataEnd = 0;
        int totalChunks = 0;
        int columnIndexCount = 0;
        long columnIndexBytes = 0;
        int offsetIndexCount = 0;
        long offsetIndexBytes = 0;
        int dictionaryCount = 0;
        Map<Encoding, Integer> encodingHistogram = new TreeMap<>();
        Map<CompressionCodec, Integer> codecHistogram = new TreeMap<>();
        for (RowGroup rg : model.metadata().rowGroups()) {
            for (ColumnChunk cc : rg.columns()) {
                totalChunks++;
                ColumnMetaData cmd = cc.metaData();
                Long dict = cmd.dictionaryPageOffset();
                long start = dict != null ? Math.min(dict, cmd.dataPageOffset()) : cmd.dataPageOffset();
                long end = start + cmd.totalCompressedSize();
                if (start < minDataOffset) {
                    minDataOffset = start;
                }
                if (end > maxDataEnd) {
                    maxDataEnd = end;
                }
                if (cc.columnIndexLength() != null) {
                    columnIndexCount++;
                    columnIndexBytes += cc.columnIndexLength();
                }
                if (cc.offsetIndexLength() != null) {
                    offsetIndexCount++;
                    offsetIndexBytes += cc.offsetIndexLength();
                }
                if (dict != null) {
                    dictionaryCount++;
                }
                for (Encoding e : cmd.encodings()) {
                    encodingHistogram.merge(e, 1, Integer::sum);
                }
                codecHistogram.merge(cmd.codec(), 1, Integer::sum);
            }
        }
        return new FooterStats(minDataOffset, maxDataEnd, totalChunks,
                columnIndexCount, columnIndexBytes,
                offsetIndexCount, offsetIndexBytes,
                dictionaryCount,
                sortedByCount(encodingHistogram),
                sortedByCount(codecHistogram));
    }

    private static <K> Map<K, Integer> sortedByCount(Map<K, Integer> in) {
        List<Map.Entry<K, Integer>> entries = new ArrayList<>(in.entrySet());
        entries.sort((a, b) -> Integer.compare(b.getValue(), a.getValue()));
        Map<K, Integer> out = new LinkedHashMap<>();
        for (Map.Entry<K, Integer> e : entries) {
            out.put(e.getKey(), e.getValue());
        }
        return out;
    }

    private static String dualSize(long bytes) {
        return Sizes.format(bytes) + "  (" + String.format("%,d", bytes) + " B)";
    }

    private static String coverage(int count, int total) {
        if (total == 0) {
            return "0/0";
        }
        int pct = (int) Math.round(100.0 * count / total);
        return String.format("%,d/%,d chunks  (%d%%)", count, total, pct);
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
