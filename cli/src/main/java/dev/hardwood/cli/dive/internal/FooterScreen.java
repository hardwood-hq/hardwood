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
import dev.hardwood.cli.internal.Fmt;
import dev.hardwood.cli.internal.Sizes;
import dev.hardwood.cli.internal.Strings;
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

    /// The state to push when opening the screen: the cursor on the first
    /// anchor that goes somewhere, so a file missing the section we would
    /// otherwise land on does not start on a dead line.
    ///
    /// Chosen here rather than corrected on each keypress — a screen that
    /// re-snapped the cursor every event would drag it back to an anchor the
    /// moment the reader moved off one.
    public static ScreenState.Footer initialState(ParquetModel model) {
        FooterBody body = bodyAndAnchors(model);
        int row = Math.max(0, body.document().rowAtLine(firstEnabledAnchorLine(body)));
        return new ScreenState.Footer(row);
    }

    public static boolean handle(KeyEvent event, ParquetModel model, NavigationStack stack) {
        ScreenState.Footer state = (ScreenState.Footer) stack.top();
        FooterBody body = bodyAndAnchors(model);
        int selected = body.document().select(event, state.cursorRow(), Keys.viewportStride());
        if (selected != CursorPane.UNHANDLED) {
            stack.replaceTop(new ScreenState.Footer(selected,
                    body.document().windowTopAfterMove(state.windowTop(), state.cursorRow(),
                            selected, Keys.viewportStride())));
            return true;
        }
        if (event.isConfirm()) {
            ScreenState.FileIndexes.Kind kind = kindAt(cursorLine(state, body), body);
            if (kind != null) {
                stack.push(new ScreenState.FileIndexes(kind, 0));
                return true;
            }
        }
        return false;
    }

    /// The body line the cursor is on. The state holds a row index; the
    /// rows are the lines `↑`/`↓` stop on, which excludes the section
    /// headings and the blanks between them.
    private static int cursorLine(ScreenState.Footer state, FooterBody body) {
        return Math.max(0, body.document().lineOfRow(state.cursorRow()));
    }

    /// The screen `Enter` opens from `line`, or null when the line is not an
    /// anchor or its section is empty.
    private static ScreenState.FileIndexes.Kind kindAt(int line, FooterBody body) {
        if (line == body.columnIndexLine() && body.columnIndexCount() > 0) {
            return ScreenState.FileIndexes.Kind.COLUMN;
        }
        if (line == body.offsetIndexLine() && body.offsetIndexCount() > 0) {
            return ScreenState.FileIndexes.Kind.OFFSET;
        }
        if (line == body.dictionaryLine() && body.dictionaryCount() > 0) {
            return ScreenState.FileIndexes.Kind.DICTIONARY;
        }
        return null;
    }

    private static boolean isAnchorLine(int line, FooterBody body) {
        return kindAt(line, body) != null;
    }

    private static int firstEnabledAnchorLine(FooterBody body) {
        int best = -1;
        for (int line : anchorLines(body)) {
            if (line >= 0 && (best < 0 || line < best)) {
                best = line;
            }
        }
        return best;
    }

    /// Body lines carrying an anchor `Enter` can act on, `-1` where the file
    /// has none of that kind.
    private static int[] anchorLines(FooterBody body) {
        return new int[] {
            body.columnIndexCount() > 0 ? body.columnIndexLine() : -1,
            body.offsetIndexCount() > 0 ? body.offsetIndexLine() : -1,
            body.dictionaryCount() > 0 ? body.dictionaryLine() : -1,
        };
    }

    public static void render(Buffer buffer, Rect area, ParquetModel model, ScreenState.Footer state) {
        // Block borders only — the in-body scroll hint was dropped earlier
        // in favor of the keybar carrying that information, so the body
        // chrome is just 2 rows (top + bottom border).
        Keys.observeViewport(area.height() - 2);
        FooterBody body = bodyAndAnchors(model);
        int cursorLine = cursorLine(state, body);
        List<Line> all = body.lines();
        int viewport = Math.max(1, area.height() - 2);
        RowWindow window = RowWindow.from(
                body.document().windowTop(state.windowTop(), state.cursorRow(), viewport),
                cursorLine, all.size(), viewport);
        int scroll = window.start();

        // Every anchor Enter can act on is marked, so they are discoverable
        // without arrowing onto each one; the cursor is the coloured line.
        List<Line> lines = new ArrayList<>(all.subList(scroll, window.end()));
        styleAnchor(lines, all, scroll, body.columnIndexLine(),
                cursorLine == body.columnIndexLine(), body.columnIndexCount() > 0);
        styleAnchor(lines, all, scroll, body.offsetIndexLine(),
                cursorLine == body.offsetIndexLine(), body.offsetIndexCount() > 0);
        styleAnchor(lines, all, scroll, body.dictionaryLine(),
                cursorLine == body.dictionaryLine(), body.dictionaryCount() > 0);
        // The cursor is coloured wherever it is, not only on the anchors:
        // painting it on three of thirty lines left it invisible everywhere
        // else, so the reader could not see what the arrows were moving.
        styleCursor(lines, all, scroll, cursorLine, kindAt(cursorLine, body) == null);

        Block block = Block.builder()
                .title(" Footer & indexes ")
                .borders(Borders.ALL)
                .borderType(BorderType.ROUNDED)
                .build();
        Paragraph.builder().block(block).text(Text.from(lines)).left().build().render(area, buffer);
    }

    /// Paints the cursor on a line the anchors did not already claim: the
    /// selection colour, and no marker, since `Enter` does nothing here.
    private static void styleCursor(List<Line> visible, List<Line> all, int scroll,
                                    int cursorLine, boolean paint) {
        if (!paint || cursorLine < 0) {
            return;
        }
        int offset = cursorLine - scroll;
        if (offset < 0 || offset >= visible.size()) {
            return;
        }
        visible.set(offset, Line.from(
                new Span(renderLine(all.get(cursorLine)), Theme.selection())));
    }

    /// Paints one anchor line: `▶` when `Enter` can act on it, and the
    /// selection colour when it is the line the cursor is on.
    private static void styleAnchor(List<Line> visible, List<Line> all, int scroll,
                                    int absoluteLine, boolean cursor, boolean enabled) {
        if (absoluteLine < 0) {
            return;
        }
        int offset = absoluteLine - scroll;
        if (offset < 0 || offset >= visible.size()) {
            return;
        }
        String text = renderLine(all.get(absoluteLine));
        // The body indents its rows by one cell, which the marker takes over.
        String marker = enabled ? "▶" : " ";
        String shown = text.startsWith(" ") ? marker + text.substring(1) : marker + text;
        visible.set(offset, Line.from(new Span(shown, cursor ? Theme.selection() : Theme.primary())));
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
            Document document,
            int columnIndexLine,
            int columnIndexCount,
            int offsetIndexLine,
            int offsetIndexCount,
            int dictionaryLine,
            int dictionaryCount) {

        List<Line> lines() {
            return document.lines();
        }
    }

    private static FooterBody bodyAndAnchors(ParquetModel model) {
        Document body = bodyLines(model);
        List<Line> lines = body.lines();
        FooterStats stats = computeStats(model);
        int columnIndexLine = -1;
        int offsetIndexLine = -1;
        int dictionaryLine = -1;
        for (int i = 0; i < lines.size(); i++) {
            String text = renderLine(lines.get(i)).trim();
            if (text.startsWith("Column indexes")) {
                columnIndexLine = i;
            }
            else if (text.startsWith("Offset indexes")) {
                offsetIndexLine = i;
            }
            else if (text.startsWith("With dictionary")) {
                dictionaryLine = i;
            }
        }
        return new FooterBody(body, columnIndexLine, stats.columnIndexCount(),
                offsetIndexLine, stats.offsetIndexCount(),
                dictionaryLine, stats.dictionaryCount());
    }

    private static Document bodyLines(ParquetModel model) {
        FooterStats stats = computeStats(model);
        long fileSize = model.fileSizeBytes();
        long footerTrailerOffset = fileSize - FOOTER_TRAILER_BYTES;

        Document.Builder lines = Document.builder();

        lines.decoration(Line.from(new Span(" File layout ", Theme.accent().bold())));
        lines.row(fact("  File size", Sizes.dualFormat(fileSize)));
        lines.row(fact("  Format version", String.valueOf(model.metadata().version())));
        lines.row(fact("  Created by",
                model.facts().createdBy() != null ? model.facts().createdBy() : "unknown"));
        lines.row(fact("  Footer trailer offset", Fmt.fmt("%,d", footerTrailerOffset)));
        lines.row(fact("  Trailer bytes", String.valueOf(FOOTER_TRAILER_BYTES)));
        if (stats.minDataOffset() < Long.MAX_VALUE) {
            lines.row(fact("  Data region",
                    Fmt.fmt("%,d .. %,d  (%s)",
                            stats.minDataOffset(), stats.maxDataEnd(),
                            Sizes.format(stats.maxDataEnd() - stats.minDataOffset()))));
            lines.row(fact("  Footer + indexes",
                    Sizes.dualFormat(footerAndIndexBytes(model))));
        }

        lines.blank();
        lines.decoration(Line.from(new Span(" Encodings ", Theme.accent().bold())));
        for (Map.Entry<Encoding, Integer> e : stats.encodingHistogram().entrySet()) {
            lines.row(fact("  " + e.getKey().name(),
                    Plurals.format(e.getValue(), "chunk", "chunks")));
        }

        lines.blank();
        lines.decoration(Line.from(new Span(" Codecs ", Theme.accent().bold())));
        for (Map.Entry<CompressionCodec, Integer> e : stats.codecHistogram().entrySet()) {
            int pct = stats.totalChunks() == 0 ? 0
                    : (int) Math.round(100.0 * e.getValue() / stats.totalChunks());
            lines.row(fact("  " + e.getKey().name(),
                    Plurals.format(e.getValue(), "chunk", "chunks") + "  (" + pct + "%)"));
        }

        lines.blank();
        lines.decoration(Line.from(new Span(" Page indexes ", Theme.accent().bold())));
        lines.row(fact("  Column indexes",
                Sizes.dualFormat(stats.columnIndexBytes()) + "  ("
                        + coverage(stats.columnIndexCount(), stats.totalChunks()) + ")"));
        lines.row(fact("  Offset indexes",
                Sizes.dualFormat(stats.offsetIndexBytes()) + "  ("
                        + coverage(stats.offsetIndexCount(), stats.totalChunks()) + ")"));

        lines.blank();
        lines.decoration(Line.from(new Span(" Bloom filters ", Theme.accent().bold())));
        lines.row(fact("  Bloom filters",
                Sizes.dualFormat(stats.bloomFilterBytes()) + "  ("
                        + coverage(stats.bloomFilterCount(), stats.totalChunks()) + ")"));

        lines.blank();
        lines.decoration(Line.from(new Span(" Dictionary ", Theme.accent().bold())));
        lines.row(fact("  With dictionary",
                coverage(stats.dictionaryCount(), stats.totalChunks())));

        lines.blank();
        lines.decoration(Line.from(new Span(" Aggregate ", Theme.accent().bold())));
        lines.row(fact("  Compressed data", Sizes.dualFormat(model.facts().compressedBytes())));
        lines.row(fact("  Uncompressed data", Sizes.dualFormat(model.facts().uncompressedBytes())));
        lines.row(fact("  Compression", Sizes.compression(model.facts().compressedBytes(), model.facts().uncompressedBytes())));
        return lines.build();
    }

    public static String keybarKeys(ScreenState.Footer state, ParquetModel model) {
        FooterBody body = bodyAndAnchors(model);
        return new Keys.Hints()
                .add(true, body.document().hints(Keys.viewportStride()))
                .add(kindAt(cursorLine(state, body), body) != null, "[Enter] open")
                .add(true, "[Esc] back")
                .build();
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
            int bloomFilterCount, long bloomFilterBytes,
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
        int bloomFilterCount = 0;
        long bloomFilterBytes = 0;
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
                if (cmd.bloomFilterLength() != null) {
                    bloomFilterCount++;
                    bloomFilterBytes += cmd.bloomFilterLength();
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
                bloomFilterCount, bloomFilterBytes,
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

    private static String coverage(int count, int total) {
        if (total == 0) {
            return "0/0";
        }
        int pct = (int) Math.round(100.0 * count / total);
        return Fmt.fmt("%,d/%,d chunks  (%d%%)", count, total, pct);
    }

    private static Line fact(String key, String value) {
        return Line.from(
                new Span(" " + padRight(key, 26), Theme.primary()),
                new Span(value, Style.EMPTY));
    }

    private static String padRight(String s, int width) {
        return Strings.padRight(s, width);
    }
}
