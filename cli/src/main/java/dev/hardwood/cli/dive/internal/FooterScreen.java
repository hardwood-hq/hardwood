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
        return false;
    }

    public static void render(Buffer buffer, Rect area, ParquetModel model) {
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
        return count + "/" + total + " chunks  (" + pct + "%)";
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
