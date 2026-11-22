/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.internal;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;

/// Footer-wide aggregates over a file's row groups: the data region, the
/// encoding and codec histograms, page-index / bloom-filter / dictionary
/// coverage, and the aggregate byte sizes. Computed from the footer metadata
/// alone, so the dive Footer screen and the `hardwood footer` command report
/// the same figures instead of each deriving them.
public record FooterStats(
        long fileSize,
        long minDataOffset,
        long maxDataEnd,
        int totalChunks,
        int columnIndexCount,
        long columnIndexBytes,
        int offsetIndexCount,
        long offsetIndexBytes,
        int bloomFilterCount,
        long bloomFilterBytes,
        int dictionaryCount,
        Map<Encoding, Integer> encodingHistogram,
        Map<CompressionCodec, Integer> codecHistogram,
        long compressedBytes,
        long uncompressedBytes) {

    /// Total bytes past the data region: the footer thrift, the page indexes
    /// and the trailer — everything the reader did not decode as data.
    public long footerAndIndexBytes() {
        if (maxDataEnd == 0) {
            return fileSize;
        }
        return Math.max(0, fileSize - maxDataEnd);
    }

    /// Bytes in the data region, from the first chunk's start to the last
    /// chunk's end. Zero when the file holds no chunks.
    public long dataRegionBytes() {
        if (minDataOffset == Long.MAX_VALUE || maxDataEnd == 0) {
            return 0;
        }
        return maxDataEnd - minDataOffset;
    }

    /// Coverage as `count/total chunks (pct%)`, the form the dive Footer
    /// screen prints.
    public static String coverage(int count, int total) {
        if (total == 0) {
            return "0/0";
        }
        int pct = (int) Math.round(100.0 * count / total);
        return Fmt.fmt("%,d/%,d chunks  (%d%%)", count, total, pct);
    }

    public static FooterStats of(FileMetaData metadata, long fileSize) {
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
        long compressedBytes = 0;
        long uncompressedBytes = 0;
        Map<Encoding, Integer> encodingHistogram = new TreeMap<>();
        Map<CompressionCodec, Integer> codecHistogram = new TreeMap<>();
        for (RowGroup rg : metadata.rowGroups()) {
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
                compressedBytes += cmd.totalCompressedSize();
                uncompressedBytes += cmd.totalUncompressedSize();
            }
        }
        return new FooterStats(fileSize, minDataOffset, maxDataEnd, totalChunks,
                columnIndexCount, columnIndexBytes,
                offsetIndexCount, offsetIndexBytes,
                bloomFilterCount, bloomFilterBytes,
                dictionaryCount,
                sortedByCount(encodingHistogram),
                sortedByCount(codecHistogram),
                compressedBytes, uncompressedBytes);
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
}
