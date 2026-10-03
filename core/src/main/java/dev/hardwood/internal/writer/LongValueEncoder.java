/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import java.util.Arrays;

import dev.hardwood.internal.encoding.ByteStreamSplitEncoder;
import dev.hardwood.internal.encoding.DeltaBinaryPackedEncoder;
import dev.hardwood.internal.encoding.LongDictionaryEncoder;
import dev.hardwood.internal.encoding.PlainEncoder;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.writer.ColumnEncoding;

/// [ValueEncoder] for `INT64` columns.
final class LongValueEncoder extends ValueEncoder {

    private long[] plain;      // this chunk's stored values, grown as the row group fills
    private int plainCount;
    private final long[] window;
    private final LongDictionaryEncoder dictionary; // null when dictionary encoding is disabled
    private final boolean unsignedOrder;
    private LongStatisticsCollector statistics;

    private LongColumnSource source;
    private int size;
    private int windowBase;
    private int windowLength;

    LongValueEncoder(boolean buildDictionary, boolean unsignedOrder, int startingCapacity) {
        this.unsignedOrder = unsignedOrder;
        this.statistics = new LongStatisticsCollector(unsignedOrder);
        this.plain = new long[startingCapacity];
        this.window = new long[windowCapacity(startingCapacity)];
        this.dictionary = buildDictionary ? new LongDictionaryEncoder() : null;
    }

    @Override
    void reset(ColumnSource source) {
        this.source = (LongColumnSource) source;
        this.size = source.size();
        this.windowBase = 0;
        this.windowLength = 0;
    }

    private long valueAt(int index) {
        if (index >= windowBase + windowLength) {
            windowBase = index;
            windowLength = Math.min(window.length, size - index);
            source.copyInto(windowBase, window, 0, windowLength);
        }
        return window[index - windowBase];
    }

    @Override
    boolean dictionaryCapable() {
        return dictionary != null;
    }

    @Override
    int intern(int valueIndex) {
        long value = valueAt(valueIndex);
        int index = dictionary.indexOf(value);
        return index >= 0 ? index : dictionary.add(value);
    }

    @Override
    int dictionarySize() {
        return dictionary.size();
    }

    @Override
    long exactDistinctCount() {
        return dictionary != null ? dictionary.size() : UNKNOWN_DISTINCT_COUNT;
    }

    @Override
    byte[] encodeDictionaryBody() {
        return PlainEncoder.encodeLongs(dictionary.values(), 0, dictionary.size());
    }

    @Override
    void startChunk() {
        statistics = new LongStatisticsCollector(unsignedOrder);
        if (dictionary != null) {
            dictionary.clear();
        }
        plainCount = 0;
    }

    @Override
    void store(int valueIndex) {
        append(valueAt(valueIndex));
    }

    @Override
    void storeDictionaryValue(int dictionaryIndex) {
        append(dictionary.values()[dictionaryIndex]);
    }

    @Override
    long dictionaryPlainBytes() {
        return (long) dictionary.size() * Long.BYTES;
    }

    @Override
    void dropDictionary() {
        dictionary.clear();
    }

    private void append(long value) {
        if (plainCount == plain.length) {
            plain = Arrays.copyOf(plain, grownCapacity(plain.length));
        }
        plain[plainCount++] = value;
    }

    @Override
    void encodeInto(ByteArrayBuilder out, ColumnEncoding encoding, int from, int count) {
        switch (encoding) {
            case PLAIN -> {
                int at = out.reserve(PlainEncoder.fixedWidthLength(count, Long.BYTES));
                PlainEncoder.encodeLongs(plain, from, count, out.array(), at);
            }
            case BYTE_STREAM_SPLIT -> {
                int at = out.reserve(PlainEncoder.fixedWidthLength(count, Long.BYTES));
                ByteStreamSplitEncoder.splitLongs(plain, from, count, out.array(), at);
            }
            case DELTA_BINARY_PACKED -> out.writeBytes(DeltaBinaryPackedEncoder.encodeLongs(plain, from, count));
            default -> throw unsupported(encoding, PhysicalType.INT64);
        }
    }

    @Override
    PageBounds pageStatistics(int[] indices, int valueFrom, int valueCount, long nullCount) {
        LongStatisticsCollector page = new LongStatisticsCollector(unsignedOrder);
        int end = valueFrom + valueCount;
        if (indices != null) {
            long[] entries = dictionary.values();
            for (int i = valueFrom; i < end; i++) {
                page.accept(entries[indices[i]]);
            }
        }
        else {
            for (int i = valueFrom; i < end; i++) {
                page.accept(plain[i]);
            }
        }
        return PageBounds.finish(statistics, page, valueCount, nullCount);
    }

    @Override
    PageBounds dictionaryPageStatistics(int[] entries, int[] occurrences, int distinct, int valueCount,
                                        long nullCount) {
        LongStatisticsCollector page = new LongStatisticsCollector(unsignedOrder);
        long[] values = dictionary.values();
        for (int k = 0; k < distinct; k++) {
            page.accept(values[entries[k]]);
        }
        return PageBounds.finish(statistics, page, valueCount, nullCount);
    }

    @Override
    int compareBounds(byte[] left, byte[] right) {
        return statistics.compareBounds(left, right);
    }

    @Override
    Statistics statistics() {
        return statistics.toStatistics();
    }

    @Override
    long uniformValueBits() {
        return Long.SIZE;
    }

    @Override
    long plainValueBits(long presentValues) {
        return presentValues * Long.SIZE;
    }

    @Override
    long retainedBytes() {
        return (long) plainCount * Long.BYTES + (dictionary == null ? 0 : dictionary.retainedBytes());
    }

    @Override
    long maxRetainedBytesPerValue() {
        return Math.max(Long.BYTES, INDEX_BYTES + LONG_DICTIONARY_BYTES_PER_ENTRY);
    }
}
