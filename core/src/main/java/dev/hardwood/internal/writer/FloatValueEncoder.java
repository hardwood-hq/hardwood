/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import java.util.Arrays;

import dev.hardwood.internal.bloomfilter.SplitBlockBloomFilterBuilder;
import dev.hardwood.internal.bloomfilter.XxHash64;
import dev.hardwood.internal.encoding.AlpEncoder;
import dev.hardwood.internal.encoding.ByteStreamSplitEncoder;
import dev.hardwood.internal.encoding.DictionaryEncoder;
import dev.hardwood.internal.encoding.PlainEncoder;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.Statistics;
import dev.hardwood.writer.ColumnEncoding;

/// [ValueEncoder] for `FLOAT` columns. Values intern by their raw IEEE-754 bit pattern through
/// an `INT32` [DictionaryEncoder] — the 4-byte little-endian `PLAIN` layout of a `FLOAT` and of
/// its `INT32` bits is identical, so the dictionary body is encoded straight from the bits.
final class FloatValueEncoder extends ValueEncoder {

    private float[] plain;      // this chunk's stored values, grown as the row group fills
    private int plainCount;
    private final float[] window;
    private final DictionaryEncoder dictionary; // null when dictionary encoding is disabled
    private FloatStatisticsCollector statistics = new FloatStatisticsCollector();
    /// The chunk's ALP preset, searched for on its first ALP page from all of its stored values.
    private int[] alpPreset;
    /// Created on the column's first ALP page and kept for the rest, so its buffers are reused.
    private AlpEncoder alp;

    private FloatColumnSource source;
    private int size;
    private int windowBase;
    private int windowLength;

    FloatValueEncoder(boolean buildDictionary, int startingCapacity) {
        this.plain = new float[startingCapacity];
        this.window = new float[windowCapacity(startingCapacity)];
        this.dictionary = buildDictionary ? new DictionaryEncoder() : null;
    }

    @Override
    void reset(ColumnSource source) {
        this.source = (FloatColumnSource) source;
        this.size = source.size();
        this.windowBase = 0;
        this.windowLength = 0;
    }

    private float valueAt(int index) {
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
        int bits = Float.floatToRawIntBits(valueAt(valueIndex));
        int index = dictionary.indexOf(bits);
        return index >= 0 ? index : dictionary.add(bits);
    }

    @Override
    void internRun(int from, int count, int[] indices, int at) {
        for (int i = 0; i < count; i++) {
            int bits = Float.floatToRawIntBits(valueAt(from + i));
            int index = dictionary.indexOf(bits);
            indices[at + i] = index >= 0 ? index : dictionary.add(bits);
        }
    }

    @Override
    void storeRun(int from, int count) {
        if (plainCount + count > plain.length) {
            plain = Arrays.copyOf(plain, grownCapacity(plain.length, plainCount + count));
        }
        // Straight from the source into the store: a run is contiguous in both.
        source.copyInto(from, plain, plainCount, count);
        plainCount += count;
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
        return PlainEncoder.encodeInts(dictionary.values(), 0, dictionary.size());
    }

    @Override
    void insertInto(SplitBlockBloomFilterBuilder filter, boolean fromDictionary) {
        if (fromDictionary) {
            // The dictionary holds each value's raw bits, which is what the filter hashes.
            int[] bits = dictionary.values();
            for (int i = 0; i < dictionary.size(); i++) {
                filter.insert(XxHash64.hash(bits[i]));
            }
        }
        else {
            for (int i = 0; i < plainCount; i++) {
                filter.insert(XxHash64.hash(plain[i]));
            }
        }
    }

    @Override
    void startChunk() {
        statistics = new FloatStatisticsCollector();
        alpPreset = null;
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
        append(Float.intBitsToFloat(dictionary.values()[dictionaryIndex]));
    }

    @Override
    void sketchStored(int from, int to, HyperLogLog sketch) {
        for (int i = from; i < to; i++) {
            sketch.offer(XxHash64.hash(plain[i]));
        }
    }

    @Override
    void reinternStored(int[] indices) {
        for (int i = 0; i < plainCount; i++) {
            int value = Float.floatToRawIntBits(plain[i]);
            int index = dictionary.indexOf(value);
            indices[i] = index >= 0 ? index : dictionary.add(value);
        }
        plainCount = 0;
    }

    @Override
    long dictionaryPlainBytes() {
        return (long) dictionary.size() * Integer.BYTES;
    }

    @Override
    void dropDictionary() {
        dictionary.clear();
    }

    private void append(float value) {
        if (plainCount == plain.length) {
            plain = Arrays.copyOf(plain, grownCapacity(plain.length));
        }
        plain[plainCount++] = value;
    }

    @Override
    void encodeInto(ByteArrayBuilder out, ColumnEncoding encoding, int from, int count) {
        switch (encoding) {
            case PLAIN -> {
                int at = out.reserve(PlainEncoder.fixedWidthLength(count, Float.BYTES));
                PlainEncoder.encodeFloats(plain, from, count, out.array(), at);
            }
            case BYTE_STREAM_SPLIT -> {
                int at = out.reserve(PlainEncoder.fixedWidthLength(count, Float.BYTES));
                ByteStreamSplitEncoder.splitFloats(plain, from, count, out.array(), at);
            }
            case ALP -> {
                if (alpPreset == null) {
                    alpPreset = AlpEncoder.presetFloats(plain, 0, plainCount);
                }
                if (alp == null) {
                    alp = new AlpEncoder(Float.BYTES);
                }
                int length = alp.encodeFloats(plain, from, count, alpPreset);
                out.write(alp.buffer(), 0, length);
            }
            default -> throw unsupported(encoding, PhysicalType.FLOAT);
        }
    }

    @Override
    PageBounds pageStatistics(int[] indices, int valueFrom, int valueCount, long nullCount) {
        FloatStatisticsCollector page = new FloatStatisticsCollector();
        int end = valueFrom + valueCount;
        if (indices != null) {
            int[] entries = dictionary.values();
            for (int i = valueFrom; i < end; i++) {
                page.accept(Float.intBitsToFloat(entries[indices[i]]));
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
        FloatStatisticsCollector page = new FloatStatisticsCollector();
        int[] values = dictionary.values();
        for (int k = 0; k < distinct; k++) {
            page.accept(Float.intBitsToFloat(values[entries[k]]), occurrences[k]);
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
        return Float.SIZE;
    }

    @Override
    long plainValueBits(long presentValues) {
        return presentValues * Float.SIZE;
    }

    @Override
    long retainedBytes() {
        return (long) plainCount * Float.BYTES + (dictionary == null ? 0 : dictionary.retainedBytes());
    }

    @Override
    long maxRetainedBytesPerValue() {
        return Math.max(Float.BYTES, INDEX_BYTES + INT_DICTIONARY_BYTES_PER_ENTRY);
    }
}
