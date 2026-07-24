/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import dev.hardwood.internal.encoding.LongDictionaryEncoder;
import dev.hardwood.internal.encoding.PlainEncoder;
import dev.hardwood.metadata.Statistics;

/// [ValueEncoder] for `DOUBLE` columns. Values intern by their raw IEEE-754 bit pattern through
/// an `INT64` [LongDictionaryEncoder] — the 8-byte little-endian `PLAIN` layout of a `DOUBLE`
/// and of its `INT64` bits is identical, so the dictionary body is encoded straight from the
/// bits.
final class DoubleValueEncoder extends ValueEncoder {

    private final double[] plain;
    private final double[] window;
    private final LongDictionaryEncoder dictionary; // null when dictionary encoding is disabled
    private final DoubleStatisticsCollector statistics = new DoubleStatisticsCollector();

    private DoubleColumnSource source;
    private int size;
    private int windowBase;
    private int windowLength;

    DoubleValueEncoder(int pageValues, boolean enableDictionary) {
        this.plain = new double[pageValues];
        this.window = new double[Math.max(1, pageValues)];
        this.dictionary = enableDictionary ? new LongDictionaryEncoder() : null;
    }

    @Override
    void reset(ColumnSource source) {
        this.source = (DoubleColumnSource) source;
        this.size = source.size();
        this.windowBase = 0;
        this.windowLength = 0;
    }

    private double valueAt(int index) {
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
    int intern(int valueIndex, long dictionaryLimitBytes) {
        long bits = Double.doubleToRawLongBits(valueAt(valueIndex));
        int index = dictionary.indexOf(bits);
        if (index >= 0) {
            return index;
        }
        if (dictionary.byteSize() + Long.BYTES > dictionaryLimitBytes) {
            return DICTIONARY_OVERFLOW;
        }
        return dictionary.add(bits);
    }

    @Override
    int dictionarySize() {
        return dictionary.size();
    }

    @Override
    byte[] encodeDictionaryBody() {
        return PlainEncoder.encodeLongs(dictionary.values(), 0, dictionary.size());
    }

    @Override
    void appendPlain(int slot, int valueIndex) {
        plain[slot] = valueAt(valueIndex);
    }

    @Override
    byte[] encodePlain(int count) {
        return PlainEncoder.encodeDoubles(plain, 0, count);
    }

    @Override
    void stat(int valueIndex) {
        statistics.accept(valueAt(valueIndex));
    }

    @Override
    void statNull() {
        statistics.acceptNull();
    }

    @Override
    Statistics statistics() {
        return statistics.toStatistics();
    }
}
