/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import dev.hardwood.internal.encoding.DictionaryEncoder;
import dev.hardwood.internal.encoding.PlainEncoder;
import dev.hardwood.metadata.Statistics;

/// [ValueEncoder] for `FLOAT` columns. Values intern by their raw IEEE-754 bit pattern through
/// an `INT32` [DictionaryEncoder] — the 4-byte little-endian `PLAIN` layout of a `FLOAT` and of
/// its `INT32` bits is identical, so the dictionary body is encoded straight from the bits.
final class FloatValueEncoder extends ValueEncoder {

    private final float[] plain;
    private final float[] window;
    private final DictionaryEncoder dictionary; // null when dictionary encoding is disabled
    private final FloatStatisticsCollector statistics = new FloatStatisticsCollector();

    private FloatColumnSource source;
    private int size;
    private int windowBase;
    private int windowLength;

    FloatValueEncoder(int pageValues, boolean enableDictionary) {
        this.plain = new float[pageValues];
        this.window = new float[Math.max(1, pageValues)];
        this.dictionary = enableDictionary ? new DictionaryEncoder() : null;
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
    int intern(int valueIndex, long dictionaryLimitBytes) {
        int bits = Float.floatToRawIntBits(valueAt(valueIndex));
        int index = dictionary.indexOf(bits);
        if (index >= 0) {
            return index;
        }
        if (dictionary.byteSize() + Integer.BYTES > dictionaryLimitBytes) {
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
        return PlainEncoder.encodeInts(dictionary.values(), 0, dictionary.size());
    }

    @Override
    void appendPlain(int slot, int valueIndex) {
        plain[slot] = valueAt(valueIndex);
    }

    @Override
    byte[] encodePlain(int count) {
        return PlainEncoder.encodeFloats(plain, 0, count);
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
