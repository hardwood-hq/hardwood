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

/// [ValueEncoder] for `INT32` columns.
final class IntValueEncoder extends ValueEncoder {

    private final int[] plain;
    private final int[] window;
    private final DictionaryEncoder dictionary; // null when dictionary encoding is disabled
    private final IntStatisticsCollector statistics = new IntStatisticsCollector();

    private IntColumnSource source;
    private int size;
    private int windowBase;
    private int windowLength;

    IntValueEncoder(int pageValues, boolean enableDictionary) {
        this.plain = new int[pageValues];
        this.window = new int[Math.max(1, pageValues)];
        this.dictionary = enableDictionary ? new DictionaryEncoder() : null;
    }

    @Override
    void reset(ColumnSource source) {
        this.source = (IntColumnSource) source;
        this.size = source.size();
        this.windowBase = 0;
        this.windowLength = 0;
    }

    private int valueAt(int index) {
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
        int value = valueAt(valueIndex);
        int index = dictionary.indexOf(value);
        if (index >= 0) {
            return index;
        }
        if (dictionary.byteSize() + Integer.BYTES > dictionaryLimitBytes) {
            return DICTIONARY_OVERFLOW;
        }
        return dictionary.add(value);
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
        return PlainEncoder.encodeInts(plain, 0, count);
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
