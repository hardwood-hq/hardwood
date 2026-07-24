/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import dev.hardwood.internal.encoding.PlainEncoder;
import dev.hardwood.metadata.Statistics;

/// [ValueEncoder] for `BOOLEAN` columns. Booleans are never dictionary-encoded — a two-value
/// dictionary cannot beat the bit-packed `PLAIN` layout — so every page is `PLAIN`.
final class BooleanValueEncoder extends ValueEncoder {

    private final boolean[] plain;
    private final boolean[] window;
    private final BooleanStatisticsCollector statistics = new BooleanStatisticsCollector();

    private BooleanColumnSource source;
    private int size;
    private int windowBase;
    private int windowLength;

    BooleanValueEncoder(int pageValues) {
        this.plain = new boolean[pageValues];
        this.window = new boolean[Math.max(1, pageValues)];
    }

    @Override
    void reset(ColumnSource source) {
        this.source = (BooleanColumnSource) source;
        this.size = source.size();
        this.windowBase = 0;
        this.windowLength = 0;
    }

    private boolean valueAt(int index) {
        if (index >= windowBase + windowLength) {
            windowBase = index;
            windowLength = Math.min(window.length, size - index);
            source.copyInto(windowBase, window, 0, windowLength);
        }
        return window[index - windowBase];
    }

    @Override
    boolean dictionaryCapable() {
        return false;
    }

    @Override
    int intern(int valueIndex, long dictionaryLimitBytes) {
        throw new UnsupportedOperationException("BOOLEAN columns are never dictionary-encoded");
    }

    @Override
    int dictionarySize() {
        return 0;
    }

    @Override
    byte[] encodeDictionaryBody() {
        throw new UnsupportedOperationException("BOOLEAN columns are never dictionary-encoded");
    }

    @Override
    void appendPlain(int slot, int valueIndex) {
        plain[slot] = valueAt(valueIndex);
    }

    @Override
    byte[] encodePlain(int count) {
        return PlainEncoder.encodeBooleans(plain, 0, count);
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
