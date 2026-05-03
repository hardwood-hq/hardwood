/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate.matcher.ints;

import java.util.Arrays;
import java.util.BitSet;

import dev.hardwood.internal.predicate.IntBatchMatcher;
import dev.hardwood.internal.reader.BatchExchange;

/// IN-list matcher for `int` columns. Sorts the values once at construction and
/// uses linear scan below [#BINARY_SEARCH_THRESHOLD] entries, [Arrays#binarySearch]
/// at or above. Mirrors the dispatch threshold used in [RecordFilterCompiler].
public final class IntInBatchMatcher implements IntBatchMatcher {

    private static final int BINARY_SEARCH_THRESHOLD = 16;

    private final int columnIndex;
    private final int[] sorted;
    private final boolean useBinarySearch;

    public IntInBatchMatcher(int columnIndex, int[] values) {
        this.columnIndex = columnIndex;
        int[] copy = values.clone();
        Arrays.sort(copy);
        this.sorted = copy;
        this.useBinarySearch = copy.length >= BINARY_SEARCH_THRESHOLD;
    }

    @Override
    public int columnIndex() {
        return columnIndex;
    }

    @Override
    public void test(BatchExchange.Batch batch, long[] outWords) {
        int[] vals = (int[]) batch.values;
        BitSet nulls = batch.nulls;
        int n = batch.recordCount;
        int[] s = sorted;
        int fullWords = n >>> 6;
        int tail = n & 63;

        if (useBinarySearch) {
            if (nulls == null) {
                for (int w = 0; w < fullWords; w++) {
                    int base = w << 6;
                    long word = 0L;
                    for (int b = 0; b < 64; b++) {
                        word |= ((Arrays.binarySearch(s, vals[base + b]) >= 0) ? 1L : 0L) << b;
                    }
                    outWords[w] = word;
                }
                if (tail != 0) {
                    int base = fullWords << 6;
                    long word = 0L;
                    for (int b = 0; b < tail; b++) {
                        word |= ((Arrays.binarySearch(s, vals[base + b]) >= 0) ? 1L : 0L) << b;
                    }
                    outWords[fullWords] = word;
                }
            }
            else {
                for (int w = 0; w < fullWords; w++) {
                    int base = w << 6;
                    long word = 0L;
                    for (int b = 0; b < 64; b++) {
                        int i = base + b;
                        word |= ((!nulls.get(i) && Arrays.binarySearch(s, vals[i]) >= 0) ? 1L : 0L) << b;
                    }
                    outWords[w] = word;
                }
                if (tail != 0) {
                    int base = fullWords << 6;
                    long word = 0L;
                    for (int b = 0; b < tail; b++) {
                        int i = base + b;
                        word |= ((!nulls.get(i) && Arrays.binarySearch(s, vals[i]) >= 0) ? 1L : 0L) << b;
                    }
                    outWords[fullWords] = word;
                }
            }
        }
        else {
            int len = s.length;
            if (nulls == null) {
                for (int w = 0; w < fullWords; w++) {
                    int base = w << 6;
                    long word = 0L;
                    for (int b = 0; b < 64; b++) {
                        int v = vals[base + b];
                        long hit = 0L;
                        for (int k = 0; k < len; k++) {
                            if (s[k] == v) { hit = 1L; break; }
                        }
                        word |= hit << b;
                    }
                    outWords[w] = word;
                }
                if (tail != 0) {
                    int base = fullWords << 6;
                    long word = 0L;
                    for (int b = 0; b < tail; b++) {
                        int v = vals[base + b];
                        long hit = 0L;
                        for (int k = 0; k < len; k++) {
                            if (s[k] == v) { hit = 1L; break; }
                        }
                        word |= hit << b;
                    }
                    outWords[fullWords] = word;
                }
            }
            else {
                for (int w = 0; w < fullWords; w++) {
                    int base = w << 6;
                    long word = 0L;
                    for (int b = 0; b < 64; b++) {
                        int i = base + b;
                        long hit = 0L;
                        if (!nulls.get(i)) {
                            int v = vals[i];
                            for (int k = 0; k < len; k++) {
                                if (s[k] == v) { hit = 1L; break; }
                            }
                        }
                        word |= hit << b;
                    }
                    outWords[w] = word;
                }
                if (tail != 0) {
                    int base = fullWords << 6;
                    long word = 0L;
                    for (int b = 0; b < tail; b++) {
                        int i = base + b;
                        long hit = 0L;
                        if (!nulls.get(i)) {
                            int v = vals[i];
                            for (int k = 0; k < len; k++) {
                                if (s[k] == v) { hit = 1L; break; }
                            }
                        }
                        word |= hit << b;
                    }
                    outWords[fullWords] = word;
                }
            }
        }

        int firstTrailing = (tail == 0) ? fullWords : fullWords + 1;
        for (int w = firstTrailing; w < outWords.length; w++) {
            outWords[w] = 0L;
        }
    }
}
