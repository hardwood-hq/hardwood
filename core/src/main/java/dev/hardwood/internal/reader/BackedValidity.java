/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import dev.hardwood.reader.Validity;

/// [Validity] whose per-item nullability is stored in a packed `long[]`
/// (set bit = item is present). Constructed by [Validity#of] when at least
/// one item is null in the batch. The wrapper does not copy — callers must
/// not mutate the bitmap after handing it in.
public final class BackedValidity implements Validity {

    private final long[] words;

    public BackedValidity(long[] words) {
        this.words = words;
    }

    @Override
    public boolean hasNulls() {
        return true;
    }

    @Override
    public boolean isNull(int i) {
        return (words[i >>> 6] & (1L << i)) == 0L;
    }

    @Override
    public boolean isNotNull(int i) {
        return (words[i >>> 6] & (1L << i)) != 0L;
    }

    @Override
    public int nullCount(int count) {
        int fullWords = count >>> 6;
        int total = 0;
        for (int w = 0; w < fullWords; w++) {
            total += Long.bitCount(~words[w]);
        }
        int tail = count & 63;
        if (tail != 0) {
            long mask = (1L << tail) - 1L;
            total += Long.bitCount(~words[fullWords] & mask);
        }
        return total;
    }

    @Override
    public int nextNull(int from, int count) {
        if (from >= count) return -1;
        int wordIdx = from >>> 6;
        int endWord = (count - 1) >>> 6;
        int tail = count & 63;
        long endMask = tail == 0 ? ~0L : (1L << tail) - 1L;
        long word = ~words[wordIdx] & (~0L << from);
        if (wordIdx == endWord) word &= endMask;
        while (true) {
            if (word != 0L) {
                return (wordIdx << 6) + Long.numberOfTrailingZeros(word);
            }
            if (++wordIdx > endWord) return -1;
            word = ~words[wordIdx];
            if (wordIdx == endWord) word &= endMask;
        }
    }

    @Override
    public int nextNotNull(int from, int count) {
        if (from >= count) return -1;
        int wordIdx = from >>> 6;
        int endWord = (count - 1) >>> 6;
        int tail = count & 63;
        long endMask = tail == 0 ? ~0L : (1L << tail) - 1L;
        long word = words[wordIdx] & (~0L << from);
        if (wordIdx == endWord) word &= endMask;
        while (true) {
            if (word != 0L) {
                return (wordIdx << 6) + Long.numberOfTrailingZeros(word);
            }
            if (++wordIdx > endWord) return -1;
            word = words[wordIdx];
            if (wordIdx == endWord) word &= endMask;
        }
    }

    @Override
    public long[] words() {
        return words;
    }
}
