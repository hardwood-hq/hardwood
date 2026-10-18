/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.Arrays;

/// Word-shape bitmap helpers shared by the validity producers
/// ([FlatFill], [NestedLevelComputer]). Set-bit-= -present polarity,
/// matching [BatchExchange.Batch#validity].
final class BitmapWords {

    private BitmapWords() {
    }

    /// Sets bits `[fromInclusive, toExclusive)` in `words`. Equivalent to
    /// `BitSet.set(int, int)`.
    static void setRange(long[] words, int fromInclusive, int toExclusive) {
        if (fromInclusive >= toExclusive) {
            return;
        }
        int firstWord = fromInclusive >>> 6;
        int lastWord = (toExclusive - 1) >>> 6;
        long firstMask = ~0L << fromInclusive;
        long lastMask = ~0L >>> -toExclusive;
        if (firstWord == lastWord) {
            words[firstWord] |= firstMask & lastMask;
            return;
        }
        words[firstWord] |= firstMask;
        Arrays.fill(words, firstWord + 1, lastWord, ~0L);
        words[lastWord] |= lastMask;
    }

    /// Writes the mask a matcher would produce when every record matches: all-ones for
    /// `[0, recordCount)` bits, tail bits of the last active word zeroed, words beyond the active
    /// range untouched.
    static void fillAllOnes(long[] words, int recordCount) {
        int fullWords = recordCount >>> 6;
        int tail = recordCount & 63;
        Arrays.fill(words, 0, fullWords, -1L);
        if (tail != 0) {
            words[fullWords] = -1L >>> (64 - tail);
        }
    }
}
