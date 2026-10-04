/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.bloomfilter;

import static dev.hardwood.internal.bloomfilter.SplitBlockBloomFilter.BYTES_PER_BLOCK;
import static dev.hardwood.internal.bloomfilter.SplitBlockBloomFilter.SALT;
import static dev.hardwood.internal.bloomfilter.SplitBlockBloomFilter.WORDS_PER_BLOCK;

/// Builds a split-block Bloom filter, the write-side counterpart of [SplitBlockBloomFilter].
///
/// The filter is sized for an expected number of distinct values and a false-positive
/// probability, filled through [#insert], then shrunk by [#foldToTargetFpp] to the fewest blocks
/// that still meet the probability on the bits actually set. Folding is exact: with a power-of-two
/// block count, the block a hash selects among `n / 2` blocks is the one it selects among `n`,
/// halved, so OR-ing each pair of adjacent blocks yields the filter those values would have
/// filled at half the size.
public final class SplitBlockBloomFilterBuilder {

    /// The smallest filter: one block.
    static final int MIN_BYTES = BYTES_PER_BLOCK;

    /// The largest filter built, 128 MiB, the cap parquet-java, Arrow C++ and arrow-rs apply.
    static final int MAX_BYTES = 128 << 20;

    private final int[] words;
    private int numBlocks;

    private SplitBlockBloomFilterBuilder(int numBytes) {
        this.numBlocks = numBytes / BYTES_PER_BLOCK;
        this.words = new int[numBlocks * WORDS_PER_BLOCK];
    }

    /// A filter sized for `distinctValues` at false-positive probability `fpp`.
    ///
    /// @param distinctValues the distinct values expected; an upper bound is fine, since
    ///        [#foldToTargetFpp] gives back what it overstates
    /// @param fpp the target false-positive probability, in `(0, 1)`
    public static SplitBlockBloomFilterBuilder forDistinctValues(long distinctValues, double fpp) {
        return new SplitBlockBloomFilterBuilder(optimalNumBytes(distinctValues, fpp));
    }

    /// The bytes a filter needs to hold `distinctValues` at `fpp`, rounded up to a power of two
    /// and clamped to `[MIN_BYTES, MAX_BYTES]`.
    ///
    /// Eight bits are set per value, one per word of its block, so the filter needs
    /// `-8 n / ln(1 - fpp^(1/8))` bits, the formula the Parquet specification gives.
    static int optimalNumBytes(long distinctValues, double fpp) {
        if (!(fpp > 0 && fpp < 1)) {
            throw new IllegalArgumentException("fpp must be in (0, 1) but was " + fpp);
        }
        if (distinctValues < 0) {
            throw new IllegalArgumentException("distinctValues must not be negative but was " + distinctValues);
        }
        double bits = -WORDS_PER_BLOCK * (double) distinctValues / Math.log1p(-Math.pow(fpp, 1.0 / WORDS_PER_BLOCK));
        double bytes = bits / Byte.SIZE;
        if (bytes >= MAX_BYTES) {
            return MAX_BYTES;
        }
        int atLeast = Math.max(MIN_BYTES, (int) Math.ceil(bytes));
        // A power of two, not merely a multiple of the block size: Arrow C++ refuses to load a
        // bitset of any other size.
        int power = Integer.highestOneBit(atLeast);
        return power == atLeast ? power : power << 1;
    }

    /// Adds the value whose XXH64 hash is `hash` (see [XxHash64]).
    public void insert(long hash) {
        int block = (int) (((hash >>> 32) * numBlocks) >>> 32);
        int key = (int) hash;
        int base = block * WORDS_PER_BLOCK;
        for (int i = 0; i < WORDS_PER_BLOCK; i++) {
            words[base + i] |= 1 << ((key * SALT[i]) >>> 27);
        }
    }

    /// Halves the filter for as long as the halved filter's false-positive probability, estimated
    /// from the bits it would hold, stays at or below `fpp`.
    public void foldToTargetFpp(double fpp) {
        while (numBlocks > 1 && foldedFpp() <= fpp) {
            fold();
        }
    }

    /// The probability that a value absent from the filter passes it once halved: the mean, over
    /// the halved filter's blocks, of the chance that all eight of its probe bits are set, which
    /// is the product of each word's fraction of set bits.
    private double foldedFpp() {
        double sum = 0;
        for (int block = 0; block < numBlocks; block += 2) {
            int left = block * WORDS_PER_BLOCK;
            int right = left + WORDS_PER_BLOCK;
            double product = 1;
            for (int i = 0; i < WORDS_PER_BLOCK; i++) {
                product *= Integer.bitCount(words[left + i] | words[right + i]) / (double) Integer.SIZE;
            }
            sum += product;
        }
        return sum / (numBlocks / 2);
    }

    /// Halves the filter in place: block `j` becomes blocks `2j` and `2j + 1` OR-ed. Ascending
    /// order is safe, since block `j` is read, as half of block `j / 2`, before it is overwritten.
    /// The array keeps its length; only the first [#numBlocks] blocks are the filter.
    private void fold() {
        int halved = numBlocks / 2;
        for (int block = 0; block < halved; block++) {
            int left = 2 * block * WORDS_PER_BLOCK;
            int right = left + WORDS_PER_BLOCK;
            int target = block * WORDS_PER_BLOCK;
            for (int i = 0; i < WORDS_PER_BLOCK; i++) {
                words[target + i] = words[left + i] | words[right + i];
            }
        }
        numBlocks = halved;
    }

    /// The filter's size in bytes.
    public int numBytes() {
        return numBlocks * BYTES_PER_BLOCK;
    }

    /// The bitset as the file stores it: each block's eight words little-endian, blocks in order.
    public byte[] toBytes() {
        byte[] bytes = new byte[numBytes()];
        for (int w = 0; w < numBlocks * WORDS_PER_BLOCK; w++) {
            int word = words[w];
            int at = w * Integer.BYTES;
            bytes[at] = (byte) word;
            bytes[at + 1] = (byte) (word >>> 8);
            bytes[at + 2] = (byte) (word >>> 16);
            bytes[at + 3] = (byte) (word >>> 24);
        }
        return bytes;
    }
}
