/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.bloomfilter;

import java.nio.ByteBuffer;

/// Membership check for the Parquet split-block bloom filter (SBBF).
///
/// The bitset is an array of 32-byte blocks, each holding eight little-endian 32-bit words. A
/// value's 64-bit hash selects exactly one block (from its high 32 bits) and, within that block,
/// one bit per word (from its low 32 bits). The value is present only if all eight bits are set;
/// any clear bit proves it's absent.
public final class SplitBlockBloomFilter {

    private static final int BYTES_PER_BLOCK = 32;
    private static final int WORDS_PER_BLOCK = 8;

    /// One odd multiplier per word, used to derive eight independent bit positions from the low
    /// 32 bits of the hash. These exact constants are fixed by the Parquet specification — readers
    /// and writers must use the same values — see the `SALT` array in the `mask` function of
    /// <a href="https://github.com/apache/parquet-format/blob/master/BloomFilter.md">BloomFilter.md</a>.
    private static final int[] SALT = {
            0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d,
            0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31
    };

    /// Probes `bitset`, a read-only little-endian view of the filter's bytes. Reads are absolute, so
    /// the buffer's position is neither used nor modified.
    public static boolean mightContain(ByteBuffer bitset, long hash) {
        int numBlocks = bitset.capacity() / BYTES_PER_BLOCK;
        int targetBlock = (int) (((hash >>> 32) * numBlocks) >>> 32);
        int blockOffset = targetBlock * BYTES_PER_BLOCK;

        int key = (int) hash; // low 32 bits drive the per-word bit positions

        // Equivalent to the spec's mask()/block_check(): word i's bit is (key * salt[i]) >>> 27.
        // Computed inline (rather than building a mask block) to avoid a per-call allocation.
        for (int i = 0; i < WORDS_PER_BLOCK; i++) {
            int mask = 1 << ((key * SALT[i]) >>> 27);
            int word = bitset.getInt(blockOffset + i * Integer.BYTES);
            if ((word & mask) == 0) {
                return false;
            }
        }
        return true;
    }
}
