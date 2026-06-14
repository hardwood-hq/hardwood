/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.bloomfilter;

import java.nio.ByteBuffer;

/// A column chunk's bloom filter: the parsed [BloomFilterHeader] together with the raw split-block
/// `bitset` that follows it (`numBytes` of uncompressed filter data).
///
/// The `bitset` is a read-only, little-endian [ByteBuffer] view that shares storage with the bytes
/// fetched from the file — for a memory-mapped input it is not copied. Probe it only through
/// absolute reads (as [SplitBlockBloomFilter] does); do not rely on or mutate its position.
public record BloomFilter(
        BloomFilterHeader header,
        ByteBuffer bitset
) {

    /// Returns whether a value with the given 64-bit hash might be present. A `false` result is
    /// definitive (the value was never added); a `true` result is probabilistic (it may be a false
    /// positive). The hash must be the XXH64 of the value's plain encoding — see [XxHash64].
    public boolean mightContain(long hash) {
        return SplitBlockBloomFilter.mightContain(bitset, hash);
    }
}
