/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.io.IOException;
import java.nio.ByteBuffer;

import dev.hardwood.internal.bloomfilter.BloomFilter;
import dev.hardwood.internal.bloomfilter.BloomFilterHeader;

/// Reader for a column chunk's bloom filter: the `BloomFilterHeader` thrift struct followed
/// immediately by `numBytes` of raw split-block bitset bytes.
///
/// The caller is responsible for positioning `reader` at the start of the filter (i.e. over the
/// bytes fetched from `bloom_filter_offset`); this reader does no file I/O of its own.
public class BloomFilterReader {

    public static BloomFilter read(ThriftCompactReader reader) throws IOException {
        BloomFilterHeader header = BloomFilterHeaderReader.read(reader);
        int numBytes = header.numBytes();
        // A split-block bitset is an array of 32-byte blocks, so its size must be a positive
        // multiple of 32; anything else would mis-shape the block math during probing.
        if (numBytes == 0 || numBytes % 32 != 0) {
            throw new IllegalStateException(
                    "Malformed bloom filter: bitset size must be a positive multiple of 32 bytes but was "
                            + numBytes);
        }
        if (numBytes > reader.remaining()) {
            throw new IllegalStateException("Malformed bloom filter: header declares " + numBytes
                    + " bitset bytes but only " + reader.remaining() + " remain");
        }
        ByteBuffer bitset = reader.readSlice(numBytes);
        return new BloomFilter(header, bitset);
    }
}
