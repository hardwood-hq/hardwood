/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.bloomfilter;

/// Parsed `BloomFilterHeader` thrift struct that precedes a column chunk's bloom filter.
public record BloomFilterHeader(
        int numBytes,
        Algorithm algorithm,
        Hash hash,
        Compression compression
) {
    public enum Algorithm {
        BLOCK;

        public static Algorithm fromVariant(short variant) {
            if (variant == 1) {
                return BLOCK;
            }
            throw new IllegalArgumentException("Unknown BloomFilterHeader.Algorithm variant: " + variant);
        }
    }

    public enum Hash {
        XXHASH;

        public static Hash fromVariant(short variant) {
            if (variant == 1) {
                return XXHASH;
            }
            throw new IllegalArgumentException("Unknown BloomFilterHeader.Hash variant: " + variant);
        }
    }

    public enum Compression {
        UNCOMPRESSED;

        public static Compression fromVariant(short variant) {
            if (variant == 1) {
                return UNCOMPRESSED;
            }
            throw new IllegalArgumentException("Unknown BloomFilterHeader.Compression variant: " + variant);
        }
    }
}
