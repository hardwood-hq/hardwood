/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.bloomfilter;

/// XXH64 hash, as used by the Parquet split-block bloom filter to turn a value's plain-encoded
/// bytes into the 64-bit hash that [SplitBlockBloomFilter] probes. The seed is fixed at 0, the
/// value Parquet specified for bloom filters. All arithmetic is unsigned 64-bit (Java `long`
/// two's-complement wraparound is bit-identical); multibyte reads are little-endian.
public final class XxHash64 {

    /// The five 64-bit prime constants `PRIME64_1`..`PRIME64_5` defined by the XXH64 algorithm.
    /// Fixed by the specification — every conformant implementation uses these exact values — see
    /// <a href="https://github.com/Cyan4973/xxHash/blob/dev/doc/xxhash_spec.md">the xxHash spec</a>.
    private static final long PRIME1 = 0x9E3779B185EBCA87L;
    private static final long PRIME2 = 0xC2B2AE3D27D4EB4FL;
    private static final long PRIME3 = 0x165667B19E3779F9L;
    private static final long PRIME4 = 0x85EBCA77C2B2AE63L;
    private static final long PRIME5 = 0x27D4EB2F165667C5L;

    /// The XXH64 seed. The Parquet specification fixes the bloom filter hash seed to 0, so the
    /// accumulators and short-input start derive from a zero seed — see the hash strategy section
    /// of <a href="https://github.com/apache/parquet-format/blob/master/BloomFilter.md">BloomFilter.md</a>.
    private static final long SEED = 0;

    private XxHash64() {
    }

    /// Hashes the whole array.
    public static long hash(byte[] data) {
        return hash(data, 0, data.length);
    }

    /// Hashes `length` bytes of `data` starting at `offset`.
    public static long hash(byte[] data, int offset, int length) {
        int p = offset;
        int end = offset + length;
        long h64;

        if (length >= 32) {
            // Four independent accumulator lanes, each seeded differently, so they diverge.
            // Written as `SEED + ...` per the XXH64 spec; the additions overflow 64 bits on
            // purpose — unsigned wraparound is part of the algorithm, not a defect.
            long v1 = SEED + PRIME1 + PRIME2;
            long v2 = SEED + PRIME2;
            long v3 = SEED;
            long v4 = SEED - PRIME1;

            int limit = end - 32;
            do {
                v1 = round(v1, read64(data, p));
                v2 = round(v2, read64(data, p + 8));
                v3 = round(v3, read64(data, p + 16));
                v4 = round(v4, read64(data, p + 24));
                p += 32;
            } while (p <= limit);

            h64 = Long.rotateLeft(v1, 1) + Long.rotateLeft(v2, 7)
                    + Long.rotateLeft(v3, 12) + Long.rotateLeft(v4, 18);
            h64 = mergeRound(h64, v1);
            h64 = mergeRound(h64, v2);
            h64 = mergeRound(h64, v3);
            h64 = mergeRound(h64, v4);
        }
        else {
            h64 = SEED + PRIME5;
        }

        h64 += length;

        while (end - p >= 8) {
            h64 ^= round(0, read64(data, p));
            h64 = Long.rotateLeft(h64, 27) * PRIME1 + PRIME4;
            p += 8;
        }
        if (end - p >= 4) {
            h64 ^= read32(data, p) * PRIME1;
            h64 = Long.rotateLeft(h64, 23) * PRIME2 + PRIME3;
            p += 4;
        }
        while (p < end) {
            h64 ^= (data[p] & 0xFFL) * PRIME5;
            h64 = Long.rotateLeft(h64, 11) * PRIME1;
            p++;
        }

        return avalanche(h64);
    }

    /// Hashes a `long` — equivalent to hashing its eight little-endian bytes (the plain encoding of
    /// an `INT64`), without allocating an array.
    public static long hash(long value) {
        long h64 = SEED + PRIME5 + Long.BYTES;
        h64 ^= round(0, value);
        h64 = Long.rotateLeft(h64, 27) * PRIME1 + PRIME4;
        return avalanche(h64);
    }

    /// Hashes an `int` — equivalent to hashing its four little-endian bytes (the plain encoding of
    /// an `INT32`), without allocating an array.
    public static long hash(int value) {
        long h64 = SEED + PRIME5 + Integer.BYTES;
        h64 ^= (value & 0xFFFFFFFFL) * PRIME1;
        h64 = Long.rotateLeft(h64, 23) * PRIME2 + PRIME3;
        return avalanche(h64);
    }

    /// Final mixing step (avalanche) shared by every entry point.
    private static long avalanche(long h64) {
        h64 ^= h64 >>> 33;
        h64 *= PRIME2;
        h64 ^= h64 >>> 29;
        h64 *= PRIME3;
        h64 ^= h64 >>> 32;
        return h64;
    }

    private static long round(long acc, long input) {
        acc += input * PRIME2;
        acc = Long.rotateLeft(acc, 31);
        acc *= PRIME1;
        return acc;
    }

    private static long mergeRound(long acc, long val) {
        val = round(0, val);
        acc ^= val;
        acc = acc * PRIME1 + PRIME4;
        return acc;
    }

    private static long read64(byte[] b, int i) {
        return (b[i] & 0xFFL)
                | (b[i + 1] & 0xFFL) << 8
                | (b[i + 2] & 0xFFL) << 16
                | (b[i + 3] & 0xFFL) << 24
                | (b[i + 4] & 0xFFL) << 32
                | (b[i + 5] & 0xFFL) << 40
                | (b[i + 6] & 0xFFL) << 48
                | (b[i + 7] & 0xFFL) << 56;
    }

    private static long read32(byte[] b, int i) {
        return (b[i] & 0xFFL)
                | (b[i + 1] & 0xFFL) << 8
                | (b[i + 2] & 0xFFL) << 16
                | (b[i + 3] & 0xFFL) << 24;
    }
}
