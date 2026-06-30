/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.bloomfilter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

import org.apache.parquet.column.values.bloomfilter.BlockSplitBloomFilter;
import org.apache.parquet.io.api.Binary;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/// Oracle cross-checking Hardwood's bloom-filter hashing and membership against the parquet-java
/// reference implementation (`org.apache.parquet.column.values.bloomfilter`).
///
/// Two independent guarantees compose into "Hardwood prunes a row group exactly as parquet-java
/// would":
///
/// 1. **Hash parity** — [XxHash64] produces the same 64-bit hash as `BlockSplitBloomFilter.hash`
///    for every supported physical type (`INT32`, `INT64`, `FLOAT`, `DOUBLE`, binary). The bloom
///    decision is `mightContain(hash(value))`, so matching the hash is half the equivalence.
/// 2. **Membership parity** — given a bitset built and serialized by parquet-java, Hardwood's
///    [BloomFilter#mightContain] returns the same verdict as `BlockSplitBloomFilter.findHash` for
///    every probe, across stored values and a wide sweep of absent ones (so even false positives
///    agree). That pins the split-block probe layout to the reference.
class BloomFilterParquetJavaOracleTest {

    /// Filter size (bytes) used to build reference filters. A power of two within parquet-java's
    /// bounds; the hash function is independent of size, so it serves the hash-parity tests too.
    private static final int FILTER_BYTES = 1024;

    private static final BlockSplitBloomFilter REF = new BlockSplitBloomFilter(FILTER_BYTES);

    @Test
    void int32HashMatchesParquetJava() {
        for (int v : new int[] {0, 1, -1, 7, 189, Integer.MIN_VALUE, Integer.MAX_VALUE, 0x01234567}) {
            assertThat(XxHash64.hash(v)).as("hash(int %d)", v).isEqualTo(REF.hash(v));
        }
    }

    @Test
    void int64HashMatchesParquetJava() {
        for (long v : new long[] {0L, 1L, -1L, 63L, Long.MIN_VALUE, Long.MAX_VALUE, 0x0123456789ABCDEFL}) {
            assertThat(XxHash64.hash(v)).as("hash(long %d)", v).isEqualTo(REF.hash(v));
        }
    }

    @Test
    void floatHashMatchesParquetJava() {
        for (float v : new float[] {0.0f, -0.0f, 1.0f, -1.0f, 3.14159f, Float.MIN_VALUE, Float.MAX_VALUE,
                Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY}) {
            assertThat(XxHash64.hash(v)).as("hash(float %s)", v).isEqualTo(REF.hash(v));
        }
    }

    @Test
    void doubleHashMatchesParquetJava() {
        for (double v : new double[] {0.0, -0.0, 1.0, -1.0, 3.141592653589793, Double.MIN_VALUE,
                Double.MAX_VALUE, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY}) {
            assertThat(XxHash64.hash(v)).as("hash(double %s)", v).isEqualTo(REF.hash(v));
        }
    }

    @Test
    void binaryHashMatchesParquetJava() {
        for (String s : new String[] {"", "a", "abc", "rg2_150", "x".repeat(40)}) {
            byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
            assertThat(XxHash64.hash(bytes)).as("hash(binary '%s')", s)
                    .isEqualTo(REF.hash(Binary.fromConstantByteArray(bytes)));
        }
    }

    @Test
    void membershipMatchesParquetJavaOnAParquetJavaBitset() throws IOException {
        // Build a filter with parquet-java over a known set, serialize its bitset, then read that
        // exact bitset back through Hardwood: same bytes + same hash ⇒ identical verdicts.
        BlockSplitBloomFilter ref = new BlockSplitBloomFilter(FILTER_BYTES);
        int[] present = new int[64];
        for (int i = 0; i < present.length; i++) {
            present[i] = i * 3; // 0, 3, ..., 189
            ref.insertHash(ref.hash(present[i]));
        }
        ByteArrayOutputStream serialized = new ByteArrayOutputStream();
        ref.writeTo(serialized);
        byte[] bitset = serialized.toByteArray();

        BloomFilter hardwood = new BloomFilter(
                new BloomFilterHeader(bitset.length, BloomFilterHeader.Algorithm.BLOCK,
                        BloomFilterHeader.Hash.XXHASH, BloomFilterHeader.Compression.UNCOMPRESSED),
                ByteBuffer.wrap(bitset).order(ByteOrder.LITTLE_ENDIAN));

        // Every stored value must be reported present by both (a false negative would be a bug).
        for (int v : present) {
            long hash = ref.hash(v);
            assertThat(hardwood.mightContain(hash)).as("stored value %d", v).isTrue();
            assertThat(hardwood.mightContain(hash)).isEqualTo(ref.findHash(hash));
        }
        // Wide sweep over absent values too: Hardwood and parquet-java must agree verdict-for-verdict,
        // so even false positives line up — proving the split-block probe is identical.
        for (int v = -1000; v <= 1000; v++) {
            long hash = ref.hash(v);
            assertThat(hardwood.mightContain(hash)).as("probe %d", v).isEqualTo(ref.findHash(hash));
        }
    }
}
